#!/usr/bin/env python3
"""Dynamic epoch benchmark with emulator-driven task metrics plus padding rows."""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List
from contextlib import contextmanager

import requests
from cattrs import structure, unstructure

from rtt_sweep_common import (
    add_common_args,
    bulk_ingest_es,
    ingest_server,
    parse_nodes_config,
    query_es_nodes,
    query_server_batch,
    reset_es_index,
    resolve_repo_path,
    start_server,
    stop_server,
    wait_for_server,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SOLVER_DUMMY_DIR = REPO_ROOT / "solver_experimental/dummy_data"
SOLVER_ROOT = REPO_ROOT / "solver_experimental"
ARMY_ROOT = REPO_ROOT / "ArmyController"
ARMY_RESOURCE_QUERY = ARMY_ROOT / "ramite/queries/resource_query_30.csv"
ARMY_MEAN_BAND = ARMY_ROOT / "ramite/queries/mean_band_30.csv"
ARMY_SOLVER_MARKER = "ARMY_INTERTASK"

DEFAULT_ROWS_PER_EPOCH = 1_000_000
DEFAULT_MAX_EPOCHS = 50
DEFAULT_SERVER_LOG = "logs/server_dynamic_epoch.log"
DEFAULT_EMULATOR_LOG = "logs/emulator_dynamic_epoch.log"
DEFAULT_EMULATOR_URL = "http://127.0.0.1:8000"
DEFAULT_EPOCH_LENGTH_S = 300.0
DEFAULT_PADDING_CPU_RATIO_MAX = 0.15
DEFAULT_PADDING_MEM_RATIO_MAX = 0.15
DEFAULT_PADDING_NET_RATIO_MAX = 0.10


@dataclass
class SolverResult:
    elapsed_ms: float
    objective_value: float
    assignments: int
    unassigned: int
    assigned_task_ids: List[str]
    unassigned_task_ids: List[str]
    decisions: Dict[str, Any]


@dataclass
class SweepResult:
    epoch: int
    rows_ingested: int
    emulator_rows: int
    padding_rows: int
    pending_tasks: int
    running_tasks: int
    completed_tasks: int
    server_ingest_ms: float
    es_ingest_ms: float
    server_query_ms: float
    es_query_ms: float
    server_solver_ms: float
    es_solver_ms: float
    server_total_ms: float
    es_total_ms: float
    server_assigned_count: int
    es_assigned_count: int
    server_unassigned_count: int
    es_unassigned_count: int
    server_objective: float
    es_objective: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Dynamic epoch benchmark: emulator-generated metrics + padding ingest, "
            "with per-epoch ingest/query/solver timings for Sketch server and ES "
            "using ArmyController for task placement."
        )
    )
    add_common_args(parser)
    parser.set_defaults(
        server_log=DEFAULT_SERVER_LOG,
        out_csv="data/dynamic_epoch_benchmark.csv",
    )
    parser.add_argument(
        "--out-plot",
        type=str,
        default="plots/dynamic_epoch_benchmark.png",
        help="Output stacked plot path",
    )
    parser.add_argument(
        "--solver-data-dir",
        type=str,
        default=str(SOLVER_DUMMY_DIR),
        help="Directory containing nodes.jsonl/edges.jsonl/tasks.jsonl",
    )
    parser.add_argument(
        "--solver-backend",
        type=str,
        choices=["CBC", "SCIP", "GLPK"],
        default="SCIP",
        help="Accepted for CLI compatibility; ArmyController path ignores this setting.",
    )
    parser.add_argument(
        "--max-epochs",
        type=int,
        default=DEFAULT_MAX_EPOCHS,
        help="Maximum number of epochs to run",
    )
    parser.add_argument(
        "--rows-per-epoch",
        type=int,
        default=DEFAULT_ROWS_PER_EPOCH,
        help="Total rows ingested per epoch (emulator + padding)",
    )
    parser.add_argument(
        "--emulator-url",
        type=str,
        default=DEFAULT_EMULATOR_URL,
        help="Emulator base URL",
    )
    parser.add_argument(
        "--skip-emulator-start",
        action="store_true",
        default=False,
        help="Do not start emulator subprocess; use already running emulator",
    )
    parser.add_argument(
        "--emulator-log",
        type=str,
        default=DEFAULT_EMULATOR_LOG,
        help="Emulator stdout/stderr log file (use '-' to disable)",
    )
    parser.add_argument(
        "--emulator-ready-timeout",
        type=float,
        default=30.0,
        help="Wait timeout for emulator health endpoint",
    )
    parser.add_argument(
        "--epoch-length-s",
        type=float,
        default=DEFAULT_EPOCH_LENGTH_S,
        help="Epoch length passed to emulator",
    )
    parser.add_argument(
        "--data-rate",
        type=int,
        default=1,
        help="Data points per second per running task in emulator",
    )
    parser.add_argument(
        "--solver-time-limit-s",
        type=float,
        default=30.0,
        help="Per-solver-call time limit in seconds (default: 30)",
    )
    parser.add_argument(
        "--no-padding",
        action="store_true",
        default=False,
        help="Disable synthetic padding rows; ingest only emulator rows",
    )
    parser.add_argument(
        "--padding-cpu-ratio-max",
        type=float,
        default=DEFAULT_PADDING_CPU_RATIO_MAX,
        help="Max CPU ratio of node capacity for padding rows (default: 0.15)",
    )
    parser.add_argument(
        "--padding-mem-ratio-max",
        type=float,
        default=DEFAULT_PADDING_MEM_RATIO_MAX,
        help="Max memory ratio of node capacity for padding rows (default: 0.15)",
    )
    parser.add_argument(
        "--padding-net-ratio-max",
        type=float,
        default=DEFAULT_PADDING_NET_RATIO_MAX,
        help="Max network ratio of derived node network capacity for padding rows (default: 0.10)",
    )
    return parser.parse_args()


def _ensure_solver_path() -> None:
    solver_root = SOLVER_ROOT.resolve()
    if str(solver_root) not in sys.path:
        sys.path.insert(0, str(solver_root))


def _ensure_army_path() -> None:
    army_root = ARMY_ROOT.resolve()
    if str(army_root) not in sys.path:
        sys.path.insert(0, str(army_root))


@contextmanager
def _chdir(path: Path):
    prev = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev)


def _numeric_suffix(node_name: str) -> int:
    try:
        return int(node_name.split("-")[1])
    except (IndexError, ValueError):
        return 0


def _army_node_sort_key(node_name: str) -> tuple[int, int, str]:
    prefix = node_name.split("-", 1)[0]
    prefix_order = {"APC": 0, "PLT": 1, "UGS": 2}.get(prefix, 99)
    return (prefix_order, _numeric_suffix(node_name), node_name)


def _load_solver_assets(data_dir: Path) -> dict:
    _ensure_solver_path()
    _ensure_army_path()
    try:
        from scheduler.load_info import (  # type: ignore
            load_edges as load_edges_jsonl,
            load_nodes as load_nodes_jsonl,
            load_tasks as load_tasks_jsonl,
        )
        from examples.AnglovaScenarioDAVC import (  # type: ignore
            DAVCTaskAllocator,
            generate_tasks,
        )
    except ImportError as exc:
        missing = getattr(exc, "name", None) or str(exc)
        army_req = ARMY_ROOT / "requirements.txt"
        raise RuntimeError(
            "Failed to import solver modules.\n"
            f"Missing Python module: {missing}\n"
            "Install ArmyController dependencies in this environment, e.g.:\n"
            f"  pip install -r {army_req}"
        ) from exc

    nodes_path = data_dir / "nodes.jsonl"
    edges_path = data_dir / "edges.jsonl"
    tasks_path = data_dir / "tasks.jsonl"
    if not (nodes_path.exists() and edges_path.exists() and tasks_path.exists()):
        raise RuntimeError(
            "solver-data-dir missing required JSONL files: nodes.jsonl, edges.jsonl, tasks.jsonl."
        )

    raw_nodes = load_nodes_jsonl(nodes_path)
    raw_edges = load_edges_jsonl(edges_path)
    raw_tasks = load_tasks_jsonl(tasks_path)

    with _chdir(ARMY_ROOT):
        allocator = DAVCTaskAllocator(
            update_interval=0,
            submit_tasks=False,
            adjust_workflows=False,
            solver_name="intertask",
            minisolver_name="minisolver",
            add_error="none",
            err_amnt=1.0,
            err_loc=None,
            err_time=None,
            set_band=None,
            set_cpu=None,
            set_ugs_cpu=None,
            req_acc=60.0,
            req_lat=1.0,
            num_images=100,
            num_tasks=1,
            seed=11,
            num_hops=None,
            obj_lambda=0.9997,
            resource_query_path=str(ARMY_RESOURCE_QUERY),
            mean_band_path=str(ARMY_MEAN_BAND),
        )
    army_nodes = sorted(
        [str(node_id) for node_id in allocator.scenario.resources()],
        key=_army_node_sort_key,
    )
    army_ugs_nodes = [node_id for node_id in army_nodes if node_id.startswith("UGS-")]
    if not army_ugs_nodes:
        raise RuntimeError("ArmyController scenario has no UGS nodes for task sources.")

    nodes: Dict[str, Any] = {}
    for node_id, node in raw_nodes.items():
        nodes[node_id] = SimpleNamespace(
            node_id=node.node_id,
            cpu_capacity=node.cpu_capacity,
            memory_capacity=node.memory_capacity,
            used_cpu=node.used_cpu,
            used_memory=node.used_memory,
        )

    edges: Dict[tuple, Any] = {}
    for edge_key, edge in raw_edges.items():
        edges[edge_key] = SimpleNamespace(
            edge_id=edge_key,
            capacity=edge.capacity,
            used_bandwidth=edge.used_bandwidth,
        )

    tasks: Dict[str, Any] = {}
    for task_id, task in raw_tasks.items():
        tasks[task_id] = SimpleNamespace(
            task_id=task.task_id,
            cpu=task.initial_cpu,
            memory=task.initial_memory,
            bandwidth=sum(task.peer_bandwidths.values()),
        )

    return {
        "nodes": nodes,
        "edges": edges,
        "tasks": tasks,
        "raw_tasks": raw_tasks,
        "army_allocator": allocator,
        "army_generate_tasks": generate_tasks,
        "army_nodes": army_nodes,
        "army_ugs_nodes": army_ugs_nodes,
    }


def _build_solver_context(
    assets: dict,
    allowed_node_ids: List[str],
) -> dict:
    allowed_set = set(allowed_node_ids)
    node_ids = sorted(n for n in assets["nodes"].keys() if n in allowed_set)
    if not node_ids:
        raise RuntimeError("No solver nodes overlap with server node configuration.")

    nodes = {nid: assets["nodes"][nid] for nid in node_ids}
    node_set = set(node_ids)
    edges = {
        eid: edge
        for eid, edge in assets["edges"].items()
        if eid[0] in node_set and eid[1] in node_set
    }
    tasks = dict(assets["tasks"])
    raw_tasks = dict(assets["raw_tasks"])
    army_nodes = list(assets.get("army_nodes", []))
    if len(army_nodes) < len(node_ids):
        raise RuntimeError(
            f"ArmyController has fewer nodes ({len(army_nodes)}) than selected benchmark nodes ({len(node_ids)})."
        )
    army_nodes = army_nodes[: len(node_ids)]
    army_to_emulator_node = {
        army_node: emulator_node
        for army_node, emulator_node in zip(army_nodes, node_ids)
    }
    return {
        "nodes": nodes,
        "edges": edges,
        "tasks": tasks,
        "raw_tasks": raw_tasks,
        "army_to_emulator_node": army_to_emulator_node,
    }


def _build_nodes_with_usage(
    base_nodes: Dict[str, object],
    usage: Dict[str, Dict[str, float]],
    ort_node_type: type,
) -> Dict[str, object]:
    updated: Dict[str, object] = {}
    for node_id, node in base_nodes.items():
        used = usage.get(node_id, {})
        used_cpu = min(used.get("cpu", node.used_cpu), node.cpu_capacity)
        used_mem = min(used.get("memory", node.used_memory), node.memory_capacity)
        updated[node_id] = ort_node_type(
            node_id=node.node_id,
            cpu_capacity=node.cpu_capacity,
            memory_capacity=node.memory_capacity,
            used_cpu=used_cpu,
            used_memory=used_mem,
        )
    return updated


def _extract_server_usage(server_json: dict) -> Dict[str, Dict[str, float]]:
    usage: Dict[str, Dict[str, float]] = {}
    for item in server_json.get("results", []):
        node_id = item.get("key")
        if not node_id:
            continue
        percentiles = item.get("percentiles") or {}
        cpu_pct = percentiles.get("cpu_cores") or {}
        mem_pct = percentiles.get("memory_gb") or {}
        usage[str(node_id)] = {
            "cpu": float(cpu_pct.get("50", 0.0) or 0.0),
            "memory": float(mem_pct.get("50", 0.0) or 0.0),
        }
    return usage


def _extract_es_usage(es_json: dict) -> Dict[str, Dict[str, float]]:
    usage: Dict[str, Dict[str, float]] = {}
    for node_id, payload in es_json.items():
        aggs = payload.get("aggregations", {})
        cpu_pct = aggs.get("cpu_pct", {}).get("values", {})
        mem_pct = aggs.get("mem_pct", {}).get("values", {})
        usage[str(node_id)] = {
            "cpu": float(cpu_pct.get("50.0", 0.0) or 0.0),
            "memory": float(mem_pct.get("50.0", 0.0) or 0.0),
        }
    return usage


def _build_army_task_payload(
    task_ids: List[str],
    ugs_nodes: List[str],
) -> str:
    # Use deterministic UGS source assignment so runs are repeatable.
    payload: List[Dict[str, object]] = []
    total = max(1, len(task_ids))
    for idx, task_id in enumerate(task_ids):
        payload.append(
            {
                "dataLoc": ugs_nodes[idx % len(ugs_nodes)],
                "id": task_id,
                "num_images": 100,
                "timeliness": 1.0,
                "req_acc": 60.0,
                "cpu_deg": 1.0,
                "bwp_deg": 1.0,
                "type": "image",
                "priority": 1.0 / total,
            }
        )
    return json.dumps(payload)


def run_solver_for_usage(
    usage: Dict[str, Dict[str, float]],
    assets: dict,
    context: dict,
    solver_backend: str,
    task_ids: List[str],
    time_limit_s: float,
) -> SolverResult:
    if not task_ids:
        return SolverResult(
            elapsed_ms=0.0,
            objective_value=0.0,
            assignments=0,
            unassigned=0,
            assigned_task_ids=[],
            unassigned_task_ids=[],
            decisions={},
        )

    _ = usage
    _ = context
    _ = solver_backend
    _ = time_limit_s
    allocator = assets["army_allocator"]
    payload = _build_army_task_payload(task_ids, assets["army_ugs_nodes"])
    task_requests = assets["army_generate_tasks"](payload)

    # Match benchmark behavior: independent one-shot solve per epoch.
    allocator.scenario.running_tasks = []
    t0 = time.perf_counter()
    with _chdir(ARMY_ROOT):
        (
            result,
            _solver_time_s,
            placement,
            _acc,
            _mean_execution_time,
            _mean_execution_time_per_frame,
            _mean_execution_time_per_frame_w_overhead,
        ) = allocator.scenario.place_tasks(task_requests)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    decisions: Dict[str, Dict[str, str]] = {}
    assigned_task_ids: List[str] = []
    for task in placement:
        if getattr(task, "dest", None):
            task_id = str(task.id)
            assigned_task_ids.append(task_id)
            decisions[task_id] = {"node_id": str(task.dest)}

    assigned_set = set(assigned_task_ids)
    unassigned_task_ids = sorted([task_id for task_id in task_ids if task_id not in assigned_set])
    assigned_task_ids = sorted(assigned_task_ids)

    return SolverResult(
        elapsed_ms=elapsed_ms,
        objective_value=float(result),
        assignments=len(assigned_task_ids),
        unassigned=len(unassigned_task_ids),
        assigned_task_ids=assigned_task_ids,
        unassigned_task_ids=unassigned_task_ids,
        decisions=decisions,
    )


def _column_record_to_rows(record: dict, epoch: int) -> List[Dict[str, object]]:
    clusters = record.get("cluster", [])
    tasks = record.get("task", [])
    cpus = record.get("cpu_cores", [])
    mems = record.get("memory_gb", [])
    nets = record.get("network_mbps", [])
    lengths = {len(clusters), len(tasks), len(cpus), len(mems), len(nets)}
    if len(lengths) != 1:
        raise RuntimeError("Emulator record columns have mismatched lengths.")

    rows: List[Dict[str, object]] = []
    for i in range(len(tasks)):
        rows.append(
            {
                "epoch": epoch,
                "node": clusters[i],
                "task": tasks[i],
                "cpu": float(cpus[i]),
                "mem": float(mems[i]),
                "net": float(nets[i]),
            }
        )
    return rows


def _records_to_rows(records: List[dict], epoch: int) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for record in records:
        rows.extend(_column_record_to_rows(record, epoch))
    return rows


def _estimate_node_network_capacity(node_id: str, context: dict) -> float:
    capacity = 0.0
    for edge_id, edge in context["edges"].items():
        if node_id in edge_id:
            capacity += float(edge.capacity)
    if capacity <= 0.0:
        capacity = 100.0
    return capacity


def _make_padding_rows(
    total_rows: int,
    node_ids: List[str],
    context: dict,
    rng: random.Random,
    epoch: int,
    cpu_ratio_max: float,
    mem_ratio_max: float,
    net_ratio_max: float,
) -> List[Dict[str, object]]:
    if total_rows <= 0:
        return []
    cpu_ratio_max = max(cpu_ratio_max, 1e-3)
    mem_ratio_max = max(mem_ratio_max, 1e-3)
    net_ratio_max = max(net_ratio_max, 1e-3)

    tasks = [f"T{i:03d}" for i in range(1, 201)]
    rows: List[Dict[str, object]] = []
    for i in range(total_rows):
        node_id = node_ids[i % len(node_ids)]
        node = context["nodes"][node_id]
        net_cap = _estimate_node_network_capacity(node_id, context)
        rows.append(
            {
                "epoch": epoch,
                "node": node_id,
                "task": tasks[i % len(tasks)],
                "cpu": rng.uniform(0.01, node.cpu_capacity * cpu_ratio_max),
                "mem": rng.uniform(0.01, node.memory_capacity * mem_ratio_max),
                "net": rng.uniform(0.01, net_cap * net_ratio_max),
            }
        )
    return rows


def _batched(rows: List[Dict[str, object]], batch_size: int) -> Iterable[List[Dict[str, object]]]:
    for i in range(0, len(rows), batch_size):
        yield rows[i : i + batch_size]


def _ingest_rows(
    rows: List[Dict[str, object]],
    epoch: int,
    args: argparse.Namespace,
    server_ingest_ms: float,
    es_ingest_ms: float,
    is_last_phase: bool,
    phase_label: str,
) -> tuple[float, float]:
    if not rows:
        return server_ingest_ms, es_ingest_ms

    total_batches = (len(rows) + args.batch_size - 1) // args.batch_size
    log_every = max(1, total_batches // 10)
    for batch_idx, batch in enumerate(_batched(rows, args.batch_size), start=1):
        t0 = time.perf_counter()
        ingest_server(
            args.server_url,
            batch,
            epoch,
            args.connect_timeout,
            args.ingest_timeout,
            args.ingest_retries,
            args.ingest_retry_backoff,
        )
        server_ingest_ms += (time.perf_counter() - t0) * 1000.0

        is_last_batch = is_last_phase and batch_idx == total_batches
        t0 = time.perf_counter()
        bulk_ingest_es(
            args.es_url,
            args.es_index,
            args.es_api_key,
            batch,
            args.connect_timeout,
            args.es_timeout,
            "wait_for" if is_last_batch else None,
        )
        es_ingest_ms += (time.perf_counter() - t0) * 1000.0

        if batch_idx % log_every == 0 or batch_idx == total_batches:
            print(
                f"  ingest progress ({phase_label}): {batch_idx}/{total_batches} batches "
                f"({batch_idx * 100 // total_batches}%) | "
                f"server={server_ingest_ms:.0f} ms  ES={es_ingest_ms:.0f} ms"
            )
    return server_ingest_ms, es_ingest_ms


def start_emulator(args: argparse.Namespace) -> subprocess.Popen:
    log_path = None if args.emulator_log == "-" else resolve_repo_path(args.emulator_log)
    stdout_target = None
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        mode = "w" if args.truncate_server_log else "a"
        stdout_target = open(log_path, mode, encoding="utf-8")

    cmd = [
        "uv",
        "run",
        "emulate_telemetry.py",
        "--host",
        "127.0.0.1",
        "--port",
        str(_port_from_url(args.emulator_url)),
        "--epoch-length-s",
        str(args.epoch_length_s),
        "--data-rate",
        str(args.data_rate),
        "--no-sketch-ingest",
        "--no-es-ingest",
    ]
    proc = subprocess.Popen(
        cmd,
        cwd=SOLVER_ROOT,
        stdout=stdout_target or subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        text=True,
    )
    if stdout_target is not None:
        proc._log_fh = stdout_target
    return proc


def stop_process(proc: subprocess.Popen | None) -> None:
    if proc is None:
        return
    if proc.poll() is not None:
        log_fh = getattr(proc, "_log_fh", None)
        if log_fh:
            log_fh.close()
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    log_fh = getattr(proc, "_log_fh", None)
    if log_fh:
        log_fh.close()


def wait_for_emulator(url: str, timeout_s: float, connect_timeout: float, read_timeout: float) -> None:
    deadline = time.time() + timeout_s
    health_url = f"{url.rstrip('/')}/health"
    while time.time() < deadline:
        try:
            resp = requests.get(health_url, timeout=(connect_timeout, read_timeout))
            if resp.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(0.5)
    raise RuntimeError("Emulator did not become ready.")


def generate_from_emulator(
    emulator_url: str,
    running_tasks: Dict[str, object],
    connect_timeout: float,
    read_timeout: float,
) -> List[dict]:
    payload = unstructure(list(running_tasks.values()))
    resp = requests.post(
        f"{emulator_url.rstrip('/')}/generate",
        json=payload,
        timeout=(connect_timeout, read_timeout),
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("records", [])


def fetch_active_tasks(
    emulator_url: str,
    connect_timeout: float,
    read_timeout: float,
) -> Dict[str, object]:
    _ensure_solver_path()
    from scheduler.entities import RunningTask  # type: ignore

    resp = requests.get(
        f"{emulator_url.rstrip('/')}/active_tasks",
        timeout=(connect_timeout, read_timeout),
    )
    resp.raise_for_status()
    payload = resp.json()
    return structure(payload.get("running_tasks", {}), dict[str, RunningTask])


def _port_from_url(url: str) -> int:
    from urllib.parse import urlparse

    parsed = urlparse(url)
    if parsed.port is not None:
        return parsed.port
    return 8000


def _build_running_tasks_for_assignments(
    assigned_task_ids: List[str],
    context: dict,
    epoch_start_time_s: float,
    assignments: dict,
) -> Dict[str, object]:
    _ensure_solver_path()
    from scheduler.entities import RunningTask  # type: ignore

    running: Dict[str, object] = {}
    for task_id in assigned_task_ids:
        decision = assignments[task_id]
        raw_task = context["raw_tasks"][task_id]
        if isinstance(decision, dict):
            node_id = str(decision["node_id"])
        else:
            node_id = str(decision.node_id)
        node_id = context.get("army_to_emulator_node", {}).get(node_id, node_id)
        running[task_id] = RunningTask(
            node_id=node_id,
            start_time_s=epoch_start_time_s,
            task=raw_task,
        )
    return running


def plot_results(results: List[SweepResult], out_path: Path, backend: str = ARMY_SOLVER_MARKER) -> None:
    import matplotlib.pyplot as plt
    import numpy as np

    epochs = [r.epoch for r in results]
    x = np.arange(len(epochs))
    bar_w = 0.35

    s_ingest = [r.server_ingest_ms for r in results]
    s_query = [r.server_query_ms for r in results]
    s_solver = [r.server_solver_ms for r in results]
    e_ingest = [r.es_ingest_ms for r in results]
    e_query = [r.es_query_ms for r in results]
    e_solver = [r.es_solver_ms for r in results]
    s_total = [r.server_total_ms for r in results]
    e_total = [r.es_total_ms for r in results]

    fig, ax = plt.subplots(figsize=(max(10, len(epochs) * 1.5), 6))
    ax.bar(x - bar_w / 2, s_ingest, bar_w, label="Server ingest", color="#4e79a7")
    ax.bar(x - bar_w / 2, s_query, bar_w, bottom=s_ingest, label="Server query", color="#76b7b2")
    ax.bar(
        x - bar_w / 2,
        s_solver,
        bar_w,
        bottom=[i + q for i, q in zip(s_ingest, s_query)],
        label="Server solver",
        color="#59a14f",
    )
    ax.bar(x + bar_w / 2, e_ingest, bar_w, label="ES ingest", color="#e15759")
    ax.bar(x + bar_w / 2, e_query, bar_w, bottom=e_ingest, label="ES query", color="#f28e2b")
    ax.bar(
        x + bar_w / 2,
        e_solver,
        bar_w,
        bottom=[i + q for i, q in zip(e_ingest, e_query)],
        label="ES solver",
        color="#b07aa1",
    )
    ax.set_xticks(x)
    ax.set_xticklabels([str(e) for e in epochs])
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Time (ms)")
    ax.set_title(f"Per-epoch timing breakdown: Server vs ES (ArmyController/{backend}, emulator)")
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close(fig)

    total_path = out_path.with_name(f"{out_path.stem}_total{out_path.suffix}")
    fig2, ax2 = plt.subplots(figsize=(max(8, len(epochs) * 1.2), 5))
    ax2.plot(epochs, s_total, marker="o", label="Server total (ms)", color="#4e79a7")
    ax2.plot(epochs, e_total, marker="s", label="ES total (ms)", color="#e15759")
    ax2.set_xlabel("Epoch")
    ax2.set_ylabel("Total time (ms)")
    ax2.set_title(f"Total epoch time: Server vs ES (ArmyController/{backend})")
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(total_path)
    plt.close(fig2)


def plot_task_progress(results: List[SweepResult], out_path: Path) -> None:
    import matplotlib.pyplot as plt

    epochs = [r.epoch for r in results]
    pending = [r.pending_tasks for r in results]
    running = [r.running_tasks for r in results]
    completed = [r.completed_tasks for r in results]

    fig, ax = plt.subplots(figsize=(max(8, len(epochs) * 1.2), 5))
    ax.plot(epochs, pending, marker="o", label="Pending", color="#f28e2b")
    ax.plot(epochs, running, marker="s", label="Running", color="#4e79a7")
    ax.plot(epochs, completed, marker="^", label="Completed", color="#59a14f")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Number of Tasks")
    ax.set_title("Task Assignment Progress Over Epochs")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    army_solver_label = ARMY_SOLVER_MARKER
    query_nodes = parse_nodes_config(args.nodes_config)

    out_csv = resolve_repo_path(args.out_csv)
    out_plot = resolve_repo_path(args.out_plot)
    out_tasks_plot = out_plot.with_name(f"{out_plot.stem}_tasks{out_plot.suffix}")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_plot.parent.mkdir(parents=True, exist_ok=True)

    assets = _load_solver_assets(Path(args.solver_data_dir))
    context = _build_solver_context(assets, query_nodes)
    print(
        f"Solver setup: nodes={len(context['nodes'])}/{len(assets['nodes'])}, "
        f"tasks={len(context['tasks'])}/{len(assets['tasks'])}, backend={army_solver_label} "
        f"(--solver-backend={args.solver_backend} accepted for CLI compatibility)"
    )
    if "N000" in context["nodes"]:
        raise RuntimeError("Node alignment failed: N000 is still present in solver context.")

    csv_exists = out_csv.exists()
    csv_mode = "w" if args.truncate_csv else "a"
    csv_file = open(out_csv, csv_mode, newline="")
    writer = csv.writer(csv_file)
    if args.truncate_csv or not csv_exists:
        writer.writerow(
            [
                "timestamp_utc",
                "epoch",
                "rows_ingested",
                "emulator_rows",
                "padding_rows",
                "pending_tasks",
                "running_tasks",
                "completed_tasks",
                "server_ingest_ms",
                "es_ingest_ms",
                "server_query_ms",
                "es_query_ms",
                "server_solver_ms",
                "es_solver_ms",
                "server_total_ms",
                "es_total_ms",
                "server_assigned_count",
                "es_assigned_count",
                "server_unassigned_count",
                "es_unassigned_count",
                "server_objective",
                "es_objective",
                "solver_backend",
            ]
        )
        csv_file.flush()

    reset_es_index(
        args.es_url,
        args.es_index,
        args.es_api_key,
        args.connect_timeout,
        args.es_timeout,
    )

    server_log_path = None if args.server_log == "-" else resolve_repo_path(args.server_log)
    server_proc = start_server(server_log_path, truncate_log=args.truncate_server_log)
    emulator_proc: subprocess.Popen | None = None

    results: List[SweepResult] = []
    pending_task_ids: List[str] = sorted(context["tasks"].keys())
    completed_task_ids: set[str] = set()
    running_tasks: Dict[str, object] = {}

    try:
        wait_for_server(
            args.server_url,
            args.server_ready_timeout,
            args.connect_timeout,
            args.query_timeout,
        )

        if not args.skip_emulator_start:
            emulator_proc = start_emulator(args)
        wait_for_emulator(
            args.emulator_url,
            args.emulator_ready_timeout,
            args.connect_timeout,
            args.query_timeout,
        )

        for epoch in range(args.max_epochs):
            print(f"\n=== Epoch {epoch} ===")
            server_ingest_ms = 0.0
            es_ingest_ms = 0.0
            server_query_ms = 0.0
            es_query_ms = 0.0
            server_solver_ms = 0.0
            es_solver_ms = 0.0

            emulator_rows = 0
            padding_rows = 0

            if epoch > 0 and running_tasks:
                records = generate_from_emulator(
                    args.emulator_url,
                    running_tasks,
                    args.connect_timeout,
                    args.query_timeout,
                )
                emulator_ingest_rows = _records_to_rows(records, epoch)
                emulator_rows = len(emulator_ingest_rows)
                padding_rows = (
                    0 if args.no_padding else max(0, args.rows_per_epoch - emulator_rows)
                )
                total_rows = emulator_rows + padding_rows
                print(
                    f"  generated emulator_rows={emulator_rows}, padding_rows={padding_rows}, total_rows={total_rows}"
                )

                server_ingest_ms, es_ingest_ms = _ingest_rows(
                    emulator_ingest_rows,
                    epoch,
                    args,
                    server_ingest_ms,
                    es_ingest_ms,
                    is_last_phase=(padding_rows == 0),
                    phase_label="emulator",
                )

                if padding_rows > 0:
                    padding_all_rows = _make_padding_rows(
                        padding_rows,
                        query_nodes,
                        context,
                        rng,
                        epoch,
                        args.padding_cpu_ratio_max,
                        args.padding_mem_ratio_max,
                        args.padding_net_ratio_max,
                    )
                    if len(padding_all_rows) != padding_rows:
                        raise RuntimeError("Padding row generation mismatch.")
                    server_ingest_ms, es_ingest_ms = _ingest_rows(
                        padding_all_rows,
                        epoch,
                        args,
                        server_ingest_ms,
                        es_ingest_ms,
                        is_last_phase=True,
                        phase_label="padding",
                    )
            elif epoch > 0:
                padding_rows = 0 if args.no_padding else args.rows_per_epoch
                total_rows = padding_rows
                if padding_rows > 0:
                    print("  no running tasks; ingesting padding-only epoch")
                    padding_all_rows = _make_padding_rows(
                        padding_rows,
                        query_nodes,
                        context,
                        rng,
                        epoch,
                        args.padding_cpu_ratio_max,
                        args.padding_mem_ratio_max,
                        args.padding_net_ratio_max,
                    )
                    server_ingest_ms, es_ingest_ms = _ingest_rows(
                        padding_all_rows,
                        epoch,
                        args,
                        server_ingest_ms,
                        es_ingest_ms,
                        is_last_phase=True,
                        phase_label="padding",
                    )
                else:
                    print("  no running tasks and padding disabled; ingest skipped")
            else:
                total_rows = 0

            if epoch > 0:
                server_json, server_query_ms = query_server_batch(
                    args.server_url,
                    query_nodes,
                    args.connect_timeout,
                    args.query_timeout,
                )
                es_json, es_query_ms = query_es_nodes(
                    args.es_url,
                    args.es_index,
                    args.es_api_key,
                    query_nodes,
                    args.connect_timeout,
                    args.es_timeout,
                    epoch,
                )
                server_usage = _extract_server_usage(server_json)
                es_usage = _extract_es_usage(es_json)
            else:
                server_usage = {}
                es_usage = {}

            print(
                f"  solver start ({army_solver_label}, limit={args.solver_time_limit_s:.1f}s): "
                f"pending_tasks={len(pending_task_ids)}"
            )
            server_sr = run_solver_for_usage(
                server_usage,
                assets,
                context,
                solver_backend=args.solver_backend,
                task_ids=pending_task_ids,
                time_limit_s=args.solver_time_limit_s,
            )
            es_sr = run_solver_for_usage(
                es_usage,
                assets,
                context,
                solver_backend=args.solver_backend,
                task_ids=pending_task_ids,
                time_limit_s=args.solver_time_limit_s,
            )
            server_solver_ms = server_sr.elapsed_ms
            es_solver_ms = es_sr.elapsed_ms
            print(
                f"  solver done ({army_solver_label}, limit={args.solver_time_limit_s:.1f}s): "
                f"server={server_solver_ms:.2f} ms, ES={es_solver_ms:.2f} ms | "
                f"assigned(server/es)={server_sr.assignments}/{es_sr.assignments}"
            )

            # Build per-epoch new assignments from ArmyController solver output.
            new_running = _build_running_tasks_for_assignments(
                server_sr.assigned_task_ids,
                context,
                epoch * args.epoch_length_s,
                server_sr.decisions,
            )

            # Keep active tasks (from previous running set), retire completed, and append new assignments.
            if epoch > 0:
                active = fetch_active_tasks(
                    args.emulator_url,
                    args.connect_timeout,
                    args.query_timeout,
                )
                newly_completed = set(running_tasks.keys()) - set(active.keys())
                completed_task_ids.update(newly_completed)
                running_tasks = dict(active)
            running_tasks.update(new_running)

            pending_task_ids = list(server_sr.unassigned_task_ids)

            server_total_ms = server_ingest_ms + server_query_ms + server_solver_ms
            es_total_ms = es_ingest_ms + es_query_ms + es_solver_ms
            rows_ingested = emulator_rows + padding_rows

            if epoch > 0 and (not args.no_padding) and rows_ingested != args.rows_per_epoch:
                raise RuntimeError(
                    f"Row count mismatch in epoch {epoch}: got {rows_ingested}, expected {args.rows_per_epoch}"
                )

            result = SweepResult(
                epoch=epoch,
                rows_ingested=rows_ingested,
                emulator_rows=emulator_rows,
                padding_rows=padding_rows,
                pending_tasks=len(pending_task_ids),
                running_tasks=len(running_tasks),
                completed_tasks=len(completed_task_ids),
                server_ingest_ms=server_ingest_ms,
                es_ingest_ms=es_ingest_ms,
                server_query_ms=server_query_ms,
                es_query_ms=es_query_ms,
                server_solver_ms=server_solver_ms,
                es_solver_ms=es_solver_ms,
                server_total_ms=server_total_ms,
                es_total_ms=es_total_ms,
                server_assigned_count=server_sr.assignments,
                es_assigned_count=es_sr.assignments,
                server_unassigned_count=server_sr.unassigned,
                es_unassigned_count=es_sr.unassigned,
                server_objective=server_sr.objective_value,
                es_objective=es_sr.objective_value,
            )
            results.append(result)

            writer.writerow(
                [
                    datetime.now(timezone.utc).isoformat(),
                    result.epoch,
                    result.rows_ingested,
                    result.emulator_rows,
                    result.padding_rows,
                    result.pending_tasks,
                    result.running_tasks,
                    result.completed_tasks,
                    f"{result.server_ingest_ms:.4f}",
                    f"{result.es_ingest_ms:.4f}",
                    f"{result.server_query_ms:.4f}",
                    f"{result.es_query_ms:.4f}",
                    f"{result.server_solver_ms:.4f}",
                    f"{result.es_solver_ms:.4f}",
                    f"{result.server_total_ms:.4f}",
                    f"{result.es_total_ms:.4f}",
                    result.server_assigned_count,
                    result.es_assigned_count,
                    result.server_unassigned_count,
                    result.es_unassigned_count,
                    f"{result.server_objective:.6f}",
                    f"{result.es_objective:.6f}",
                    army_solver_label,
                ]
            )
            csv_file.flush()

            print(
                f"  totals: server={server_total_ms:.2f} ms, ES={es_total_ms:.2f} ms | "
                f"pending={len(pending_task_ids)} running={len(running_tasks)} completed={len(completed_task_ids)}"
            )

            if not pending_task_ids and not running_tasks:
                print("All tasks assigned and completed; stopping early.")
                break

    finally:
        stop_process(emulator_proc)
        stop_server(server_proc)
        csv_file.close()

    if results:
        plot_results(results, out_plot, backend=army_solver_label)
        plot_task_progress(results, out_tasks_plot)
        total_plot = out_plot.with_name(f"{out_plot.stem}_total{out_plot.suffix}")
        print(f"\nWrote {out_csv}")
        print(f"Wrote {out_plot}")
        print(f"Wrote {total_plot}")
        print(f"Wrote {out_tasks_plot}")


if __name__ == "__main__":
    main()

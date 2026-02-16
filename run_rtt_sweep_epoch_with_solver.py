#!/usr/bin/env python3
"""Run epoch-based ingestion/query RTT sweep for server + Elasticsearch."""

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
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import requests

SOLVER_DUMMY_DIR = Path("solver_experimental/dummy_data")
SOLVER_ROOT = Path("solver_experimental")


DEFAULT_ES_URL = "http://localhost:9200"
DEFAULT_ES_INDEX = "cluster-metrics"
DEFAULT_ES_API_KEY = os.getenv(
    "ES_API_KEY",
    "UzhwdVM1d0Jtb2JkQy1QOE1GTDM6NFRRSHBRXzJtLV9xTXhMUzFJM1FPZw==",
)
DEFAULT_SERVER_URL = "http://localhost:10101"
DEFAULT_BATCH_SIZE = 1000
DEFAULT_ROWS_PER_EPOCH = 1_000_000
DEFAULT_START_EPOCH = 1
DEFAULT_END_EPOCH = 10
DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_INGEST_TIMEOUT = 60.0
DEFAULT_QUERY_TIMEOUT = 60.0
DEFAULT_ES_TIMEOUT = 60.0
DEFAULT_SERVER_READY_TIMEOUT = 30.0
DEFAULT_INGEST_RETRIES = 2
DEFAULT_INGEST_RETRY_BACKOFF = 2.0
DEFAULT_SERVER_LOG = "logs/server_epoch.log"
DEFAULT_TRUNCATE_CSV = False
DEFAULT_TRUNCATE_SERVER_LOG = False


@dataclass
class SweepResult:
    epoch: int
    server_query_ms: float
    server_solver_ms: float
    server_total_ms: float
    es_query_ms: float
    es_solver_ms: float
    es_total_ms: float


@dataclass
class SolverResult:
    elapsed_ms: float
    objective_value: float
    assignments: int
    unassigned: int
    status_code: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-epoch", type=int, default=DEFAULT_START_EPOCH, help="Start epoch id")
    parser.add_argument("--end-epoch", type=int, default=DEFAULT_END_EPOCH, help="End epoch id (inclusive)")
    parser.add_argument("--rows-per-epoch", type=int, default=DEFAULT_ROWS_PER_EPOCH, help="Rows per epoch")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Rows per ingest batch")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--server-url", type=str, default=DEFAULT_SERVER_URL, help="Server base URL")
    parser.add_argument(
        "--server-log",
        type=str,
        default=DEFAULT_SERVER_LOG,
        help="Server stdout/stderr log file (use '-' to disable)",
    )
    parser.add_argument(
        "--truncate-csv",
        action="store_true",
        default=DEFAULT_TRUNCATE_CSV,
        help="Truncate output CSV before writing",
    )
    parser.add_argument(
        "--truncate-server-log",
        action="store_true",
        default=DEFAULT_TRUNCATE_SERVER_LOG,
        help="Truncate server log before writing",
    )
    parser.add_argument("--connect-timeout", type=float, default=DEFAULT_CONNECT_TIMEOUT, help="HTTP connect timeout (s)")
    parser.add_argument("--ingest-timeout", type=float, default=DEFAULT_INGEST_TIMEOUT, help="Server ingest read timeout (s)")
    parser.add_argument("--query-timeout", type=float, default=DEFAULT_QUERY_TIMEOUT, help="Server query read timeout (s)")
    parser.add_argument("--es-timeout", type=float, default=DEFAULT_ES_TIMEOUT, help="Elasticsearch read timeout (s)")
    parser.add_argument(
        "--server-ready-timeout",
        type=float,
        default=DEFAULT_SERVER_READY_TIMEOUT,
        help="Wait for server readiness (s)",
    )
    parser.add_argument(
        "--ingest-retries",
        type=int,
        default=DEFAULT_INGEST_RETRIES,
        help="Retries for server ingest on timeout/connection error",
    )
    parser.add_argument(
        "--ingest-retry-backoff",
        type=float,
        default=DEFAULT_INGEST_RETRY_BACKOFF,
        help="Base backoff (s) between ingest retries",
    )
    parser.add_argument("--es-url", type=str, default=DEFAULT_ES_URL, help="Elasticsearch URL")
    parser.add_argument("--es-index", type=str, default=DEFAULT_ES_INDEX, help="Elasticsearch index")
    parser.add_argument("--es-api-key", type=str, default=DEFAULT_ES_API_KEY, help="Elasticsearch API key")
    parser.add_argument(
        "--nodes-config",
        type=str,
        default="single_node_server/network-control-server/nodes-config.yaml",
        help="Path to nodes-config.yaml",
    )
    parser.add_argument(
        "--out-csv",
        type=str,
        default="query_rtt.csv",
        help="Output CSV filename",
    )
    parser.add_argument(
        "--out-plot",
        type=str,
        default="query_rtt_plot_epoch_with_solver.png",
        help="Output plot filename",
    )
    parser.add_argument(
        "--run-solver",
        action="store_true",
        default=False,
        help="Run the Python solver once before the epoch sweep",
    )
    parser.add_argument(
        "--solver-task-count",
        type=int,
        default=0,
        help="Number of solver tasks to include (0 means all)",
    )
    parser.add_argument(
        "--solver-node-count",
        type=int,
        default=0,
        help="Number of solver nodes to include (0 means all)",
    )
    parser.add_argument(
        "--query-node-count",
        type=int,
        default=0,
        help="Number of nodes to query (0 means all)",
    )
    parser.add_argument(
        "--solver-data-dir",
        type=str,
        default=str(SOLVER_DUMMY_DIR),
        help="Directory containing dummy_data JSONL inputs",
    )
    return parser.parse_args()


def _ensure_solver_path() -> None:
    solver_root = SOLVER_ROOT.resolve()
    if str(solver_root) not in sys.path:
        sys.path.insert(0, str(solver_root))


def _load_solver_assets(data_dir: Path):
    _ensure_solver_path()
    try:
        from scheduler.entities import NetworkTopology, Node, Edge, Task  # type: ignore
        from scheduler.load_info import (  # type: ignore
            build_task_graph,
            load_edges,
            load_nodes,
            load_tasks,
        )
        from scheduler.solver import TaskScheduler  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "Failed to import scheduler modules. Ensure solver_experimental is on PYTHONPATH "
            "and dependencies are installed."
        ) from exc

    nodes_path = data_dir / "nodes.jsonl"
    edges_path = data_dir / "edges.jsonl"
    tasks_path = data_dir / "tasks.jsonl"
    if not (nodes_path.exists() and edges_path.exists() and tasks_path.exists()):
        alt_nodes = data_dir / "nodes.json"
        alt_edges = data_dir / "edges.json"
        alt_tasks = data_dir / "tasks.json"
        if alt_nodes.exists() or alt_edges.exists() or alt_tasks.exists():
            raise RuntimeError(
                "solver-data-dir must point to JSONL dummy_data (nodes.jsonl/edges.jsonl/tasks.jsonl). "
                "The python_solver/data JSON files are in a different schema."
            )
        raise RuntimeError(
            "solver-data-dir missing required JSONL files: nodes.jsonl, edges.jsonl, tasks.jsonl."
        )

    nodes = load_nodes(nodes_path)
    edges = load_edges(edges_path)
    tasks = load_tasks(tasks_path)
    task_graph = build_task_graph(tasks)

    base_topology = NetworkTopology(nodes.values(), edges.values())
    paths = _build_paths(base_topology)

    return {
        "nodes": nodes,
        "edges": edges,
        "tasks": tasks,
        "task_graph": task_graph,
        "paths": paths,
        "build_task_graph": build_task_graph,
        "NetworkTopology": NetworkTopology,
        "TaskScheduler": TaskScheduler,
        "Node": Node,
        "Edge": Edge,
        "Task": Task,
    }


def _extract_server_usage(server_json: dict) -> Dict[str, Dict[str, float]]:
    usage: Dict[str, Dict[str, float]] = {}
    for item in server_json.get("results", []):
        node_id = item.get("key")
        if not node_id:
            continue
        cumulative = item.get("cumulative") or {}
        usage[str(node_id)] = {
            "cpu": float(cumulative.get("cpu_cores", 0.0) or 0.0),
            "memory": float(cumulative.get("memory_gb", 0.0) or 0.0),
            "network": float(cumulative.get("network_mbps", 0.0) or 0.0),
        }
    return usage


def _extract_es_usage(es_json: dict) -> Dict[str, Dict[str, float]]:
    usage: Dict[str, Dict[str, float]] = {}
    for node_id, payload in es_json.items():
        aggs = payload.get("aggregations", {})
        usage[str(node_id)] = {
            "cpu": float(aggs.get("cpu_sum", {}).get("value", 0.0) or 0.0),
            "memory": float(aggs.get("mem_sum", {}).get("value", 0.0) or 0.0),
            "network": float(aggs.get("net_sum", {}).get("value", 0.0) or 0.0),
        }
    return usage


def _build_paths(topology) -> dict:
    paths = {}
    from itertools import combinations

    for n_i, n_j in combinations(topology.nodes, 2):
        if topology.has_path(n_i, n_j):
            paths[(n_i, n_j)] = [topology.find_shortest_path(n_i, n_j)]
    return paths


def _select_first_n(items: List[str], count: int) -> List[str]:
    if count <= 0 or count >= len(items):
        return list(items)
    return list(items[:count])


def _build_solver_context(assets: dict, task_count: int, node_count: int) -> dict:
    node_ids = _select_first_n(sorted(assets["nodes"].keys()), node_count)
    task_ids = _select_first_n(sorted(assets["tasks"].keys()), task_count)

    nodes = {node_id: assets["nodes"][node_id] for node_id in node_ids}
    tasks = {task_id: assets["tasks"][task_id] for task_id in task_ids}

    node_set = set(nodes.keys())
    edges = {
        edge_id: edge
        for edge_id, edge in assets["edges"].items()
        if edge_id[0] in node_set and edge_id[1] in node_set
    }

    topology = assets["NetworkTopology"](nodes.values(), edges.values())
    paths = _build_paths(topology)
    task_graph = assets["build_task_graph"](tasks)

    return {
        "nodes": nodes,
        "edges": edges,
        "tasks": tasks,
        "task_graph": task_graph,
        "paths": paths,
    }


def _build_nodes_with_usage(
    base_nodes: Dict[str, object],
    usage: Dict[str, Dict[str, float]],
    NodeType: type,
) -> Dict[str, object]:
    updated: Dict[str, object] = {}
    for node_id, node in base_nodes.items():
        used = usage.get(node_id, {})
        updated[node_id] = NodeType(
            node_id=node.node_id,
            cpu_capacity=node.cpu_capacity,
            memory_capacity=node.memory_capacity,
            network_capacity=node.network_capacity,
            used_cpu=used.get("cpu", node.used_cpu),
            used_memory=used.get("memory", node.used_memory),
            used_network=used.get("network", node.used_network),
        )
    return updated


def run_solver_for_usage(
    usage: Dict[str, Dict[str, float]],
    assets: dict,
    context: dict,
) -> SolverResult:
    nodes = _build_nodes_with_usage(context["nodes"], usage, assets["Node"])
    edges = context["edges"]
    tasks = context["tasks"]
    task_graph = context["task_graph"]
    paths = context["paths"]

    topology = assets["NetworkTopology"](nodes.values(), edges.values())
    solver = assets["TaskScheduler"](network=topology)

    t0 = time.perf_counter()
    assignment, leftover, objective_value, status_code = solver.solve(
        tasks,
        running_tasks={},
        paths=paths,
        task_graph=task_graph,
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    print(
        f"solver objective: {objective_value} | "
        f"assignments: {len(assignment)} | "
        f"unassigned: {len(leftover)} | "
        f"status: {status_code}"
    )
    print(f"solver time: {elapsed_ms:.2f} ms")
    return SolverResult(
        elapsed_ms=elapsed_ms,
        objective_value=float(objective_value),
        assignments=len(assignment),
        unassigned=len(leftover),
        status_code=int(status_code),
    )


def _solver_compare_csv_name(task_count: int, solver_node_count: int, query_node_count: int) -> str:
    return (
        "solver_compare_"
        f"tasks{task_count}_solvernodes{solver_node_count}_querynodes{query_node_count}.csv"
    )


def parse_nodes_config(path: str) -> List[str]:
    start = None
    end = None
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line.startswith("start:"):
                start = line.split(":", 1)[1].strip()
            elif line.startswith("end:"):
                end = line.split(":", 1)[1].strip()
    if not start or not end:
        raise ValueError("nodes-config.yaml missing start/end")
    prefix_start, start_num = start[:-3], int(start[-3:])
    prefix_end, end_num = end[:-3], int(end[-3:])
    if prefix_start != prefix_end:
        raise ValueError("node id prefixes do not match")
    nodes = [f"{prefix_start}{i:03d}" for i in range(start_num, end_num + 1)]
    return nodes


def iter_batches(
    total_rows: int,
    nodes: List[str],
    rng: random.Random,
    batch_size: int,
    epoch: int,
) -> Iterable[List[Dict[str, object]]]:
    tasks = [f"T{i:03d}" for i in range(1, 201)]
    for start in range(0, total_rows, batch_size):
        end = min(total_rows, start + batch_size)
        batch: List[Dict[str, object]] = []
        for i in range(start, end):
            node = nodes[i % len(nodes)]
            task = tasks[i % len(tasks)]
            cpu = rng.uniform(0.1, 64.0)
            mem = rng.uniform(0.1, 256.0)
            net = rng.uniform(0.1, 10_000.0)
            batch.append(
                {
                    "epoch": epoch,
                    "node": node,
                    "task": task,
                    "cpu": cpu,
                    "mem": mem,
                    "net": net,
                }
            )
        yield batch


def es_headers(api_key: str | None) -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"ApiKey {api_key}"
    return headers


def reset_es_index(
    es_url: str,
    es_index: str,
    api_key: str | None,
    connect_timeout: float,
    read_timeout: float,
) -> None:
    headers = es_headers(api_key)
    requests.delete(
        f"{es_url}/{es_index}",
        headers=headers,
        timeout=(connect_timeout, read_timeout),
    )
    mapping = {
        "mappings": {
            "properties": {
                "epoch": {"type": "long"},
                "node": {"type": "keyword"},
                "task": {"type": "keyword"},
                "cpu": {"type": "float"},
                "mem": {"type": "float"},
                "net": {"type": "float"},
            }
        }
    }
    resp = requests.put(
        f"{es_url}/{es_index}",
        headers=headers,
        json=mapping,
        timeout=(connect_timeout, read_timeout),
    )
    resp.raise_for_status()


def bulk_ingest_es(
    es_url: str,
    es_index: str,
    api_key: str | None,
    batch: List[Dict[str, object]],
    connect_timeout: float,
    read_timeout: float,
    refresh: str | None,
) -> None:
    headers = es_headers(api_key)
    bulk_url = f"{es_url}/{es_index}/_bulk"
    lines = []
    for row in batch:
        lines.append(json.dumps({"index": {}}))
        lines.append(json.dumps(row))
    payload = "\n".join(lines) + "\n"
    params = {"refresh": refresh} if refresh else None
    resp = requests.post(
        bulk_url,
        headers=headers,
        data=payload,
        params=params,
        timeout=(connect_timeout, read_timeout),
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("errors"):
        raise RuntimeError("Elasticsearch bulk ingestion reported errors")
    items = data.get("items", [])
    if len(items) != len(batch):
        raise RuntimeError(
            f"Elasticsearch bulk ingestion count mismatch: {len(items)} != {len(batch)}"
        )


def ingest_server(
    server_url: str,
    batch: List[Dict[str, object]],
    epoch: int,
    connect_timeout: float,
    read_timeout: float,
    retries: int,
    retry_backoff_s: float,
) -> None:
    ingest_url = f"{server_url}/"
    payload = {
        "epoch": epoch,
        "task": [row["task"] for row in batch],
        "cluster": [row["node"] for row in batch],
        "cpu_cores": [row["cpu"] for row in batch],
        "memory_gb": [row["mem"] for row in batch],
        "network_mbps": [row["net"] for row in batch],
    }
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            resp = requests.post(
                ingest_url,
                json=payload,
                timeout=(connect_timeout, read_timeout),
            )
            resp.raise_for_status()
            data = resp.json()
            inserted = data.get("inserted")
            if inserted is None:
                raise RuntimeError("Server ingest response missing 'inserted'")
            if inserted != len(batch):
                raise RuntimeError(f"Server ingest count mismatch: {inserted} != {len(batch)}")
            return
        except (requests.ReadTimeout, requests.ConnectionError) as err:
            last_err = err
            if attempt >= retries:
                raise
            sleep_s = retry_backoff_s * (2 ** attempt)
            time.sleep(sleep_s)
    if last_err:
        raise last_err


def start_server(log_path: Path | None = None, truncate_log: bool = False) -> subprocess.Popen:
    server_dir = Path("single_node_server/network-control-server")
    stdout_target = None
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        mode = "w" if truncate_log else "a"
        stdout_target = open(log_path, mode, encoding="utf-8")
    proc = subprocess.Popen(
        ["cargo", "run"],
        cwd=server_dir,
        stdout=stdout_target or subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        text=True,
    )
    if stdout_target is not None:
        proc._log_fh = stdout_target
    return proc


def wait_for_server(
    server_url: str,
    timeout_s: float,
    connect_timeout: float,
    read_timeout: float,
) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            resp = requests.get(
                server_url,
                timeout=(connect_timeout, read_timeout),
            )
            if resp.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(0.5)
    raise RuntimeError("server did not become ready")


def stop_server(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        log_fh = getattr(proc, "_log_fh", None)
        if log_fh:
            log_fh.close()
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        log_fh = getattr(proc, "_log_fh", None)
        if log_fh:
            log_fh.close()
        return
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


def query_server_batch(
    server_url: str,
    nodes: List[str],
    connect_timeout: float,
    read_timeout: float,
) -> Tuple[dict, float]:
    url = f"{server_url}/cluster-metrics/_batch"
    payload = {
        "keys": nodes,
        "fields": ["cpu_cores", "memory_gb", "network_mbps"],
        "aggs": ["percentiles", "cumulative"],
        "percents": [0, 50, 90, 100],
    }
    t0 = time.perf_counter()
    resp = requests.post(
        url,
        json=payload,
        timeout=(connect_timeout, read_timeout),
    )
    resp.raise_for_status()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return resp.json(), elapsed_ms


def query_es_nodes(
    es_url: str,
    es_index: str,
    api_key: str | None,
    nodes: List[str],
    connect_timeout: float,
    read_timeout: float,
    epoch: int,
) -> Tuple[dict, float]:
    headers = es_headers(api_key)
    url = f"{es_url}/{es_index}/_search"
    results: Dict[str, Dict[str, object]] = {}
    t0 = time.perf_counter()
    for node in nodes:
        payload = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"node": node}},
                        {"term": {"epoch": epoch}},
                    ]
                }
            },
            "aggs": {
                "cpu_pct": {"percentiles": {"field": "cpu", "percents": [0, 50, 90, 100]}},
                "mem_pct": {"percentiles": {"field": "mem", "percents": [0, 50, 90, 100]}},
                "net_pct": {"percentiles": {"field": "net", "percents": [0, 50, 90, 100]}},
                "cpu_sum": {"sum": {"field": "cpu"}},
                "mem_sum": {"sum": {"field": "mem"}},
                "net_sum": {"sum": {"field": "net"}},
            },
        }
        resp = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=(connect_timeout, read_timeout),
        )
        resp.raise_for_status()
        results[node] = resp.json()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return results, elapsed_ms


def compare_results(server_json: dict, es_json: dict) -> float:
    max_pct = 0.0
    server_results = {item["key"]: item for item in server_json.get("results", [])}
    for node_id, es_result in es_json.items():
        server_entry = server_results.get(node_id)
        if not server_entry:
            continue
        server_pct = server_entry.get("percentiles") or {}
        server_cum = server_entry.get("cumulative") or {}
        aggs = es_result.get("aggregations", {})
        for field_key, es_name in [
            ("cpu_cores", "cpu"),
            ("memory_gb", "mem"),
            ("network_mbps", "net"),
        ]:
            pct_values = server_pct.get(field_key, {})
            es_pct = aggs.get(f"{es_name}_pct", {}).get("values", {})
            for pct in [0, 50, 90, 100]:
                s_val = pct_values.get(str(pct))
                e_val = es_pct.get(str(float(pct)))
                if s_val is None or e_val is None:
                    continue
                pct_diff = _pct_diff(float(s_val), float(e_val))
                if pct_diff >= 2.0 and pct_diff > max_pct:
                    max_pct = pct_diff
            s_cum = server_cum.get(field_key)
            e_cum = aggs.get(f"{es_name}_sum", {}).get("value")
            if s_cum is not None and e_cum is not None:
                pct_diff = _pct_diff(float(s_cum), float(e_cum))
                if pct_diff >= 2.0 and pct_diff > max_pct:
                    max_pct = pct_diff
    return max_pct


def _pct_diff(server_val: float, es_val: float) -> float:
    denom = abs(es_val) if abs(es_val) > 1e-9 else 1e-9
    return abs(server_val - es_val) / denom * 100.0


def format_compact(
    server_json: dict, es_json: dict, nodes: List[str]
) -> List[str]:
    lines: List[str] = []
    server_results = {item["key"]: item for item in server_json.get("results", [])}
    fields = [
        ("cpu_cores", "cpu"),
        ("memory_gb", "mem"),
        ("network_mbps", "net"),
    ]
    percents = [0, 50, 90, 100]
    for node_id in nodes:
        server_entry = server_results.get(node_id)
        es_entry = es_json.get(node_id)
        if not server_entry or not es_entry:
            lines.append(f"{node_id}: missing data in server or ES")
            continue
        server_pct = server_entry.get("percentiles") or {}
        server_cum = server_entry.get("cumulative") or {}
        aggs = es_entry.get("aggregations", {})

        node_lines: List[str] = []
        for field_key, es_name in fields:
            pct_values = server_pct.get(field_key, {})
            es_pct = aggs.get(f"{es_name}_pct", {}).get("values", {})
            pieces = []
            for pct in percents:
                s_val = pct_values.get(str(pct))
                e_val = es_pct.get(str(float(pct)))
                if s_val is None or e_val is None:
                    continue
                else:
                    pct_diff = _pct_diff(float(s_val), float(e_val))
                    if pct_diff >= 2.0:
                        pieces.append(
                            f"p{pct}:{float(s_val):.3f}/{float(e_val):.3f}({pct_diff:.2f}%)"
                        )
            s_cum = server_cum.get(field_key)
            e_cum = aggs.get(f"{es_name}_sum", {}).get("value")
            cum_piece = ""
            if s_cum is not None and e_cum is not None:
                pct_diff = _pct_diff(float(s_cum), float(e_cum))
                if pct_diff >= 2.0:
                    cum_piece = f"sum:{float(s_cum):.3f}/{float(e_cum):.3f}({pct_diff:.2f}%)"
            if pieces or cum_piece:
                detail = " ".join(pieces)
                if cum_piece:
                    detail = f"{detail} {cum_piece}".strip()
                node_lines.append(f"  {field_key}: {detail}")
        if node_lines:
            lines.append(f"{node_id}:")
            lines.extend(node_lines)
    return lines


def plot_results(results: List[SweepResult], out_path: Path) -> None:
    import matplotlib.pyplot as plt

    xs = [r.epoch for r in results]
    server_query = [r.server_query_ms for r in results]
    server_solver = [r.server_solver_ms for r in results]
    server_total = [r.server_total_ms for r in results]
    es_query = [r.es_query_ms for r in results]
    es_solver = [r.es_solver_ms for r in results]
    es_total = [r.es_total_ms for r in results]

    plt.figure(figsize=(11, 7))
    plt.plot(xs, server_query, label="Server query (ms)")
    plt.plot(xs, server_solver, label="Server solver (ms)")
    plt.plot(xs, es_query, label="ES query (ms)")
    plt.plot(xs, es_solver, label="ES solver (ms)")
    plt.xlabel("Epoch")
    plt.ylabel("Time (ms)")
    plt.title("Query + Solver Time vs Epoch")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path)

    total_plot = out_path.with_name(f"{out_path.stem}_total{out_path.suffix}")
    plt.figure(figsize=(11, 7))
    plt.plot(xs, server_total, label="Server total (ms)")
    plt.plot(xs, es_total, label="ES total (ms)")
    plt.xlabel("Epoch")
    plt.ylabel("Time (ms)")
    plt.title("Total Time vs Epoch")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(total_plot)


def main() -> None:
    args = parse_args()
    nodes = parse_nodes_config(args.nodes_config)
    rng = random.Random(args.seed)

    out_csv = Path(args.out_csv)
    out_plot = Path(args.out_plot)

    results: List[SweepResult] = []

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    csv_exists = out_csv.exists()
    csv_mode = "w" if args.truncate_csv else "a"
    csv_file = open(out_csv, csv_mode, newline="")
    writer = csv.writer(csv_file)
    if args.truncate_csv or not csv_exists:
        writer.writerow(
            [
                "timestamp_utc",
                "epoch",
                "server_query_ms",
                "server_solver_ms",
                "server_total_ms",
                "es_query_ms",
                "es_solver_ms",
                "es_total_ms",
                "solver_task_count",
                "solver_node_count",
                "query_node_count",
                "all_nodes_count",
                "all_tasks_count",
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

    solver_compare_file = None
    solver_compare_writer = None

    server_log_path = None if args.server_log == "-" else Path(args.server_log)
    proc = start_server(server_log_path, truncate_log=args.truncate_server_log)
    try:
        wait_for_server(
            args.server_url,
            args.server_ready_timeout,
            args.connect_timeout,
            args.query_timeout,
        )

        assets = None
        solver_context = None
        if args.run_solver:
            solver_dir = Path(args.solver_data_dir)
            print(f"Loading solver inputs from {solver_dir} ...")
            assets = _load_solver_assets(solver_dir)

        if args.run_solver:
            print("Solver will run once per epoch after each query.")

            solver_context = _build_solver_context(
                assets,
                task_count=args.solver_task_count,
                node_count=args.solver_node_count,
            )
            print(
                "Solver setup: "
                f"tasks={len(solver_context['tasks'])}/{len(assets['tasks'])} | "
                f"nodes={len(solver_context['nodes'])}/{len(assets['nodes'])}"
            )

            solver_compare_path = Path(
                _solver_compare_csv_name(
                    args.solver_task_count,
                    args.solver_node_count,
                    args.query_node_count,
                )
            )
            solver_compare_file = open(solver_compare_path, "w", newline="")
            solver_compare_writer = csv.writer(solver_compare_file)
            solver_compare_writer.writerow(
                [
                    "timestamp_utc",
                    "epoch",
                    "server_objective",
                    "server_assignments",
                    "server_unassigned",
                    "server_status",
                    "server_solver_ms",
                    "es_objective",
                    "es_assignments",
                    "es_unassigned",
                    "es_status",
                    "es_solver_ms",
                ]
            )
            solver_compare_file.flush()

        query_nodes = _select_first_n(nodes, args.query_node_count)
        if args.solver_node_count > 0 and args.query_node_count > 0:
            if args.query_node_count > args.solver_node_count:
                print(
                    "Warning: query_node_count exceeds solver_node_count; "
                    "clamping query_node_count to solver_node_count."
                )
                query_nodes = _select_first_n(nodes, args.solver_node_count)
        if args.query_node_count > 0:
            print(f"Query setup: nodes={len(query_nodes)}/{len(nodes)}")

        for epoch in range(args.start_epoch, args.end_epoch + 1):
            print(f"\n=== Epoch {epoch} ===")
            total_rows = args.rows_per_epoch
            total_batches = (total_rows + args.batch_size - 1) // args.batch_size
            log_every = max(1, total_batches // 10)
            for batch_idx, batch in enumerate(
                iter_batches(total_rows, nodes, rng, args.batch_size, epoch), start=1
            ):
                ingest_server(
                    args.server_url,
                    batch,
                    epoch,
                    args.connect_timeout,
                    args.ingest_timeout,
                    args.ingest_retries,
                    args.ingest_retry_backoff,
                )
                is_last_batch = batch_idx == total_batches
                bulk_ingest_es(
                    args.es_url,
                    args.es_index,
                    args.es_api_key,
                    batch,
                    args.connect_timeout,
                    args.es_timeout,
                    "wait_for" if is_last_batch else None,
                )
                if batch_idx % log_every == 0 or batch_idx == total_batches:
                    print(
                        f"  ingest progress: {batch_idx}/{total_batches} batches "
                        f"({batch_idx * 100 // total_batches}%)"
                    )

            server_json, server_rtt = query_server_batch(
                args.server_url,
                query_nodes,
                args.connect_timeout,
                args.query_timeout,
            )
            es_json, es_rtt = query_es_nodes(
                args.es_url,
                args.es_index,
                args.es_api_key,
                query_nodes,
                args.connect_timeout,
                args.es_timeout,
                epoch,
            )

            server_solver_ms = 0.0
            es_solver_ms = 0.0
            if args.run_solver and assets is not None and solver_context is not None:
                server_usage = _extract_server_usage(server_json)
                es_usage = _extract_es_usage(es_json)
                server_result = run_solver_for_usage(server_usage, assets, solver_context)
                es_result = run_solver_for_usage(es_usage, assets, solver_context)
                server_solver_ms = server_result.elapsed_ms
                es_solver_ms = es_result.elapsed_ms
                if solver_compare_writer is not None and solver_compare_file is not None:
                    solver_compare_writer.writerow(
                        [
                            datetime.now(timezone.utc).isoformat(),
                            epoch,
                            f"{server_result.objective_value:.6f}",
                            server_result.assignments,
                            server_result.unassigned,
                            server_result.status_code,
                            f"{server_result.elapsed_ms:.4f}",
                            f"{es_result.objective_value:.6f}",
                            es_result.assignments,
                            es_result.unassigned,
                            es_result.status_code,
                            f"{es_result.elapsed_ms:.4f}",
                        ]
                    )
                    solver_compare_file.flush()

            max_diff = compare_results(server_json, es_json)
            server_rtt_total = server_rtt + server_solver_ms
            es_rtt_total = es_rtt + es_solver_ms
            results.append(
                SweepResult(
                    epoch=epoch,
                    server_query_ms=server_rtt,
                    server_solver_ms=server_solver_ms,
                    server_total_ms=server_rtt_total,
                    es_query_ms=es_rtt,
                    es_solver_ms=es_solver_ms,
                    es_total_ms=es_rtt_total,
                )
            )
            print(
                f"server+solver RTT: {server_rtt_total:.2f} ms | "
                f"ES+solver RTT: {es_rtt_total:.2f} ms | "
                f"max diff >=2%: {max_diff:.2f}%"
            )
            print("comparisons:")
            for line in format_compact(server_json, es_json, query_nodes):
                print(line)
            writer.writerow(
                [
                    datetime.now(timezone.utc).isoformat(),
                    epoch,
                    f"{server_rtt:.4f}",
                    f"{server_solver_ms:.4f}",
                    f"{server_rtt_total:.4f}",
                    f"{es_rtt:.4f}",
                    f"{es_solver_ms:.4f}",
                    f"{es_rtt_total:.4f}",
                    args.solver_task_count,
                    args.solver_node_count,
                    args.query_node_count,
                    len(nodes),
                    len(assets["tasks"]) if assets is not None else 0,
                ]
            )
            csv_file.flush()
    finally:
        stop_server(proc)
        csv_file.close()
        if solver_compare_file is not None:
            solver_compare_file.close()

    plot_results(results, out_plot)
    total_plot = out_plot.with_name(f"{out_plot.stem}_total{out_plot.suffix}")
    print(f"\nWrote {out_csv}, {out_plot}, and {total_plot}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Sketch server vs Elasticsearch: does the telemetry source change assignment?

Paper setup, but the cluster and the per-epoch telemetry come from raw_data/:

  topology   data/raw_topology/{nodes,edges,tasks}.jsonl  (see raw_data_prep.py)
  telemetry  raw_data/synthetic_cpu_var.csv -- 996,800 rows spanning exactly
             300 s, i.e. one epoch's worth, replayed once per epoch with fresh
             lognormal jitter so epochs are independent draws

Each epoch:
  1. the same rows are ingested into the sketch server and into Elasticsearch
  2. both are asked for per-node CPU/memory quantiles over that epoch
  3. tasks whose `arrival_offset_s` falls in this epoch join the pending queue
  4. each backend's answer drives its OWN scheduling simulation -- independent
     pending queues, running sets and completion clocks -- so the assignment
     counts accumulate the effect of the telemetry difference instead of being
     re-synchronised every epoch

Tasks arrive on a rolling schedule (see raw_data_prep.py), so the queue keeps a
backlog instead of draining after a few epochs. `--runs` repeats the whole
trajectory with a fresh telemetry jitter draw and a restarted server, which is
what separates a real telemetry effect from solver/jitter noise: a per-epoch
gap of a few tasks means nothing at n=1.

Every solve records its MILP status. `FEASIBLE` means the solve stopped at
`--solver-time-limit-s` with a valid but not provably optimal solution -- such
an epoch's assignment count is not attributable to the telemetry source, so it
must be excluded (or the queue made smaller) rather than read as a result.

Node resource state handed to the MILP is
    used_cpu    = telemetry_quantile(cpu_usage)            + running task CPU
    used_memory = capacity - telemetry_quantile(mem_avail) + running task memory
because raw_data's cpu_var.csv is explicitly background usage with no tasks
running; assigned tasks add on top of it.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Sequence

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rtt_sweep_common import (  # noqa: E402
    DEFAULT_ES_API_KEY,
    es_headers,
    resolve_repo_path,
    start_server,
    stop_server,
    wait_for_server,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SOLVER_ROOT = REPO_ROOT / "solver_experimental"
DEFAULT_TOPOLOGY_DIR = REPO_ROOT / "data" / "raw_topology"

MILLI = 1000.0
BYTES_PER_GB = 1e9
BACKENDS = ("sketch", "es")


# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------

def _default_raw_dir() -> Path:
    """First existing of $RAW_DATA_DIR, ~/raw_data, ~/Downloads/raw_data."""
    candidates = []
    env = os.environ.get("RAW_DATA_DIR")
    if env:
        candidates.append(Path(env))
    candidates += [Path.home() / "raw_data", Path.home() / "Downloads" / "raw_data"]
    for c in candidates:
        if c.is_dir():
            return c
    return candidates[-1]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--telemetry-csv", type=Path,
                   default=_default_raw_dir() / "synthetic_cpu_var.csv")
    p.add_argument("--topology-dir", type=Path, default=DEFAULT_TOPOLOGY_DIR)
    p.add_argument("--out-csv", type=str, default="data/raw_data_assignment.csv")
    p.add_argument("--nodes-csv", type=str, default="data/raw_data_assignment_nodes.csv")

    p.add_argument("--runs", type=int, default=1,
                   help="Independent repeats. Each gets a fresh jitter draw and a restarted server.")
    p.add_argument("--epochs", type=int, default=10)
    p.add_argument("--epoch-length-s", type=float, default=300.0,
                   help="Must match raw_data_prep.py --epoch-length-s.")
    p.add_argument(
        "--task-ttl-epochs", type=int, default=0,
        help=(
            "Drop a task after this many epochs in the pending queue (0 = never). "
            "Guards against tasks that never fit growing the MILP without bound."
        ),
    )
    p.add_argument(
        "--rows-per-epoch", type=int, default=0,
        help="Subsample the telemetry to this many rows per epoch (0 = use all ~996,800).",
    )
    p.add_argument(
        "--usage-quantile", type=float, default=50.0,
        help="Quantile of the epoch's telemetry used as the resource estimate (50 = paper's p50).",
    )
    p.add_argument(
        "--epoch-jitter", type=float, default=0.02,
        help="Per-epoch lognormal sigma applied to telemetry values (0 disables; matches gen_synth.py).",
    )
    p.add_argument("--seed", type=int, default=20260903)

    p.add_argument("--solver-backend", type=str, choices=["CBC", "SCIP", "GLPK"], default="SCIP")
    p.add_argument("--solver-time-limit-s", type=float, default=60.0)

    p.add_argument("--server-url", type=str, default="http://localhost:10101")
    p.add_argument("--server-log", type=str, default="logs/server_raw_data.log")
    p.add_argument("--server-ready-timeout", type=float, default=60.0)
    p.add_argument("--skip-server-start", action="store_true", default=False)
    p.add_argument("--es-url", type=str, default="http://localhost:9200")
    p.add_argument("--es-index", type=str, default="raw-cluster-metrics")
    p.add_argument("--es-api-key", type=str, default=DEFAULT_ES_API_KEY)

    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--connect-timeout", type=float, default=5.0)
    p.add_argument("--ingest-timeout", type=float, default=180.0)
    p.add_argument("--query-timeout", type=float, default=180.0)
    return p.parse_args()


# ---------------------------------------------------------------------------
# Telemetry
# ---------------------------------------------------------------------------

@dataclass
class Telemetry:
    node_ids: List[str]
    node_idx: "object"   # numpy int arrays, kept untyped to avoid a hard import here
    cpu_cores: "object"
    mem_gb: "object"


def load_telemetry(path: Path, keep_nodes: Sequence[str], rows_per_epoch: int, seed: int) -> Telemetry:
    import numpy as np

    keep = set(keep_nodes)
    order: Dict[str, int] = {n: i for i, n in enumerate(sorted(keep))}

    idx_list: List[int] = []
    cpu_list: List[float] = []
    mem_list: List[float] = []
    with open(path, newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        col = {name: i for i, name in enumerate(header)}
        c_node = col["scenario_node"]
        c_cpu = col["cpu_usage_millicores"]
        c_mem = col["memory_available"]
        for row in reader:
            node = row[c_node]
            i = order.get(node)
            if i is None:
                continue
            idx_list.append(i)
            cpu_list.append(float(row[c_cpu]))
            mem_list.append(float(row[c_mem]))

    node_idx = np.asarray(idx_list, dtype=np.int16)
    cpu = np.asarray(cpu_list, dtype=np.float32) / MILLI          # millicores -> cores
    mem = np.asarray(mem_list, dtype=np.float32) / BYTES_PER_GB   # bytes -> GB available

    if rows_per_epoch and rows_per_epoch < len(node_idx):
        rng = np.random.default_rng(seed)
        pick = np.sort(rng.choice(len(node_idx), size=rows_per_epoch, replace=False))
        node_idx, cpu, mem = node_idx[pick], cpu[pick], mem[pick]

    return Telemetry(node_ids=sorted(keep), node_idx=node_idx, cpu_cores=cpu, mem_gb=mem)


def epoch_values(tel: Telemetry, epoch: int, jitter: float, seed: int, run: int = 0):
    """Values for one epoch: the same replay with fresh multiplicative jitter."""
    import numpy as np

    if jitter <= 0.0:
        return tel.cpu_cores, tel.mem_gb
    rng = np.random.default_rng(seed + run * 1_000_003 + epoch)
    n = len(tel.cpu_cores)
    return (
        tel.cpu_cores * np.exp(rng.normal(0.0, jitter, n)).astype("float32"),
        tel.mem_gb * np.exp(rng.normal(0.0, jitter, n)).astype("float32"),
    )


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------

def reset_es_index(args: argparse.Namespace) -> None:
    headers = es_headers(args.es_api_key)
    requests.delete(f"{args.es_url}/{args.es_index}", headers=headers,
                    timeout=(args.connect_timeout, args.ingest_timeout))
    mapping = {
        "mappings": {
            "properties": {
                "epoch": {"type": "long"},
                "node": {"type": "keyword"},
                "cpu": {"type": "float"},
                "mem": {"type": "float"},
            }
        }
    }
    resp = requests.put(f"{args.es_url}/{args.es_index}", headers=headers, json=mapping,
                        timeout=(args.connect_timeout, args.ingest_timeout))
    resp.raise_for_status()


def ingest_epoch(args: argparse.Namespace, tel: Telemetry, epoch: int, cpu, mem) -> tuple[float, float]:
    """Push one epoch to both backends. Returns (sketch_ms, es_ms)."""
    headers = es_headers(args.es_api_key)
    bulk_url = f"{args.es_url}/{args.es_index}/_bulk"
    server_url = f"{args.server_url}/cluster-metrics"
    names = tel.node_ids
    idx = tel.node_idx
    total = len(idx)
    action = json.dumps({"index": {}})

    sketch_ms = 0.0
    es_ms = 0.0
    n_batches = (total + args.batch_size - 1) // args.batch_size
    log_every = max(1, n_batches // 10)

    for b, start in enumerate(range(0, total, args.batch_size), start=1):
        end = min(total, start + args.batch_size)
        keys = [names[int(i)] for i in idx[start:end]]
        cpu_slice = [round(float(v), 6) for v in cpu[start:end]]
        mem_slice = [round(float(v), 6) for v in mem[start:end]]

        payload = {
            "epoch": epoch,
            "cluster": keys,
            "cpu_cores": cpu_slice,
            "memory_gb": mem_slice,
        }
        t0 = time.perf_counter()
        r = requests.post(server_url, json=payload,
                          timeout=(args.connect_timeout, args.ingest_timeout))
        r.raise_for_status()
        inserted = r.json().get("inserted")
        if inserted != len(keys):
            raise RuntimeError(f"sketch ingest mismatch: {inserted} != {len(keys)}")
        sketch_ms += (time.perf_counter() - t0) * 1000.0

        lines = []
        for k, c, m in zip(keys, cpu_slice, mem_slice):
            lines.append(action)
            lines.append(json.dumps({"epoch": epoch, "node": k, "cpu": c, "mem": m}))
        body = "\n".join(lines) + "\n"
        params = {"refresh": "wait_for"} if end >= total else None
        t0 = time.perf_counter()
        r = requests.post(bulk_url, headers=headers, data=body, params=params,
                          timeout=(args.connect_timeout, args.ingest_timeout))
        r.raise_for_status()
        if r.json().get("errors"):
            raise RuntimeError("ES bulk reported errors")
        es_ms += (time.perf_counter() - t0) * 1000.0

        if b % log_every == 0 or b == n_batches:
            print(f"    ingest {b}/{n_batches}  sketch={sketch_ms:.0f}ms es={es_ms:.0f}ms", flush=True)

    return sketch_ms, es_ms


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

def _pick(values: dict, q: float) -> float:
    for key in (str(int(q)) if float(q).is_integer() else None, str(q), f"{float(q):.1f}"):
        if key is not None and key in values and values[key] is not None:
            return float(values[key])
    raise KeyError(f"percentile {q} not in {list(values)}")


def query_sketch(args: argparse.Namespace, nodes: List[str], percents: List[float]):
    payload = {
        "keys": nodes,
        "fields": ["cpu_cores", "memory_gb"],
        "aggs": ["percentiles"],
        "percents": percents,
    }
    t0 = time.perf_counter()
    r = requests.post(f"{args.server_url}/cluster-metrics/_batch", json=payload,
                      timeout=(args.connect_timeout, args.query_timeout))
    r.raise_for_status()
    elapsed = (time.perf_counter() - t0) * 1000.0
    out: Dict[str, Dict[str, float]] = {}
    for item in r.json().get("results", []):
        pct = item.get("percentiles") or {}
        out[item["key"]] = {
            f"cpu_{q}": _pick(pct.get("cpu_cores", {}), q) for q in percents
        } | {
            f"mem_{q}": _pick(pct.get("memory_gb", {}), q) for q in percents
        }
    return out, elapsed


def query_es(args: argparse.Namespace, nodes: List[str], percents: List[float], epoch: int):
    headers = es_headers(args.es_api_key)
    url = f"{args.es_url}/{args.es_index}/_search"
    out: Dict[str, Dict[str, float]] = {}
    t0 = time.perf_counter()
    for node in nodes:
        body = {
            "size": 0,
            "query": {"bool": {"filter": [{"term": {"node": node}}, {"term": {"epoch": epoch}}]}},
            "aggs": {
                "cpu_pct": {"percentiles": {"field": "cpu", "percents": percents}},
                "mem_pct": {"percentiles": {"field": "mem", "percents": percents}},
            },
        }
        r = requests.post(url, headers=headers, json=body, params={"request_cache": "false"},
                          timeout=(args.connect_timeout, args.query_timeout))
        r.raise_for_status()
        aggs = r.json()["aggregations"]
        out[node] = {
            f"cpu_{q}": _pick(aggs["cpu_pct"]["values"], q) for q in percents
        } | {
            f"mem_{q}": _pick(aggs["mem_pct"]["values"], q) for q in percents
        }
    return out, (time.perf_counter() - t0) * 1000.0


# ---------------------------------------------------------------------------
# Solver
# ---------------------------------------------------------------------------

def load_solver_assets(topology_dir: Path) -> dict:
    if str(SOLVER_ROOT) not in sys.path:
        sys.path.insert(0, str(SOLVER_ROOT))
    from scheduler.load_info import load_edges, load_nodes, load_tasks  # type: ignore
    from python_solver.src.network_controller.solver import (  # type: ignore
        Edge as OrtEdge,
        NetworkControllerSolver,
        Node as OrtNode,
        Task as OrtTask,
        TaskCommunication as OrtTaskCommunication,
    )

    raw_nodes = load_nodes(topology_dir / "nodes.jsonl")
    raw_edges = load_edges(topology_dir / "edges.jsonl")
    raw_tasks = load_tasks(topology_dir / "tasks.jsonl")

    nodes = {
        nid: OrtNode(node_id=n.node_id, cpu_capacity=n.cpu_capacity,
                     memory_capacity=n.memory_capacity, used_cpu=0.0, used_memory=0.0)
        for nid, n in raw_nodes.items()
    }
    edges = {
        eid: OrtEdge(edge_id=eid, capacity=e.capacity, used_bandwidth=0.0)
        for eid, e in raw_edges.items()
    }
    tasks = {}
    for tid, t in raw_tasks.items():
        comms = tuple(
            OrtTaskCommunication(target_task_id=peer, bandwidth=bw)
            for peer, bw in t.peer_bandwidths.items()
        )
        tasks[tid] = OrtTask(
            task_id=t.task_id, cpu=t.initial_cpu, memory=t.initial_memory,
            bandwidth=sum(t.peer_bandwidths.values()), priority=1.0, communications=comms,
        )
    durations = {tid: t.duration_s for tid, t in raw_tasks.items()}
    arrivals = sorted((t.arrival_offset_s, tid) for tid, t in raw_tasks.items())
    return {
        "nodes": nodes, "edges": edges, "tasks": tasks, "durations": durations,
        "arrivals": arrivals,
        "NetworkControllerSolver": NetworkControllerSolver, "OrtNode": OrtNode,
    }


@dataclass
class SimState:
    """One backend's independent scheduling trajectory."""
    unreleased: List[tuple]                                   # (arrival_offset_s, task_id), ascending
    pending: List[str] = field(default_factory=list)
    running: Dict[str, tuple] = field(default_factory=dict)   # task_id -> (node_id, finish_epoch)
    completed: set = field(default_factory=set)
    dropped: set = field(default_factory=set)
    queued_since: Dict[str, int] = field(default_factory=dict)

    def release(self, epoch: int, epoch_length_s: float) -> int:
        """Admit tasks that arrive during this epoch."""
        cutoff = (epoch + 1) * epoch_length_s
        n = 0
        while self.unreleased and self.unreleased[0][0] < cutoff:
            _, tid = self.unreleased.pop(0)
            self.pending.append(tid)
            self.queued_since[tid] = epoch
            n += 1
        return n

    def retire(self, epoch: int) -> int:
        done = [t for t, (_, fin) in self.running.items() if fin <= epoch]
        for t in done:
            self.running.pop(t)
            self.completed.add(t)
        return len(done)

    def expire(self, epoch: int, ttl_epochs: int) -> int:
        """Drop tasks that have waited too long, so the MILP stays bounded."""
        if ttl_epochs <= 0:
            return 0
        stale = [t for t in self.pending if epoch - self.queued_since.get(t, epoch) >= ttl_epochs]
        for t in stale:
            self.pending.remove(t)
            self.dropped.add(t)
        return len(stale)

    def drained(self) -> bool:
        return not self.unreleased and not self.pending and not self.running

    def running_load(self, tasks: dict) -> Dict[str, tuple[float, float]]:
        load: Dict[str, list] = {}
        for tid, (node_id, _) in self.running.items():
            t = tasks[tid]
            acc = load.setdefault(node_id, [0.0, 0.0])
            acc[0] += t.cpu
            acc[1] += t.memory
        return {k: (v[0], v[1]) for k, v in load.items()}


def solve_for(assets: dict, telemetry_usage: Dict[str, Dict[str, float]], state: SimState,
              args: argparse.Namespace) -> tuple[object, float]:
    OrtNode = assets["OrtNode"]
    load = state.running_load(assets["tasks"])
    nodes = {}
    for nid, base in assets["nodes"].items():
        tel = telemetry_usage.get(nid, {"cpu": 0.0, "memory": 0.0})
        run_cpu, run_mem = load.get(nid, (0.0, 0.0))
        nodes[nid] = OrtNode(
            node_id=nid,
            cpu_capacity=base.cpu_capacity,
            memory_capacity=base.memory_capacity,
            used_cpu=min(tel["cpu"] + run_cpu, base.cpu_capacity),
            used_memory=min(tel["memory"] + run_mem, base.memory_capacity),
        )
    solver = assets["NetworkControllerSolver"](nodes, assets["edges"],
                                               solver_backend=args.solver_backend)
    task_list = [assets["tasks"][t] for t in state.pending]
    t0 = time.perf_counter()
    # A deadline miss with no solution found is recorded as `NOT_SOLVED` and zero
    # placements rather than killing the run; the status column marks the epoch.
    result = solver.solve(task_list, time_limit_s=args.solver_time_limit_s,
                          raise_on_no_solution=False)
    return result, (time.perf_counter() - t0) * 1000.0


def telemetry_to_usage(readings: Dict[str, Dict[str, float]], assets: dict,
                       q: float, q_lo: float) -> Dict[str, Dict[str, float]]:
    """cpu: quantile of usage. memory: capacity minus the matching low quantile of availability."""
    usage: Dict[str, Dict[str, float]] = {}
    for nid, base in assets["nodes"].items():
        r = readings.get(nid)
        if r is None:
            usage[nid] = {"cpu": 0.0, "memory": 0.0}
            continue
        avail = r[f"mem_{q_lo}"]
        usage[nid] = {
            "cpu": max(r[f"cpu_{q}"], 0.0),
            "memory": max(base.memory_capacity - avail, 0.0),
        }
    return usage


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

CSV_HEADER = [
    "timestamp_utc", "run", "epoch", "rows", "usage_quantile", "arrivals",
    "sketch_ingest_ms", "es_ingest_ms", "sketch_query_ms", "es_query_ms",
    "sketch_solver_ms", "es_solver_ms",
    "sketch_solver_status", "es_solver_status",
    "sketch_assigned", "es_assigned",
    "sketch_pending_before", "es_pending_before",
    "sketch_running", "es_running",
    "sketch_completed", "es_completed",
    "sketch_dropped", "es_dropped",
    "sketch_objective", "es_objective",
    "cpu_q_max_abs_err", "cpu_q_max_rel_err_pct", "mem_q_max_abs_err",
    "solver_backend",
]


def run_one(args: argparse.Namespace, run: int, tel: Telemetry, assets: dict,
            node_ids: List[str], q: float, q_lo: float, percents: List[float],
            writer, nwriter, fh, nfh) -> None:
    """One independent trajectory: fresh queues, fresh jitter, restarted server."""
    reset_es_index(args)

    server_proc = None
    if not args.skip_server_start:
        os.environ["NCS_CONFIG_PATH"] = str(
            REPO_ROOT / "single_node_server/network-control-server/raw-data-config.yaml"
        )
        server_proc = start_server(resolve_repo_path(args.server_log),
                                   truncate_log=(run == 0))

    states = {b: SimState(unreleased=list(assets["arrivals"])) for b in BACKENDS}

    try:
        if server_proc is not None:
            wait_for_server(args.server_url, args.server_ready_timeout,
                            args.connect_timeout, args.query_timeout)

        for epoch in range(args.epochs):
            print(f"\n=== run {run} epoch {epoch} ===", flush=True)
            cpu, mem = epoch_values(tel, epoch, args.epoch_jitter, args.seed, run)

            s_ing, e_ing = ingest_epoch(args, tel, epoch, cpu, mem)
            print(f"  ingest: sketch={s_ing:.0f}ms es={e_ing:.0f}ms", flush=True)

            sketch_read, s_qms = query_sketch(args, node_ids, percents)
            es_read, e_qms = query_es(args, node_ids, percents, epoch)
            print(f"  query:  sketch={s_qms:.1f}ms es={e_qms:.1f}ms", flush=True)

            cpu_abs, cpu_rel, mem_abs = 0.0, 0.0, 0.0
            for nid in node_ids:
                sc, ec = sketch_read[nid][f"cpu_{q}"], es_read[nid][f"cpu_{q}"]
                sm, em = sketch_read[nid][f"mem_{q_lo}"], es_read[nid][f"mem_{q_lo}"]
                cpu_abs = max(cpu_abs, abs(sc - ec))
                if abs(ec) > 1e-9:
                    cpu_rel = max(cpu_rel, abs(sc - ec) / abs(ec) * 100.0)
                mem_abs = max(mem_abs, abs(sm - em))
                nwriter.writerow([run, epoch, nid, f"{sc:.6f}", f"{ec:.6f}",
                                  f"{sm:.6f}", f"{em:.6f}"])
            nfh.flush()
            print(f"  telemetry divergence: cpu max |Δ|={cpu_abs:.6f} cores "
                  f"({cpu_rel:.3f}%), mem max |Δ|={mem_abs:.6f} GB", flush=True)

            row: Dict[str, object] = {}
            arrivals = 0
            for backend, readings in (("sketch", sketch_read), ("es", es_read)):
                st = states[backend]
                st.retire(epoch)
                arrivals = st.release(epoch, args.epoch_length_s)
                st.expire(epoch, args.task_ttl_epochs)
                pending_before = len(st.pending)
                usage = telemetry_to_usage(readings, assets, q, q_lo)
                result, ms = solve_for(assets, usage, st, args)

                finish_at = {}
                for tid, decision in result.decisions.items():
                    span = max(1, math.ceil(assets["durations"][tid] / args.epoch_length_s))
                    finish_at[tid] = (decision.node_id, epoch + span)
                st.running.update(finish_at)
                st.pending = sorted(result.unassigned_tasks)

                row[f"{backend}_solver_ms"] = ms
                row[f"{backend}_solver_status"] = result.status
                row[f"{backend}_assigned"] = len(result.decisions)
                row[f"{backend}_pending_before"] = pending_before
                row[f"{backend}_running"] = len(st.running)
                row[f"{backend}_completed"] = len(st.completed)
                row[f"{backend}_dropped"] = len(st.dropped)
                row[f"{backend}_objective"] = result.objective_value

            capped = [b for b in BACKENDS if row[f"{b}_solver_status"] != "OPTIMAL"]
            print(f"  assigned: sketch={row['sketch_assigned']}/{row['sketch_pending_before']} "
                  f"es={row['es_assigned']}/{row['es_pending_before']} | "
                  f"arrivals={arrivals} | "
                  f"running s={row['sketch_running']} e={row['es_running']} | "
                  f"completed s={row['sketch_completed']} e={row['es_completed']}"
                  + (f" | NOT OPTIMAL: {','.join(capped)}" if capped else ""), flush=True)

            writer.writerow([
                datetime.now(timezone.utc).isoformat(), run, epoch, len(tel.node_idx), q,
                arrivals,
                f"{s_ing:.3f}", f"{e_ing:.3f}", f"{s_qms:.3f}", f"{e_qms:.3f}",
                f"{row['sketch_solver_ms']:.3f}", f"{row['es_solver_ms']:.3f}",
                row["sketch_solver_status"], row["es_solver_status"],
                row["sketch_assigned"], row["es_assigned"],
                row["sketch_pending_before"], row["es_pending_before"],
                row["sketch_running"], row["es_running"],
                row["sketch_completed"], row["es_completed"],
                row["sketch_dropped"], row["es_dropped"],
                f"{row['sketch_objective']:.6f}", f"{row['es_objective']:.6f}",
                f"{cpu_abs:.8f}", f"{cpu_rel:.6f}", f"{mem_abs:.8f}",
                args.solver_backend,
            ])
            fh.flush()

            if all(states[b].drained() for b in BACKENDS):
                print("both backends drained; ending this run.")
                break
    finally:
        if server_proc is not None:
            stop_server(server_proc)


def main() -> None:
    args = parse_args()
    out_csv = resolve_repo_path(args.out_csv)
    nodes_csv = resolve_repo_path(args.nodes_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    assets = load_solver_assets(args.topology_dir)
    node_ids = sorted(assets["nodes"])
    arrival_span = max(a for a, _ in assets["arrivals"]) if assets["arrivals"] else 0.0
    print(f"topology: {len(node_ids)} nodes, {len(assets['edges'])} edges, "
          f"{len(assets['tasks'])} tasks arriving over {arrival_span:.0f}s "
          f"(~{arrival_span / args.epoch_length_s:.0f} epochs)")
    if arrival_span > args.epochs * args.epoch_length_s:
        print(f"  note: --epochs {args.epochs} stops before the last arrival; "
              f"raise it to {math.ceil(arrival_span / args.epoch_length_s)}+ to drain the queue")

    q = args.usage_quantile
    q_lo = round(100.0 - q, 6)
    percents = sorted({q, q_lo})
    print(f"update rule: used_cpu = p{q:g}(cpu_usage), used_mem = capacity - p{q_lo:g}(mem_available)")

    print(f"loading telemetry from {args.telemetry_csv} ...", flush=True)
    tel = load_telemetry(args.telemetry_csv, node_ids, args.rows_per_epoch, args.seed)
    print(f"  {len(tel.node_idx):,} rows over {len(tel.node_ids)} nodes")

    with open(out_csv, "w", newline="") as fh, open(nodes_csv, "w", newline="") as nfh:
        writer = csv.writer(fh)
        writer.writerow(CSV_HEADER)
        nwriter = csv.writer(nfh)
        nwriter.writerow(["run", "epoch", "node", "cpu_sketch", "cpu_es",
                          "mem_avail_sketch", "mem_avail_es"])
        for run in range(args.runs):
            run_one(args, run, tel, assets, node_ids, q, q_lo, percents,
                    writer, nwriter, fh, nfh)

    print(f"\nwrote {out_csv}")
    print(f"wrote {nodes_csv}")


if __name__ == "__main__":
    main()

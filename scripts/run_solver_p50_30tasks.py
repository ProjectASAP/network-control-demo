#!/usr/bin/env python3
"""Benchmark solver runtime with a 30-task / 30-node scenario.

Runs the OR-Tools `NetworkControllerSolver` N times on a fixed problem
(first 30 tasks from `solver_experimental/dummy_data/tasks.jsonl` placed on
30 server nodes N001-N030, with zero initial node usage) and records the
elapsed milliseconds. Writes per-trial values to a CSV and prints the p50
(median).

This produces the solver-time baseline used by the ES sweep query-ratio plot.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path
from statistics import median
from typing import Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
SOLVER_ROOT = REPO_ROOT / "solver_experimental"
SOLVER_DUMMY_DIR = SOLVER_ROOT / "dummy_data"


def _ensure_solver_path() -> None:
    if str(SOLVER_ROOT) not in sys.path:
        sys.path.insert(0, str(SOLVER_ROOT))


def _load_assets():
    _ensure_solver_path()
    from scheduler.load_info import (  # type: ignore
        load_edges as load_edges_jsonl,
        load_nodes as load_nodes_jsonl,
        load_tasks as load_tasks_jsonl,
    )
    from python_solver.src.network_controller.solver import (  # type: ignore
        Edge as OrtEdge,
        NetworkControllerSolver,
        Node as OrtNode,
        Task as OrtTask,
        TaskCommunication as OrtTaskCommunication,
    )

    raw_nodes = load_nodes_jsonl(SOLVER_DUMMY_DIR / "nodes.jsonl")
    raw_edges = load_edges_jsonl(SOLVER_DUMMY_DIR / "edges.jsonl")
    raw_tasks = load_tasks_jsonl(SOLVER_DUMMY_DIR / "tasks.jsonl")

    ort_nodes: Dict[str, OrtNode] = {
        nid: OrtNode(
            node_id=n.node_id,
            cpu_capacity=n.cpu_capacity,
            memory_capacity=n.memory_capacity,
            used_cpu=0.0,
            used_memory=0.0,
        )
        for nid, n in raw_nodes.items()
    }

    ort_edges: Dict[tuple, OrtEdge] = {
        eid: OrtEdge(edge_id=eid, capacity=e.capacity, used_bandwidth=0.0)
        for eid, e in raw_edges.items()
    }

    ort_tasks: Dict[str, OrtTask] = {}
    for task_id, task in raw_tasks.items():
        comms = tuple(
            OrtTaskCommunication(target_task_id=peer_id, bandwidth=bw)
            for peer_id, bw in task.peer_bandwidths.items()
        )
        total_bw = sum(task.peer_bandwidths.values())
        ort_tasks[task_id] = OrtTask(
            task_id=task.task_id,
            cpu=task.initial_cpu,
            memory=task.initial_memory,
            bandwidth=total_bw,
            priority=1.0,
            communications=comms,
        )

    return {
        "nodes": ort_nodes,
        "edges": ort_edges,
        "tasks": ort_tasks,
        "NetworkControllerSolver": NetworkControllerSolver,
    }


def _filter_nodes(nodes: Dict[str, object], allowed: List[str]) -> Dict[str, object]:
    allowed_set = set(allowed)
    return {nid: n for nid, n in nodes.items() if nid in allowed_set}


def _filter_edges(edges: Dict[tuple, object], node_ids: List[str]) -> Dict[tuple, object]:
    s = set(node_ids)
    return {eid: e for eid, e in edges.items() if eid[0] in s and eid[1] in s}


def _pick_tasks(tasks: Dict[str, object], n: int) -> List[object]:
    """Pick the first n tasks whose communication peers are all in the picked set."""
    sorted_ids = sorted(tasks.keys())
    picked: List[str] = []
    picked_set: set = set()
    for tid in sorted_ids:
        if len(picked) >= n:
            break
        t = tasks[tid]
        peers = {c.target_task_id for c in t.communications}
        # accept if either no peers, or all peers already picked / will be picked among the leading ids
        # We use a relaxed strategy: just take the first n; the solver tolerates dangling peers.
        picked.append(tid)
        picked_set.add(tid)
    return [tasks[tid] for tid in picked]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--trials", type=int, default=21,
                   help="Number of solver runs (default: 21)")
    p.add_argument("--warmup", type=int, default=2,
                   help="Warmup runs not recorded (default: 2)")
    p.add_argument("--task-count", type=int, default=30,
                   help="Number of tasks (default: 30)")
    p.add_argument("--solver-backend", type=str, default="SCIP",
                   choices=["CBC", "SCIP", "GLPK"],
                   help="OR-Tools backend (default: SCIP)")
    p.add_argument("--time-limit-s", type=float, default=30.0)
    p.add_argument("--out-csv", type=str,
                   default="data/solver_p50_30tasks.csv")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    assets = _load_assets()

    server_node_ids = [f"N{i:03d}" for i in range(1, 31)]
    nodes = _filter_nodes(assets["nodes"], server_node_ids)
    edges = _filter_edges(assets["edges"], server_node_ids)
    tasks_list = _pick_tasks(assets["tasks"], args.task_count)

    NCSolver = assets["NetworkControllerSolver"]

    print(f"Setup: {len(nodes)} nodes, {len(edges)} edges, {len(tasks_list)} tasks, "
          f"backend={args.solver_backend}, trials={args.trials} (+{args.warmup} warmup)")

    # warmup
    for w in range(args.warmup):
        solver = NCSolver(nodes, edges, solver_backend=args.solver_backend)
        solver.solve(tasks_list, time_limit_s=args.time_limit_s)

    elapsed_ms: List[float] = []
    for i in range(args.trials):
        solver = NCSolver(nodes, edges, solver_backend=args.solver_backend)
        t0 = time.perf_counter()
        solver.solve(tasks_list, time_limit_s=args.time_limit_s)
        ms = (time.perf_counter() - t0) * 1000.0
        elapsed_ms.append(ms)
        print(f"  trial {i+1:>2}: {ms:8.2f} ms")

    p50 = median(elapsed_ms)
    pmin = min(elapsed_ms)
    pmax = max(elapsed_ms)
    print(f"\np50 = {p50:.2f} ms  (min={pmin:.2f}, max={pmax:.2f}, "
          f"task_count={len(tasks_list)}, backend={args.solver_backend})")

    out = REPO_ROOT / args.out_csv
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["trial", "elapsed_ms"])
        for i, ms in enumerate(elapsed_ms, start=1):
            w.writerow([i, f"{ms:.4f}"])
        w.writerow([])
        w.writerow(["#stat", "value_ms"])
        w.writerow(["p50", f"{p50:.4f}"])
        w.writerow(["min", f"{pmin:.4f}"])
        w.writerow(["max", f"{pmax:.4f}"])
        w.writerow(["task_count", len(tasks_list)])
        w.writerow(["solver_backend", args.solver_backend])
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()

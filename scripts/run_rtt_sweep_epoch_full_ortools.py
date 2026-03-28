#!/usr/bin/env python3
"""Epoch-based RTT sweep capturing ingest, query, and solver time for both backends.

This variant uses the OR-Tools solver (python_solver/src/network_controller/solver.py)
instead of the PuLP solver (scheduler/solver.py).
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from rtt_sweep_common import (
    add_common_args,
    bulk_ingest_es,
    compare_results,
    format_compact,
    ingest_server,
    iter_batches,
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

DEFAULT_ROWS_PER_EPOCH = 1_000_000
DEFAULT_START_EPOCH = 1
DEFAULT_END_EPOCH = 10
DEFAULT_SERVER_LOG = "logs/server_epoch_full_ortools.log"


@dataclass
class SweepResult:
    epoch: int
    rows_ingested: int
    server_ingest_ms: float
    es_ingest_ms: float
    server_query_ms: float
    es_query_ms: float
    server_solver_ms: float
    es_solver_ms: float
    server_total_ms: float   # ingest + query + solver
    es_total_ms: float


@dataclass
class SolverResult:
    elapsed_ms: float
    objective_value: float
    assignments: int
    unassigned: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Epoch-based RTT sweep (OR-Tools solver): ingest + query + solver timing for server and ES."
    )
    parser.add_argument("--start-epoch", type=int, default=DEFAULT_START_EPOCH)
    parser.add_argument("--end-epoch", type=int, default=DEFAULT_END_EPOCH)
    parser.add_argument(
        "--rows-per-epoch", type=int, default=DEFAULT_ROWS_PER_EPOCH,
        help="Rows ingested per epoch (default 1_000_000)",
    )
    add_common_args(parser)
    parser.set_defaults(
        server_log=DEFAULT_SERVER_LOG,
        out_csv="data/rtt_results_epoch_full_ortools.csv",
    )
    parser.add_argument(
        "--out-plot",
        type=str,
        default="plots/rtt_epoch_full_ortools.png",
        help="Output plot filename",
    )
    parser.add_argument(
        "--run-solver",
        action="store_true",
        default=False,
        help="Run the OR-Tools solver once per epoch after queries",
    )
    parser.add_argument(
        "--solver-task-count",
        type=int,
        default=0,
        help="Number of solver tasks to include (0 = all)",
    )
    parser.add_argument(
        "--solver-node-count",
        type=int,
        default=0,
        help="Number of solver nodes to include (0 = all)",
    )
    parser.add_argument(
        "--query-node-count",
        type=int,
        default=0,
        help="Number of nodes to query (0 = all)",
    )
    parser.add_argument(
        "--solver-data-dir",
        type=str,
        default=str(SOLVER_DUMMY_DIR),
        help="Directory containing dummy_data JSONL inputs",
    )
    parser.add_argument(
        "--solver-backend",
        type=str,
        choices=["CBC", "SCIP", "GLPK"],
        default="CBC",
        help="OR-Tools solver backend (default: CBC)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Solver helpers (OR-Tools variant)
# ---------------------------------------------------------------------------

def _ensure_solver_path() -> None:
    solver_root = SOLVER_ROOT.resolve()
    if str(solver_root) not in sys.path:
        sys.path.insert(0, str(solver_root))


def _load_solver_assets(data_dir: Path):
    """Load JSONL data and convert to OR-Tools solver types."""
    _ensure_solver_path()
    try:
        # Use scheduler's JSONL loaders to read the raw data.
        from scheduler.load_info import (  # type: ignore
            load_edges as load_edges_jsonl,
            load_nodes as load_nodes_jsonl,
            load_tasks as load_tasks_jsonl,
        )
        # Import OR-Tools solver types.
        from python_solver.src.network_controller.solver import (  # type: ignore
            Edge as OrtEdge,
            NetworkControllerSolver,
            Node as OrtNode,
            Task as OrtTask,
            TaskCommunication as OrtTaskCommunication,
        )
    except ImportError as exc:
        raise RuntimeError(
            "Failed to import solver modules. Ensure solver_experimental is on PYTHONPATH."
        ) from exc

    nodes_path = data_dir / "nodes.jsonl"
    edges_path = data_dir / "edges.jsonl"
    tasks_path = data_dir / "tasks.jsonl"
    if not (nodes_path.exists() and edges_path.exists() and tasks_path.exists()):
        raise RuntimeError(
            "solver-data-dir missing required JSONL files: nodes.jsonl, edges.jsonl, tasks.jsonl."
        )

    # Load raw scheduler-format data.
    raw_nodes = load_nodes_jsonl(nodes_path)
    raw_edges = load_edges_jsonl(edges_path)
    raw_tasks = load_tasks_jsonl(tasks_path)

    # Convert to OR-Tools types.
    ort_nodes: Dict[str, OrtNode] = {}
    for node_id, node in raw_nodes.items():
        ort_nodes[node_id] = OrtNode(
            node_id=node.node_id,
            cpu_capacity=node.cpu_capacity,
            memory_capacity=node.memory_capacity,
            used_cpu=node.used_cpu,
            used_memory=node.used_memory,
        )

    ort_edges: Dict[tuple, OrtEdge] = {}
    for edge_key, edge in raw_edges.items():
        ort_edges[edge_key] = OrtEdge(
            edge_id=edge_key,
            capacity=edge.capacity,
            used_bandwidth=edge.used_bandwidth,
        )

    ort_tasks: Dict[str, OrtTask] = {}
    for task_id, task in raw_tasks.items():
        communications = tuple(
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
            communications=communications,
        )

    return {
        "nodes": ort_nodes,
        "edges": ort_edges,
        "tasks": ort_tasks,
        "NetworkControllerSolver": NetworkControllerSolver,
        "OrtNode": OrtNode,
        "OrtEdge": OrtEdge,
        "OrtTask": OrtTask,
        "OrtTaskCommunication": OrtTaskCommunication,
    }


def _select_first_n(items: List[str], count: int) -> List[str]:
    if count <= 0 or count >= len(items):
        return list(items)
    return list(items[:count])


def _build_solver_context(assets: dict, task_count: int, node_count: int) -> dict:
    node_ids = _select_first_n(sorted(assets["nodes"].keys()), node_count)
    task_ids = _select_first_n(sorted(assets["tasks"].keys()), task_count)

    nodes = {nid: assets["nodes"][nid] for nid in node_ids}
    tasks = {tid: assets["tasks"][tid] for tid in task_ids}

    node_set = set(nodes.keys())
    edges = {
        eid: edge
        for eid, edge in assets["edges"].items()
        if eid[0] in node_set and eid[1] in node_set
    }

    return {
        "nodes": nodes,
        "edges": edges,
        "tasks": tasks,
    }


def _build_nodes_with_usage(
    base_nodes: Dict[str, object],
    usage: Dict[str, Dict[str, float]],
    OrtNode: type,
) -> Dict[str, object]:
    updated: Dict[str, object] = {}
    for node_id, node in base_nodes.items():
        used = usage.get(node_id, {})
        used_cpu = min(used.get("cpu", node.used_cpu), node.cpu_capacity)
        used_mem = min(used.get("memory", node.used_memory), node.memory_capacity)
        updated[node_id] = OrtNode(
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
        cumulative = item.get("cumulative") or {}
        usage[str(node_id)] = {
            "cpu": float(cumulative.get("cpu_cores", 0.0) or 0.0),
            "memory": float(cumulative.get("memory_gb", 0.0) or 0.0),
        }
    return usage


def _extract_es_usage(es_json: dict) -> Dict[str, Dict[str, float]]:
    usage: Dict[str, Dict[str, float]] = {}
    for node_id, payload in es_json.items():
        aggs = payload.get("aggregations", {})
        usage[str(node_id)] = {
            "cpu": float(aggs.get("cpu_sum", {}).get("value", 0.0) or 0.0),
            "memory": float(aggs.get("mem_sum", {}).get("value", 0.0) or 0.0),
        }
    return usage


def run_solver_for_usage(
    usage: Dict[str, Dict[str, float]],
    assets: dict,
    context: dict,
    solver_backend: str = "CBC",
) -> SolverResult:
    nodes = _build_nodes_with_usage(context["nodes"], usage, assets["OrtNode"])
    edges = context["edges"]

    solver = assets["NetworkControllerSolver"](nodes, edges, solver_backend=solver_backend)
    tasks_list = list(context["tasks"].values())

    t0 = time.perf_counter()
    result = solver.solve(tasks_list)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    print(
        f"  solver objective: {result.objective_value} | "
        f"assignments: {len(result.decisions)} | "
        f"unassigned: {len(result.unassigned_tasks)} | "
        f"time: {elapsed_ms:.2f} ms"
    )
    return SolverResult(
        elapsed_ms=elapsed_ms,
        objective_value=float(result.objective_value),
        assignments=len(result.decisions),
        unassigned=len(result.unassigned_tasks),
    )


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def plot_results(results: List[SweepResult], out_path: Path, backend: str = "CBC") -> None:
    """Stacked bar chart (ingest / query / solver) per epoch for each backend,
    plus a separate total-time comparison line chart."""
    import matplotlib.pyplot as plt
    import numpy as np

    epochs = [r.epoch for r in results]
    x = np.arange(len(epochs))
    bar_w = 0.35

    s_ingest = [r.server_ingest_ms for r in results]
    s_query  = [r.server_query_ms  for r in results]
    s_solver = [r.server_solver_ms for r in results]
    e_ingest = [r.es_ingest_ms     for r in results]
    e_query  = [r.es_query_ms      for r in results]
    e_solver = [r.es_solver_ms     for r in results]
    s_total  = [r.server_total_ms  for r in results]
    e_total  = [r.es_total_ms      for r in results]

    # --- stacked bar: breakdown ---
    fig, ax = plt.subplots(figsize=(max(10, len(epochs) * 1.5), 6))

    ax.bar(x - bar_w / 2, s_ingest, bar_w, label="Server ingest", color="#4e79a7")
    ax.bar(x - bar_w / 2, s_query,  bar_w, bottom=s_ingest,
           label="Server query",  color="#76b7b2")
    ax.bar(x - bar_w / 2, s_solver, bar_w,
           bottom=[i + q for i, q in zip(s_ingest, s_query)],
           label="Server solver", color="#59a14f")

    ax.bar(x + bar_w / 2, e_ingest, bar_w, label="ES ingest",  color="#e15759")
    ax.bar(x + bar_w / 2, e_query,  bar_w, bottom=e_ingest,
           label="ES query",   color="#f28e2b")
    ax.bar(x + bar_w / 2, e_solver, bar_w,
           bottom=[i + q for i, q in zip(e_ingest, e_query)],
           label="ES solver",  color="#b07aa1")

    ax.set_xticks(x)
    ax.set_xticklabels([str(e) for e in epochs])
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Time (ms)")
    ax.set_title(f"Per-epoch timing breakdown: Server vs ES (OR-Tools/{backend} solver)\n(ingest + query + solver)")
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close(fig)

    # --- total comparison line ---
    total_path = out_path.with_name(f"{out_path.stem}_total{out_path.suffix}")
    fig2, ax2 = plt.subplots(figsize=(max(8, len(epochs) * 1.2), 5))
    ax2.plot(epochs, s_total, marker="o", label="Server total (ms)", color="#4e79a7")
    ax2.plot(epochs, e_total, marker="s", label="ES total (ms)",     color="#e15759")
    ax2.set_xlabel("Epoch")
    ax2.set_ylabel("Total time (ms)")
    ax2.set_title(f"Total epoch time: Server vs ES (OR-Tools/{backend} solver)\n(ingest + query + solver)")
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(total_path)
    plt.close(fig2)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    nodes = parse_nodes_config(args.nodes_config)
    rng = random.Random(args.seed)

    out_csv = resolve_repo_path(args.out_csv)
    out_plot = resolve_repo_path(args.out_plot)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_plot.parent.mkdir(parents=True, exist_ok=True)

    results: List[SweepResult] = []

    csv_exists = out_csv.exists()
    csv_mode = "w" if args.truncate_csv else "a"
    csv_file = open(out_csv, csv_mode, newline="")
    writer = csv.writer(csv_file)
    if args.truncate_csv or not csv_exists:
        writer.writerow([
            "timestamp_utc",
            "epoch",
            "rows_ingested",
            "server_ingest_ms",
            "es_ingest_ms",
            "server_query_ms",
            "es_query_ms",
            "server_solver_ms",
            "es_solver_ms",
            "server_total_ms",
            "es_total_ms",
            "solver_task_count",
            "solver_node_count",
            "query_node_count",
            "all_nodes_count",
            "all_tasks_count",
        ])
        csv_file.flush()

    reset_es_index(
        args.es_url,
        args.es_index,
        args.es_api_key,
        args.connect_timeout,
        args.es_timeout,
    )

    server_log_path = None if args.server_log == "-" else resolve_repo_path(args.server_log)
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
            print(f"Loading solver inputs from {solver_dir} (OR-Tools/{args.solver_backend}) ...")
            assets = _load_solver_assets(solver_dir)
            solver_context = _build_solver_context(
                assets,
                task_count=args.solver_task_count,
                node_count=args.solver_node_count,
            )
            print(
                f"Solver setup: tasks={len(solver_context['tasks'])}/{len(assets['tasks'])} | "
                f"nodes={len(solver_context['nodes'])}/{len(assets['nodes'])}"
            )

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

            # --- INGEST PHASE: accumulate timing across all batches ---
            server_ingest_ms = 0.0
            es_ingest_ms = 0.0
            for batch_idx, batch in enumerate(
                iter_batches(total_rows, nodes, rng, args.batch_size, epoch), start=1
            ):
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

                is_last_batch = batch_idx == total_batches
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
                        f"  ingest progress: {batch_idx}/{total_batches} batches "
                        f"({batch_idx * 100 // total_batches}%) | "
                        f"server={server_ingest_ms:.0f} ms  ES={es_ingest_ms:.0f} ms"
                    )

            print(
                f"  ingest done: server={server_ingest_ms:.2f} ms | ES={es_ingest_ms:.2f} ms"
            )

            # --- QUERY PHASE ---
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
            print(
                f"  query done: server={server_query_ms:.2f} ms | ES={es_query_ms:.2f} ms"
            )

            # --- SOLVER PHASE ---
            server_solver_ms = 0.0
            es_solver_ms = 0.0
            if args.run_solver and assets is not None and solver_context is not None:
                backend = args.solver_backend
                print(f"  running OR-Tools ({backend}) solver on server metrics ...")
                server_usage = _extract_server_usage(server_json)
                server_sr = run_solver_for_usage(server_usage, assets, solver_context, solver_backend=backend)
                server_solver_ms = server_sr.elapsed_ms

                print(f"  running OR-Tools ({backend}) solver on ES metrics ...")
                es_usage = _extract_es_usage(es_json)
                es_sr = run_solver_for_usage(es_usage, assets, solver_context, solver_backend=backend)
                es_solver_ms = es_sr.elapsed_ms

            # --- TOTALS ---
            server_total_ms = server_ingest_ms + server_query_ms + server_solver_ms
            es_total_ms = es_ingest_ms + es_query_ms + es_solver_ms

            max_diff = compare_results(server_json, es_json)
            results.append(SweepResult(
                epoch=epoch,
                rows_ingested=total_rows,
                server_ingest_ms=server_ingest_ms,
                es_ingest_ms=es_ingest_ms,
                server_query_ms=server_query_ms,
                es_query_ms=es_query_ms,
                server_solver_ms=server_solver_ms,
                es_solver_ms=es_solver_ms,
                server_total_ms=server_total_ms,
                es_total_ms=es_total_ms,
            ))

            print(
                f"  TOTAL: server={server_total_ms:.2f} ms | ES={es_total_ms:.2f} ms | "
                f"max diff >=2%: {max_diff:.2f}%"
            )
            for line in format_compact(server_json, es_json, query_nodes):
                print(line)

            writer.writerow([
                datetime.now(timezone.utc).isoformat(),
                epoch,
                total_rows,
                f"{server_ingest_ms:.4f}",
                f"{es_ingest_ms:.4f}",
                f"{server_query_ms:.4f}",
                f"{es_query_ms:.4f}",
                f"{server_solver_ms:.4f}",
                f"{es_solver_ms:.4f}",
                f"{server_total_ms:.4f}",
                f"{es_total_ms:.4f}",
                args.solver_task_count,
                args.solver_node_count,
                args.query_node_count,
                len(nodes),
                len(assets["tasks"]) if assets is not None else 0,
            ])
            csv_file.flush()

    finally:
        stop_server(proc)
        csv_file.close()

    plot_results(results, out_plot, backend=args.solver_backend)
    total_plot = out_plot.with_name(f"{out_plot.stem}_total{out_plot.suffix}")
    print(f"\nWrote {out_csv}")
    print(f"Wrote {out_plot} and {total_plot}")


if __name__ == "__main__":
    main()

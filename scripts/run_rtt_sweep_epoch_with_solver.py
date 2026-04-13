#!/usr/bin/env python3
"""Run epoch-based ingestion/query RTT sweep for server + Elasticsearch."""

from __future__ import annotations

import argparse
import csv
import random
import sys
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from itertools import combinations
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
DEFAULT_SERVER_LOG = "logs/server_epoch.log"


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
    add_common_args(parser)
    parser.set_defaults(server_log=DEFAULT_SERVER_LOG, out_csv="data/rtt_results_epoch_with_solver.csv")
    parser.add_argument(
        "--out-plot",
        type=str,
        default="plots/query_rtt_plot_epoch_with_solver.png",
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


# ---------------------------------------------------------------------------
# Solver helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    nodes = parse_nodes_config(args.nodes_config)
    rng = random.Random(args.seed)

    out_csv = resolve_repo_path(args.out_csv)
    out_plot = resolve_repo_path(args.out_plot)

    results: List[SweepResult] = []

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_plot.parent.mkdir(parents=True, exist_ok=True)
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
                "data",
                _solver_compare_csv_name(
                    args.solver_task_count,
                    args.solver_node_count,
                    args.query_node_count,
                ),
            )
            solver_compare_path.parent.mkdir(parents=True, exist_ok=True)
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

#!/usr/bin/env python3
"""Sweep Elasticsearch ingest and query time across exponentially increasing row counts.

Per row count: ingest the data ONCE, then run N repeats of
  (clear ES cache) -> query -> feed per-node p50 metrics into the solver.
Records ingest_ms (once), and per-repeat query_ms and solver_ms.
"""

from __future__ import annotations

import argparse
import csv
import random
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from rtt_sweep_common import (
    add_common_args,
    bulk_ingest_es,
    clear_es_cache,
    parse_nodes_config,
    query_es_nodes,
    reset_es_index,
    resolve_repo_path,
)

# Reuse solver harness from the OR-Tools sweep script.
SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from run_dynamic_epoch_benchmark import (  # type: ignore
    DEFAULT_PADDING_CPU_RATIO_MAX,
    DEFAULT_PADDING_MEM_RATIO_MAX,
    DEFAULT_PADDING_NET_RATIO_MAX,
    SOLVER_DUMMY_DIR,
    _build_nodes_with_usage,
    _build_solver_context,
    _load_solver_assets,
    _make_padding_rows,
)


DEFAULT_START_ROWS = 1_000
DEFAULT_MAX_ROWS = 1_024_000
DEFAULT_REPEATS = 10
DEFAULT_OUT_CSV = "data/es_ingest_query_sweep.csv"
DEFAULT_OUT_SUMMARY_CSV = "data/es_ingest_query_sweep_summary.csv"
DEFAULT_OUT_PLOT = "plots/es_ingest_query_sweep.png"
DEFAULT_SOLVER_BACKEND = "SCIP"
DEFAULT_SOLVER_TIME_LIMIT_S = 30.0
DEFAULT_TASK_COUNT = 30


@dataclass
class RepeatResult:
    rows: int
    repeat: int
    es_ingest_ms: float  # same value duplicated per row count (ingest runs once)
    es_query_ms: float
    solver_ms: float


@dataclass
class SummaryResult:
    rows: int
    ingest_ms: float
    median_query_ms: float
    min_query_ms: float
    max_query_ms: float
    median_solver_ms: float
    min_solver_ms: float
    max_solver_ms: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark Elasticsearch ingest + query latency and pair each query with "
            "an OR-Tools solver run fed by the per-node p50 metrics."
        )
    )
    parser.add_argument("--start-rows", type=int, default=DEFAULT_START_ROWS)
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS)
    parser.add_argument("--multiplier", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=DEFAULT_REPEATS,
                        help="Query+solver repeats per row count (ingest still runs once)")
    parser.add_argument("--summary-csv", type=str, default=DEFAULT_OUT_SUMMARY_CSV)
    parser.add_argument("--out-plot", type=str, default=DEFAULT_OUT_PLOT)
    parser.add_argument("--task-count", type=int, default=DEFAULT_TASK_COUNT,
                        help="Number of tasks (first N sorted) fed to the solver")
    parser.add_argument("--solver-backend", type=str, default=DEFAULT_SOLVER_BACKEND,
                        choices=["CBC", "SCIP", "GLPK"])
    parser.add_argument("--solver-time-limit-s", type=float,
                        default=DEFAULT_SOLVER_TIME_LIMIT_S)
    parser.add_argument("--solver-warmup", type=int, default=2,
                        help="Untimed solver runs before the sweep")
    parser.add_argument("--solver-data-dir", type=str, default=str(SOLVER_DUMMY_DIR),
                        help="Directory with nodes.jsonl/edges.jsonl/tasks.jsonl")
    parser.add_argument("--padding-cpu-ratio-max", type=float,
                        default=DEFAULT_PADDING_CPU_RATIO_MAX,
                        help="Max cpu per padding row = node.cpu_capacity * ratio")
    parser.add_argument("--padding-mem-ratio-max", type=float,
                        default=DEFAULT_PADDING_MEM_RATIO_MAX)
    parser.add_argument("--padding-net-ratio-max", type=float,
                        default=DEFAULT_PADDING_NET_RATIO_MAX)
    add_common_args(parser)
    parser.set_defaults(out_csv=DEFAULT_OUT_CSV)
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.start_rows <= 0:
        raise ValueError("--start-rows must be > 0")
    if args.max_rows < args.start_rows:
        raise ValueError("--max-rows must be >= --start-rows")
    if args.multiplier < 2:
        raise ValueError("--multiplier must be >= 2")
    if args.repeats <= 0:
        raise ValueError("--repeats must be > 0")


def generate_row_counts(start_rows: int, max_rows: int, multiplier: int) -> List[int]:
    rows = start_rows
    values: List[int] = []
    while rows <= max_rows:
        values.append(rows)
        rows *= multiplier
    return values


def _extract_es_p50_usage(es_json: dict) -> Dict[str, Dict[str, float]]:
    """Pull per-node p50 cpu/memory out of the percentile aggregations.

    `es_json` is the dict returned by query_es_nodes (one entry per node).
    """
    usage: Dict[str, Dict[str, float]] = {}
    for node_id, payload in es_json.items():
        aggs = payload.get("aggregations", {})
        cpu_vals = aggs.get("cpu_pct", {}).get("values", {}) or {}
        mem_vals = aggs.get("mem_pct", {}).get("values", {}) or {}
        cpu_p50 = cpu_vals.get("50.0")
        mem_p50 = mem_vals.get("50.0")
        usage[str(node_id)] = {
            "cpu": float(cpu_p50) if cpu_p50 is not None else 0.0,
            "memory": float(mem_p50) if mem_p50 is not None else 0.0,
        }
    return usage


def _run_solver(
    usage: Dict[str, Dict[str, float]],
    assets: dict,
    context: dict,
    solver_backend: str,
    time_limit_s: float,
) -> float:
    nodes = _build_nodes_with_usage(context["nodes"], usage, assets["OrtNode"])
    solver = assets["NetworkControllerSolver"](
        nodes, context["edges"], solver_backend=solver_backend
    )
    tasks_list = list(context["tasks"].values())
    t0 = time.perf_counter()
    solver.solve(tasks_list, time_limit_s=time_limit_s)
    return (time.perf_counter() - t0) * 1000.0


def write_detail_csv(path: Path, results: List[RepeatResult]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["timestamp_utc", "rows", "repeat",
                         "es_ingest_ms", "es_query_ms", "solver_ms"])
        for r in results:
            writer.writerow([
                now, r.rows, r.repeat,
                f"{r.es_ingest_ms:.4f}",
                f"{r.es_query_ms:.4f}",
                f"{r.solver_ms:.4f}",
            ])


def build_summary(results: List[RepeatResult]) -> List[SummaryResult]:
    grouped: Dict[int, List[RepeatResult]] = {}
    for r in results:
        grouped.setdefault(r.rows, []).append(r)

    summary_rows: List[SummaryResult] = []
    for rows in sorted(grouped):
        items = grouped[rows]
        query = [it.es_query_ms for it in items]
        solver = [it.solver_ms for it in items]
        summary_rows.append(SummaryResult(
            rows=rows,
            ingest_ms=items[0].es_ingest_ms,
            median_query_ms=statistics.median(query),
            min_query_ms=min(query),
            max_query_ms=max(query),
            median_solver_ms=statistics.median(solver),
            min_solver_ms=min(solver),
            max_solver_ms=max(solver),
        ))
    return summary_rows


def write_summary_csv(path: Path, summary_rows: List[SummaryResult]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "rows", "ingest_ms",
            "median_query_ms", "min_query_ms", "max_query_ms",
            "median_solver_ms", "min_solver_ms", "max_solver_ms",
        ])
        for r in summary_rows:
            writer.writerow([
                r.rows,
                f"{r.ingest_ms:.4f}",
                f"{r.median_query_ms:.4f}",
                f"{r.min_query_ms:.4f}",
                f"{r.max_query_ms:.4f}",
                f"{r.median_solver_ms:.4f}",
                f"{r.min_solver_ms:.4f}",
                f"{r.max_solver_ms:.4f}",
            ])


def plot_results(summary_rows: List[SummaryResult], out_path: Path) -> None:
    import matplotlib.pyplot as plt

    xs = [row.rows for row in summary_rows]
    ingest = [row.ingest_ms for row in summary_rows]
    query = [row.median_query_ms for row in summary_rows]
    solver = [row.median_solver_ms for row in summary_rows]

    plt.figure(figsize=(10, 6))
    plt.plot(xs, ingest, marker="o", label="ES ingest (single run, ms)")
    plt.plot(xs, query, marker="o", label="ES query median (ms)")
    plt.plot(xs, solver, marker="o", label="Solver median (ms)")
    plt.xscale("log", base=2)
    plt.xlabel("Rows")
    plt.ylabel("Latency (ms)")
    plt.title("ES Ingest/Query + Solver Latency vs Row Count")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path)


def main() -> None:
    args = parse_args()
    validate_args(args)

    nodes = parse_nodes_config(args.nodes_config)
    row_counts = generate_row_counts(args.start_rows, args.max_rows, args.multiplier)

    out_csv = resolve_repo_path(args.out_csv)
    summary_csv = resolve_repo_path(args.summary_csv)
    out_plot = resolve_repo_path(args.out_plot)
    for p in (out_csv, summary_csv, out_plot):
        p.parent.mkdir(parents=True, exist_ok=True)

    # Build solver context once. Node set comes from the server config (same as
    # the ES query targets); all tasks/edges from the JSONL inputs are used.
    print(f"Loading solver assets from {args.solver_data_dir} (backend={args.solver_backend})")
    assets = _load_solver_assets(Path(args.solver_data_dir))
    context = _build_solver_context(assets, nodes)
    # Trim to first N sorted tasks (full 248-task set is intractable for SCIP).
    selected_task_ids = sorted(context["tasks"].keys())[:args.task_count]
    context["tasks"] = {tid: context["tasks"][tid] for tid in selected_task_ids}
    print(f"Solver context: nodes={len(context['nodes'])}, "
          f"edges={len(context['edges'])}, tasks={len(context['tasks'])}")

    # Warm up solver.
    for w in range(args.solver_warmup):
        _run_solver({}, assets, context, args.solver_backend, args.solver_time_limit_s)
    print(f"Solver warmup: {args.solver_warmup} runs done")

    repeat_results: List[RepeatResult] = []

    for rows in row_counts:
        print(f"\n=== Sweep rows={rows} ===")
        reset_es_index(args.es_url, args.es_index, args.es_api_key,
                       args.connect_timeout, args.es_timeout)

        rng = random.Random(args.seed + rows * 10_000)
        all_rows = _make_padding_rows(
            rows, nodes, context, rng, 0,
            args.padding_cpu_ratio_max,
            args.padding_mem_ratio_max,
            args.padding_net_ratio_max,
        )
        total_batches = (rows + args.batch_size - 1) // args.batch_size
        log_every = max(1, total_batches // 10)

        def _iter_local_batches():
            for start in range(0, rows, args.batch_size):
                yield all_rows[start : start + args.batch_size]

        es_ingest_ms = 0.0
        for batch_idx, batch in enumerate(_iter_local_batches(), start=1):
            is_last = batch_idx == total_batches
            t0 = time.perf_counter()
            bulk_ingest_es(
                args.es_url, args.es_index, args.es_api_key, batch,
                args.connect_timeout, args.es_timeout,
                "wait_for" if is_last else None,
            )
            es_ingest_ms += (time.perf_counter() - t0) * 1000.0
            if batch_idx % log_every == 0 or batch_idx == total_batches:
                print(f"    ingest progress: {batch_idx}/{total_batches} "
                      f"({batch_idx * 100 // total_batches}%)")
        print(f"  ES ingest: {es_ingest_ms:.2f} ms")

        for repeat in range(1, args.repeats + 1):
            clear_es_cache(args.es_url, args.es_index, args.es_api_key,
                           args.connect_timeout, args.es_timeout)
            es_json, es_query_ms = query_es_nodes(
                args.es_url, args.es_index, args.es_api_key, nodes,
                args.connect_timeout, args.es_timeout,
                request_cache=False,
            )
            usage = _extract_es_p50_usage(es_json)
            solver_ms = _run_solver(usage, assets, context, args.solver_backend,
                                    args.solver_time_limit_s)

            repeat_results.append(RepeatResult(
                rows=rows, repeat=repeat,
                es_ingest_ms=es_ingest_ms,
                es_query_ms=es_query_ms,
                solver_ms=solver_ms,
            ))
            print(f"  repeat {repeat:>2}/{args.repeats}: "
                  f"query {es_query_ms:7.2f} ms | solver {solver_ms:7.2f} ms")

    summary_rows = build_summary(repeat_results)
    write_detail_csv(out_csv, repeat_results)
    write_summary_csv(summary_csv, summary_rows)
    plot_results(summary_rows, out_plot)
    print(f"\nWrote {out_csv}")
    print(f"Wrote {summary_csv}")
    print(f"Wrote {out_plot}")


if __name__ == "__main__":
    main()

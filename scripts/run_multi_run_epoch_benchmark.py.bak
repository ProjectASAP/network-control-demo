#!/usr/bin/env python3
"""Multi-run epoch benchmark for paper-grade error-bar plots.

For each of N runs:
  - Restart sketch server (clears sketch state).
  - Reset ES index.
  - Restart emulator with a per-run seed (varies workload across runs).
  - Run a warmup epoch (not recorded).
  - For each of M epochs:
    - Generate emulator + padding rows; ingest ONCE to sketch + ES.
    - Query sketch.
    - Query ES with tdigest compression=default (100).
    - Query ES with tdigest compression=large (1000).
    - Compute ground-truth quantiles + sums from ingested rows.
    - Run solver 3 times (sketch input, ES-default input, ES-large input).
    - Append one CSV row + write one JSON sidecar with all raw query results.

Data layout:
    data/multi_run_epoch_benchmark.csv
    data/multi_run_epoch_benchmark_queries/run{R}_epoch{E}.json
"""

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
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))

from rtt_sweep_common import (  # noqa: E402
    parse_nodes_config,
    query_es_nodes,
    query_server_batch,
    reset_es_index,
    resolve_repo_path,
    start_server,
    stop_server,
    wait_for_server,
)

import run_dynamic_epoch_benchmark as dyn  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Ground truth
# ---------------------------------------------------------------------------

FIELD_MAP = [("cpu", "cpu_cores"), ("mem", "memory_gb"), ("net", "network_mbps")]
PERCENTS = [0, 50, 90, 100]


def compute_ground_truth(rows: List[Dict[str, object]]) -> Dict[str, Dict[str, Dict[str, float]]]:
    """Exact per-node × per-field percentiles + sum from ingested rows."""
    grouped: Dict[str, Dict[str, List[float]]] = {}
    for r in rows:
        node = r["node"]
        bucket = grouped.setdefault(node, {"cpu": [], "mem": [], "net": []})
        bucket["cpu"].append(float(r["cpu"]))
        bucket["mem"].append(float(r["mem"]))
        bucket["net"].append(float(r["net"]))

    truth: Dict[str, Dict[str, Dict[str, float]]] = {}
    for node, fields in grouped.items():
        per_node: Dict[str, Dict[str, float]] = {}
        for short, long_name in FIELD_MAP:
            arr = np.asarray(fields[short], dtype=np.float64)
            pct = {str(p): float(np.percentile(arr, p, method="linear")) for p in PERCENTS}
            per_node[long_name] = {
                "percentiles": pct,
                "sum": float(arr.sum()),
                "count": int(arr.size),
            }
        truth[node] = per_node
    return truth


def es_aggs_to_dict(es_json_by_node: Dict[str, dict]) -> Dict[str, Dict[str, Dict[str, float]]]:
    """Flatten ES per-node search responses into the same shape as ground truth."""
    out: Dict[str, Dict[str, Dict[str, float]]] = {}
    for node, payload in es_json_by_node.items():
        aggs = payload.get("aggregations", {})
        per_node: Dict[str, Dict[str, float]] = {}
        for short, long_name in FIELD_MAP:
            pct_vals = (aggs.get(f"{short}_pct") or {}).get("values", {}) or {}
            sum_val = (aggs.get(f"{short}_sum") or {}).get("value", 0.0) or 0.0
            # ES uses "0.0", "50.0", "90.0", "100.0" string keys.
            pct = {}
            for p in PERCENTS:
                v = pct_vals.get(f"{float(p):.1f}") or pct_vals.get(str(p)) or pct_vals.get(str(float(p)))
                pct[str(p)] = float(v) if v is not None else float("nan")
            per_node[long_name] = {"percentiles": pct, "sum": float(sum_val)}
        out[node] = per_node
    return out


def server_results_to_dict(server_json: dict) -> Dict[str, Dict[str, Dict[str, float]]]:
    """Normalize server _batch response. Server returns per-item {key, percentiles, sum}
    where `sum` is a flat dict keyed by field name."""
    out: Dict[str, Dict[str, Dict[str, float]]] = {}
    for item in server_json.get("results", []):
        node = item["key"]
        pct_block = item.get("percentiles") or {}
        sum_block = item.get("sum") or {}
        per_node: Dict[str, Dict[str, float]] = {}
        for _, long_name in FIELD_MAP:
            pct = pct_block.get(long_name, {}) or {}
            s = sum_block.get(long_name, 0.0) or 0.0
            pct_norm = {str(k): float(v) for k, v in pct.items()}
            per_node[long_name] = {"percentiles": pct_norm, "sum": float(s)}
        out[node] = per_node
    return out


# ---------------------------------------------------------------------------
# Emulator with per-run seed
# ---------------------------------------------------------------------------

def start_emulator_with_seed(args: argparse.Namespace, seed: int) -> subprocess.Popen:
    """Same as dyn.start_emulator but passes --seed."""
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
        "--host", "127.0.0.1",
        "--port", str(dyn._port_from_url(args.emulator_url)),
        "--epoch-length-s", str(args.epoch_length_s),
        "--data-rate", str(args.data_rate),
        "--no-sketch-ingest",
        "--no-es-ingest",
        "--seed", str(seed),
    ]
    proc = subprocess.Popen(
        cmd,
        cwd=dyn.SOLVER_ROOT,
        stdout=stdout_target or subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        text=True,
    )
    if stdout_target is not None:
        proc._log_fh = stdout_target
    return proc


# ---------------------------------------------------------------------------
# Single-epoch execution
# ---------------------------------------------------------------------------

def gather_epoch_rows(
    args: argparse.Namespace,
    rng: random.Random,
    context: dict,
    query_nodes: List[str],
    epoch: int,
    running_tasks: Dict[str, object],
) -> Tuple[List[Dict[str, object]], int, int]:
    """Return (all_rows, emulator_rows, padding_rows) for this epoch."""
    emulator_rows: List[Dict[str, object]] = []
    if running_tasks:
        records = dyn.generate_from_emulator(
            args.emulator_url,
            running_tasks,
            args.connect_timeout,
            args.query_timeout,
        )
        emulator_rows = dyn._records_to_rows(records, epoch)

    n_emu = len(emulator_rows)
    n_pad = 0 if args.no_padding else max(0, args.rows_per_epoch - n_emu)
    padding_rows: List[Dict[str, object]] = []
    if n_pad > 0:
        padding_rows = dyn._make_padding_rows(
            n_pad,
            query_nodes,
            context,
            rng,
            epoch,
            args.padding_cpu_ratio_max,
            args.padding_mem_ratio_max,
            args.padding_net_ratio_max,
        )
    return emulator_rows + padding_rows, n_emu, n_pad


def ingest_once(
    rows: List[Dict[str, object]],
    epoch: int,
    args: argparse.Namespace,
) -> Tuple[float, float]:
    """Ingest the full epoch's rows once to sketch + ES; returns (server_ms, es_ms)."""
    if not rows:
        return 0.0, 0.0
    server_ms = 0.0
    es_ms = 0.0
    server_ms, es_ms = dyn._ingest_rows(
        rows,
        epoch,
        args,
        server_ms,
        es_ms,
        is_last_phase=True,
        phase_label="combined",
    )
    return server_ms, es_ms


# ---------------------------------------------------------------------------
# Solver wrapper
# ---------------------------------------------------------------------------

def solver_pass(
    backend_usage: Dict[str, Dict[str, float]],
    assets: dict,
    context: dict,
    args: argparse.Namespace,
    pending_task_ids: List[str],
) -> dyn.SolverResult:
    return dyn.run_solver_for_usage(
        backend_usage,
        assets,
        context,
        solver_backend=args.solver_backend,
        task_ids=pending_task_ids,
        time_limit_s=args.solver_time_limit_s,
    )


# ---------------------------------------------------------------------------
# CSV schema
# ---------------------------------------------------------------------------

CSV_HEADER = [
    "timestamp_utc",
    "run_id",
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
    "es_default_query_ms",
    "es_large_query_ms",
    "server_solver_ms",
    "es_default_solver_ms",
    "es_large_solver_ms",
    "server_assigned_count",
    "es_default_assigned_count",
    "es_large_assigned_count",
    "server_unassigned_count",
    "es_default_unassigned_count",
    "es_large_unassigned_count",
    "server_objective",
    "es_default_objective",
    "es_large_objective",
    "solver_backend",
    "es_large_compression",
    "seed",
    "queries_json_path",
]


# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Multi-run epoch benchmark: 10 runs × 10 epochs with full raw query data captured."
    )
    # Borrow common args from dyn (same shape).
    dyn.parse_args  # ensure import
    from rtt_sweep_common import add_common_args
    add_common_args(parser)

    parser.set_defaults(
        server_log="logs/server_multi_run.log",
        out_csv="data/multi_run_epoch_benchmark.csv",
    )

    parser.add_argument("--solver-data-dir", type=str, default=str(dyn.SOLVER_DUMMY_DIR))
    parser.add_argument("--solver-backend", type=str, choices=["CBC", "SCIP", "GLPK"], default="SCIP")
    parser.add_argument("--solver-time-limit-s", type=float, default=30.0)

    parser.add_argument("--n-runs", type=int, default=10)
    parser.add_argument("--n-epochs", type=int, default=10, help="Recorded epochs per run.")
    parser.add_argument("--warmup-epochs", type=int, default=1)
    parser.add_argument("--rows-per-epoch", type=int, default=dyn.DEFAULT_ROWS_PER_EPOCH)
    parser.add_argument("--max-epochs", type=int, default=None, help=argparse.SUPPRESS)  # unused but kept for compat

    parser.add_argument("--emulator-url", type=str, default=dyn.DEFAULT_EMULATOR_URL)
    parser.add_argument("--emulator-log", type=str, default="logs/emulator_multi_run.log")
    parser.add_argument("--emulator-ready-timeout", type=float, default=30.0)
    parser.add_argument("--skip-emulator-start", action="store_true", default=False)
    parser.add_argument("--epoch-length-s", type=float, default=dyn.DEFAULT_EPOCH_LENGTH_S)
    parser.add_argument("--data-rate", type=int, default=1)

    parser.add_argument("--no-padding", action="store_true", default=False)
    parser.add_argument("--padding-cpu-ratio-max", type=float, default=dyn.DEFAULT_PADDING_CPU_RATIO_MAX)
    parser.add_argument("--padding-mem-ratio-max", type=float, default=dyn.DEFAULT_PADDING_MEM_RATIO_MAX)
    parser.add_argument("--padding-net-ratio-max", type=float, default=dyn.DEFAULT_PADDING_NET_RATIO_MAX)

    parser.add_argument("--es-large-compression", type=int, default=1000,
                        help="tdigest compression for the 'large' ES query config (default 1000).")
    parser.add_argument("--seed-base", type=int, default=1000,
                        help="Per-run seed = seed_base + run_id (1..n_runs).")

    parser.add_argument("--queries-dir", type=str, default="data/multi_run_epoch_benchmark_queries",
                        help="Directory for per-epoch JSON sidecars with raw query results.")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Per-run loop
# ---------------------------------------------------------------------------

def run_single(
    args: argparse.Namespace,
    run_id: int,
    seed: int,
    assets: dict,
    context: dict,
    query_nodes: List[str],
    writer: csv.writer,
    csv_file,
    queries_dir: Path,
) -> None:
    print(f"\n========== RUN {run_id}/{args.n_runs} (seed={seed}) ==========")

    rng = random.Random(seed)

    # Reset ES.
    reset_es_index(
        args.es_url, args.es_index, args.es_api_key,
        args.connect_timeout, args.es_timeout,
    )

    # Start server (truncate log only on first run).
    server_log_path = None if args.server_log == "-" else resolve_repo_path(args.server_log)
    truncate_server_log = args.truncate_server_log and run_id == 1
    server_proc = start_server(server_log_path, truncate_log=truncate_server_log)

    emulator_proc = None
    try:
        wait_for_server(
            args.server_url, args.server_ready_timeout,
            args.connect_timeout, args.query_timeout,
        )

        if not args.skip_emulator_start:
            emulator_proc = start_emulator_with_seed(args, seed)
        dyn.wait_for_emulator(
            args.emulator_url, args.emulator_ready_timeout,
            args.connect_timeout, args.query_timeout,
        )

        # Per-run task state.
        pending_task_ids: List[str] = sorted(context["tasks"].keys())
        completed_task_ids: set[str] = set()
        running_tasks: Dict[str, object] = {}

        total_epochs = args.warmup_epochs + args.n_epochs
        for epoch_idx in range(total_epochs):
            is_warmup = epoch_idx < args.warmup_epochs
            recorded_epoch = epoch_idx - args.warmup_epochs + 1  # 1..n_epochs
            label = f"warmup-{epoch_idx}" if is_warmup else f"epoch-{recorded_epoch}"
            print(f"\n--- run {run_id} / {label} ---")

            # 1. Gather rows + ingest once.
            all_rows, n_emu, n_pad = gather_epoch_rows(
                args, rng, context, query_nodes, epoch_idx, running_tasks,
            )
            server_ingest_ms, es_ingest_ms = ingest_once(all_rows, epoch_idx, args)
            rows_ingested = n_emu + n_pad
            print(f"  ingested {rows_ingested} rows (emu={n_emu}, pad={n_pad}); "
                  f"server={server_ingest_ms:.0f}ms es={es_ingest_ms:.0f}ms")

            # 2. Queries (3x).
            if rows_ingested > 0:
                server_json, server_query_ms = query_server_batch(
                    args.server_url, query_nodes,
                    args.connect_timeout, args.query_timeout,
                )
                es_default_json, es_default_query_ms = query_es_nodes(
                    args.es_url, args.es_index, args.es_api_key, query_nodes,
                    args.connect_timeout, args.es_timeout,
                    epoch=epoch_idx, tdigest_compression=None,
                )
                es_large_json, es_large_query_ms = query_es_nodes(
                    args.es_url, args.es_index, args.es_api_key, query_nodes,
                    args.connect_timeout, args.es_timeout,
                    epoch=epoch_idx, tdigest_compression=args.es_large_compression,
                )
            else:
                server_json, server_query_ms = {"results": []}, 0.0
                es_default_json, es_default_query_ms = {}, 0.0
                es_large_json, es_large_query_ms = {}, 0.0

            print(f"  query ms: server={server_query_ms:.1f} "
                  f"es_default={es_default_query_ms:.1f} es_large={es_large_query_ms:.1f}")

            # 3. Solvers (3x — same pending tasks, different cumulative usage).
            server_usage = dyn._extract_server_usage(server_json)
            es_default_usage = dyn._extract_es_usage(es_default_json)
            es_large_usage = dyn._extract_es_usage(es_large_json)

            server_sr = solver_pass(server_usage, assets, context, args, pending_task_ids)
            es_default_sr = solver_pass(es_default_usage, assets, context, args, pending_task_ids)
            es_large_sr = solver_pass(es_large_usage, assets, context, args, pending_task_ids)

            print(f"  solver ms: server={server_sr.elapsed_ms:.1f} "
                  f"es_default={es_default_sr.elapsed_ms:.1f} es_large={es_large_sr.elapsed_ms:.1f} "
                  f"| assigned s={server_sr.assignments} ed={es_default_sr.assignments} el={es_large_sr.assignments}")

            # 4. Update running task state (use sketch path for assignments — matches dyn).
            new_running = dyn._build_running_tasks_for_assignments(
                server_sr.assigned_task_ids, context,
                epoch_idx * args.epoch_length_s, server_sr.decisions,
            )
            if running_tasks:
                active = dyn.fetch_active_tasks(
                    args.emulator_url, args.connect_timeout, args.query_timeout,
                )
                newly_completed = set(running_tasks.keys()) - set(active.keys())
                completed_task_ids.update(newly_completed)
                running_tasks = dict(active)
            running_tasks.update(new_running)
            pending_task_ids = list(server_sr.unassigned_task_ids)

            if is_warmup:
                continue

            # 5. Persist raw query results + ground truth to JSON sidecar.
            ground_truth = compute_ground_truth(all_rows) if rows_ingested > 0 else {}
            sidecar = {
                "run_id": run_id,
                "epoch": recorded_epoch,
                "seed": seed,
                "rows_ingested": rows_ingested,
                "emulator_rows": n_emu,
                "padding_rows": n_pad,
                "ground_truth": ground_truth,
                "server": server_results_to_dict(server_json),
                "es_default": es_aggs_to_dict(es_default_json),
                "es_large": es_aggs_to_dict(es_large_json),
                "es_large_compression": args.es_large_compression,
            }
            json_path = queries_dir / f"run{run_id:02d}_epoch{recorded_epoch:02d}.json"
            with open(json_path, "w", encoding="utf-8") as fh:
                json.dump(sidecar, fh, separators=(",", ":"))

            # 6. CSV row.
            writer.writerow([
                datetime.now(timezone.utc).isoformat(),
                run_id,
                recorded_epoch,
                rows_ingested,
                n_emu,
                n_pad,
                len(pending_task_ids),
                len(running_tasks),
                len(completed_task_ids),
                f"{server_ingest_ms:.4f}",
                f"{es_ingest_ms:.4f}",
                f"{server_query_ms:.4f}",
                f"{es_default_query_ms:.4f}",
                f"{es_large_query_ms:.4f}",
                f"{server_sr.elapsed_ms:.4f}",
                f"{es_default_sr.elapsed_ms:.4f}",
                f"{es_large_sr.elapsed_ms:.4f}",
                server_sr.assignments,
                es_default_sr.assignments,
                es_large_sr.assignments,
                server_sr.unassigned,
                es_default_sr.unassigned,
                es_large_sr.unassigned,
                f"{server_sr.objective_value:.6f}",
                f"{es_default_sr.objective_value:.6f}",
                f"{es_large_sr.objective_value:.6f}",
                args.solver_backend,
                args.es_large_compression,
                seed,
                str(json_path.relative_to(REPO_ROOT)),
            ])
            csv_file.flush()

    finally:
        dyn.stop_process(emulator_proc)
        stop_server(server_proc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    query_nodes = parse_nodes_config(args.nodes_config)
    out_csv = resolve_repo_path(args.out_csv)
    queries_dir = resolve_repo_path(args.queries_dir)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    queries_dir.mkdir(parents=True, exist_ok=True)

    assets = dyn._load_solver_assets(Path(args.solver_data_dir))
    context = dyn._build_solver_context(assets, query_nodes)
    if "N000" in context["nodes"]:
        raise RuntimeError("Node alignment failed: N000 present in solver context.")
    print(f"Solver setup: nodes={len(context['nodes'])}, tasks={len(context['tasks'])}, "
          f"backend={args.solver_backend}")
    print(f"Plan: {args.n_runs} runs × {args.n_epochs} epochs (+ {args.warmup_epochs} warmup) | "
          f"rows/epoch={args.rows_per_epoch} | ES configs: default + compression={args.es_large_compression}")

    csv_exists = out_csv.exists()
    if csv_exists and not args.truncate_csv:
        raise RuntimeError(
            f"{out_csv} already exists. Pass --truncate-csv to overwrite, or move the file aside."
        )
    csv_file = open(out_csv, "w", newline="")
    writer = csv.writer(csv_file)
    writer.writerow(CSV_HEADER)
    csv_file.flush()

    try:
        for run_id in range(1, args.n_runs + 1):
            seed = args.seed_base + run_id
            run_single(args, run_id, seed, assets, context, query_nodes, writer, csv_file, queries_dir)
    finally:
        csv_file.close()
        print(f"\nWrote {out_csv}")
        print(f"Wrote {queries_dir}/")


if __name__ == "__main__":
    main()

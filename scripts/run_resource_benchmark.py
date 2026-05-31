#!/usr/bin/env python3
"""Resource (CPU + whole-process RSS) benchmark: Sketch server vs Elasticsearch.

This mirrors the structure of the latency experiment
(run_multi_run_epoch_benchmark.py): N runs x M epochs, ingesting --rows-per-epoch
(default 1,000,000) rows per epoch. The sketch server is restarted per run (so
its KLL state accumulates across that run's epochs, exactly like the latency
experiment); ES is reset per run and each epoch's query is filtered to that
epoch. Solver / ground-truth steps are intentionally omitted -- they are
downstream consumers of the query, not part of the query's resource cost.

For every (run, epoch, backend, query-shape) it captures, IN THE SAME window:
  - latency (per-query perf_counter),
  - CPU time attributable to the query (whole process, idle-baseline subtracted),
  - whole-process RSS (mean/max/VmHWM) of the measured backend.

Measurement is OS-level and symmetric (same /proc method both backends), and
external to the server (a thread polls RSS; CPU is read only at window
boundaries), so it does not distort latency.

CPU resolution: /proc accounts CPU in jiffies (~10ms). A single query can read
0 ticks, so each measurement point issues queries ADAPTIVELY -- it keeps firing
until the wall window reaches --min-measure-seconds (and at least --min-repeats),
capped at --max-repeats. The cheap sketch query therefore runs many times (CPU
accumulates to a measurable level) while the expensive ES query runs fewer.

The 100 (run x epoch) samples per backend give the cross-run/epoch error bars,
matching the latency figure. Aggregation/plotting: plot_resource_benchmark.py.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))

from rtt_sweep_common import (  # noqa: E402
    add_common_args,
    parse_nodes_config,
    query_es_nodes_custom,
    query_server_batch_custom,
    reset_es_index,
    resolve_repo_path,
    stop_server,
    wait_for_server,
)

import run_dynamic_epoch_benchmark as dyn  # noqa: E402
import run_multi_run_epoch_benchmark as multi  # noqa: E402
import proc_monitor as pm  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = REPO_ROOT / "single_node_server/network-control-server"

# Query shapes. "default" mirrors the production query used by the latency
# experiment (query_server_batch: percentiles + sum over [0,50,90,100]).
QUERY_SHAPES: Dict[str, Dict[str, Any]] = {
    "default": {"aggs": ["percentiles", "sum"], "percents": [0, 50, 90, 100]},
    "p50": {"aggs": ["percentiles"], "percents": [50]},
    "sum": {"aggs": ["sum"], "percents": None},
    "all_quantile": {"aggs": ["percentiles"], "percents": [0, 50, 90, 100]},
}

# ES backends: (label, tdigest_compression). server has no compression.
ES_BACKENDS = [("es_default", None), ("es_large", 1000)]

# How many tasks the padding generator cycles through (see dyn._make_padding_rows).
PADDING_TASK_COUNT = 200

CSV_HEADER = [
    "timestamp_utc",
    "run",
    "epoch",
    "seed",
    "rows_per_epoch",
    "query_node_count",
    "task_count",
    "backend",
    "query_shape",
    "repeat_count",
    "warmup_count",
    "idle_seconds",
    "lat_mean_ms",
    "lat_median_ms",
    "lat_p90_ms",
    "lat_p100_ms",
    "lat_min_ms",
    "lat_max_ms",
    "lat_std_ms",
    "cpu_per_query_ms",
    "cpu_busy_ticks",
    "idle_cpu_rate_tps",
    "measure_window_s",
    "rss_mean_mb",
    "rss_max_mb",
    "vmhwm_mb",
    "rss_idle_mb",
    "es_took_mean_ms",
    "es_took_median_ms",
]

INGEST_CSV_HEADER = [
    "timestamp_utc",
    "run",
    "epoch",
    "is_warmup",
    "seed",
    "rows_per_epoch",
    "backend",
    "ingest_cpu_ms",
    "ingest_wall_ms",
    "ingest_rss_delta_mb",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    add_common_args(parser)
    parser.set_defaults(
        server_log="logs/server_resource_benchmark.log",
        out_csv="data/resource_benchmark.csv",
    )
    parser.add_argument("--ingestion-csv", type=str, default="data/resource_ingestion.csv")
    parser.add_argument("--raw-dir", type=str, default="data/resource_benchmark_raw")
    parser.add_argument("--solver-data-dir", type=str, default=str(dyn.SOLVER_DUMMY_DIR))
    # Experiment shape (mirrors the latency experiment).
    parser.add_argument("--runs", type=int, default=10, help="Runs (server restarts); error-bar dimension")
    parser.add_argument("--epochs", type=int, default=10, help="Recorded epochs per run")
    parser.add_argument("--warmup-epochs", type=int, default=0, help="Unrecorded warmup epochs per run")
    parser.add_argument("--rows-per-epoch", type=int, default=dyn.DEFAULT_ROWS_PER_EPOCH)
    parser.add_argument("--seed-base", type=int, default=1000, help="Per-run seed = seed_base + run")
    # Adaptive measurement.
    parser.add_argument("--min-repeats", type=int, default=10, help="Min measured query repeats per point")
    parser.add_argument("--max-repeats", type=int, default=1000, help="Max measured query repeats per point")
    parser.add_argument("--min-measure-seconds", type=float, default=1.0,
                        help="Keep repeating queries until the measured wall window reaches this")
    parser.add_argument("--warmup", type=int, default=3, help="Warmup queries discarded per point")
    parser.add_argument("--idle-seconds", type=float, default=3.0, help="Idle-baseline window (s)")
    parser.add_argument("--rss-poll-ms", type=float, default=50.0, help="RSS sampler interval (ms)")
    parser.add_argument("--shapes", type=str, nargs="+", default=["default"],
                        choices=list(QUERY_SHAPES.keys()), help="Query shapes to measure")
    parser.add_argument("--server-cores", type=str, default="0-9", help="taskset cores for sketch server")
    parser.add_argument("--es-cores", type=str, default="10-29", help="taskset cores for Elasticsearch")
    parser.add_argument("--client-cores", type=str, default="30-39", help="taskset cores for this client")
    parser.add_argument("--es-large-compression", type=int, default=1000)
    # Padding knobs (forwarded to gather_epoch_rows). Padding only, no emulator.
    parser.add_argument("--padding-cpu-ratio-max", type=float, default=dyn.DEFAULT_PADDING_CPU_RATIO_MAX)
    parser.add_argument("--padding-mem-ratio-max", type=float, default=dyn.DEFAULT_PADDING_MEM_RATIO_MAX)
    parser.add_argument("--padding-net-ratio-max", type=float, default=dyn.DEFAULT_PADDING_NET_RATIO_MAX)
    parser.add_argument("--emulator-url", type=str, default=dyn.DEFAULT_EMULATOR_URL)
    parser.add_argument("--no-padding", action="store_true", default=False)
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Server lifecycle (taskset-pinned)
# ---------------------------------------------------------------------------

def start_server_pinned(cores: str, log_path: Optional[Path], truncate_log: bool) -> subprocess.Popen:
    """Launch the sketch server under `taskset -c cores cargo run --release`.

    taskset execs cargo in-place (same PID), and cargo's child binary inherits
    the affinity, so the whole server tree stays pinned. start_new_session lets
    stop_server kill the process group.
    """
    stdout_target = None
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        mode = "w" if truncate_log else "a"
        stdout_target = open(log_path, mode, encoding="utf-8")
    cmd = ["cargo", "run", "--release"]
    if cores:
        cmd = ["taskset", "-c", cores] + cmd
    proc = subprocess.Popen(
        cmd,
        cwd=SERVER_DIR,
        stdout=stdout_target or subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        text=True,
    )
    if stdout_target is not None:
        proc._log_fh = stdout_target  # type: ignore[attr-defined]
    return proc


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------

def _safe_std(values: List[float]) -> float:
    return float(statistics.stdev(values)) if len(values) > 1 else 0.0


def _pct(values: List[float], pct: float) -> float:
    if not values:
        return float("nan")
    return float(np.percentile(np.asarray(values, dtype=np.float64), pct, method="linear"))


def _latency_stats(latencies: List[float]) -> Dict[str, float]:
    return {
        "mean": float(statistics.mean(latencies)),
        "median": float(statistics.median(latencies)),
        "p90": _pct(latencies, 90),
        "p100": max(latencies),
        "min": min(latencies),
        "max": max(latencies),
        "std": _safe_std(latencies),
    }


# ---------------------------------------------------------------------------
# Query drivers (return latency + raw meta, no resource logic here)
# ---------------------------------------------------------------------------

def _run_server_query(args, query_nodes, shape) -> Tuple[float, Dict[str, Any]]:
    body, elapsed_ms, meta = query_server_batch_custom(
        args.server_url, query_nodes, args.connect_timeout, args.query_timeout,
        aggs=shape["aggs"], percents=shape["percents"],
    )
    return elapsed_ms, {
        "x_server_timing_ms": meta.get("x_server_timing_ms"),
        "body_timing": meta.get("body_timing"),
    }


def _run_es_query(args, query_nodes, shape, compression, epoch) -> Tuple[float, Dict[str, Any]]:
    _, elapsed_ms, meta = query_es_nodes_custom(
        args.es_url, args.es_index, args.es_api_key, query_nodes,
        args.connect_timeout, args.es_timeout,
        aggs=shape["aggs"], percents=shape["percents"], epoch=epoch,
        tdigest_compression=compression, request_cache=False,
    )
    tooks = [
        float(pn["took_ms"]) for pn in meta.get("per_node", {}).values()
        if pn.get("took_ms") is not None
    ]
    return elapsed_ms, {"per_node_took_ms": tooks}


# ---------------------------------------------------------------------------
# Core measurement (adaptive repeats)
# ---------------------------------------------------------------------------

def measure_point(args: argparse.Namespace, pid: int, query_fn) -> Dict[str, Any]:
    """Warmup, idle-baseline, then adaptive measured repeats with CPU+RSS capture.

    `query_fn()` performs one query and returns (latency_ms, meta).
    Repeats until the wall window reaches --min-measure-seconds (>= --min-repeats),
    capped at --max-repeats, so CPU accumulates above jiffy granularity for both
    the cheap (sketch) and expensive (ES) backends.
    """
    # --- warmup (discarded) ---
    warmup_lat: List[float] = []
    for _ in range(args.warmup):
        lat, _ = query_fn()
        warmup_lat.append(lat)

    # --- idle baseline: CPU rate of the target process while no queries run ---
    ci0 = pm.read_cpu_ticks(pid)
    rss_idle = pm.read_rss(pid)
    time.sleep(args.idle_seconds)
    ci1 = pm.read_cpu_ticks(pid)
    idle_window_s = ci1.wall_s - ci0.wall_s
    idle_rate_tps = (ci1.total_ticks - ci0.total_ticks) / idle_window_s if idle_window_s > 0 else 0.0

    # --- measured window: latency + CPU + RSS together (adaptive count) ---
    sampler = pm.ResourceSampler(pid=pid, poll_interval_s=args.rss_poll_ms / 1000.0)
    sampler.start()
    cpu0 = pm.read_cpu_ticks(pid)
    latencies: List[float] = []
    metas: List[Dict[str, Any]] = []
    t_start = time.monotonic()
    while True:
        lat, meta = query_fn()
        latencies.append(lat)
        metas.append(meta)
        n = len(latencies)
        elapsed = time.monotonic() - t_start
        if n >= args.max_repeats:
            break
        if n >= args.min_repeats and elapsed >= args.min_measure_seconds:
            break
    cpu1 = pm.read_cpu_ticks(pid)
    sampler.stop()

    window_s = cpu1.wall_s - cpu0.wall_s
    busy_ticks = cpu1.total_ticks - cpu0.total_ticks
    idle_ticks_equiv = idle_rate_tps * window_s
    net_ticks = max(busy_ticks - idle_ticks_equiv, 0.0)
    repeat_count = len(latencies)
    cpu_per_query_ms = (net_ticks / pm.HZ) / repeat_count * 1000.0

    lat_stats = _latency_stats(latencies)
    es_tooks = [t for m in metas for t in m.get("per_node_took_ms", [])]

    return {
        "repeat_count": repeat_count,
        "latencies_ms": latencies,
        "lat_stats": lat_stats,
        "cpu_per_query_ms": cpu_per_query_ms,
        "cpu_busy_ticks": busy_ticks,
        "idle_cpu_rate_tps": idle_rate_tps,
        "idle_window_s": idle_window_s,
        "measure_window_s": window_s,
        "rss_mean_mb": sampler.mean_rss_mb(),
        "rss_max_mb": sampler.max_rss_mb(),
        "vmhwm_mb": sampler.max_vmhwm_mb(),
        "rss_idle_mb": rss_idle.vmrss_kb / 1024.0,
        "es_took_mean_ms": float(statistics.mean(es_tooks)) if es_tooks else None,
        "es_took_median_ms": float(statistics.median(es_tooks)) if es_tooks else None,
        # raw, for the sidecar
        "warmup_lat_ms": warmup_lat,
        "cpu_boundary": {
            "idle": [ci0.__dict__, ci1.__dict__],
            "measure": [cpu0.__dict__, cpu1.__dict__],
        },
        "rss_series": sampler.raw_series(),
        "query_metas": metas,
    }


# ---------------------------------------------------------------------------
# Per-run loop
# ---------------------------------------------------------------------------

def run_single(
    args: argparse.Namespace,
    run_idx: int,
    seed: int,
    query_nodes: List[str],
    context: dict,
    es_pid: int,
    summary_rows: List[List[Any]],
    ingest_rows_out: List[List[Any]],
    raw_dir: Path,
) -> None:
    print(f"\n========== RUN {run_idx + 1}/{args.runs} (seed={seed}) ==========")
    rng = random.Random(seed)

    server_log = None if args.server_log == "-" else resolve_repo_path(args.server_log)
    truncate = args.truncate_server_log and run_idx == 0
    server_proc = start_server_pinned(args.server_cores, server_log, truncate_log=truncate)
    try:
        wait_for_server(args.server_url, args.server_ready_timeout, args.connect_timeout, args.query_timeout)
        server_pid = pm.resolve_server_pid()
        if server_pid is None:
            raise RuntimeError("Could not resolve sketch server PID after start.")
        reset_es_index(args.es_url, args.es_index, args.es_api_key, args.connect_timeout, args.es_timeout)

        total_epochs = args.warmup_epochs + args.epochs
        for epoch_idx in range(total_epochs):
            is_warmup = epoch_idx < args.warmup_epochs
            recorded_epoch = epoch_idx - args.warmup_epochs  # 0..epochs-1
            label = f"warmup-{epoch_idx}" if is_warmup else f"epoch-{recorded_epoch}"
            print(f"--- run {run_idx + 1} / {label} ---")

            # Build rows: padding only (no emulator/solver dependency).
            rows, _, n_pad = multi.gather_epoch_rows(
                args=args, rng=rng, context=context, query_nodes=query_nodes,
                epoch=epoch_idx, running_tasks={},
            )
            if not rows:
                raise RuntimeError("Epoch produced zero rows (check --no-padding / --rows-per-epoch).")

            # Ingest once; measure CPU + RSS delta on both processes.
            s_cpu0 = pm.read_cpu_ticks(server_pid); s_rss0 = pm.read_rss(server_pid)
            e_cpu0 = pm.read_cpu_ticks(es_pid); e_rss0 = pm.read_rss(es_pid)
            server_ingest_ms, es_ingest_ms = multi.ingest_once(rows, epoch_idx, args)
            s_cpu1 = pm.read_cpu_ticks(server_pid); s_rss1 = pm.read_rss(server_pid)
            e_cpu1 = pm.read_cpu_ticks(es_pid); e_rss1 = pm.read_rss(es_pid)
            ts = datetime.now(timezone.utc).isoformat()
            for label_b, c0, c1, r0, r1, wall in [
                ("server", s_cpu0, s_cpu1, s_rss0, s_rss1, server_ingest_ms),
                ("es", e_cpu0, e_cpu1, e_rss0, e_rss1, es_ingest_ms),
            ]:
                ingest_rows_out.append([
                    ts, run_idx, recorded_epoch, int(is_warmup), seed, args.rows_per_epoch, label_b,
                    f"{(c1.total_ticks - c0.total_ticks) / pm.HZ * 1000.0:.4f}",
                    f"{wall:.4f}",
                    f"{(r1.vmrss_kb - r0.vmrss_kb) / 1024.0:.4f}",
                ])
            print(f"  ingested {n_pad} rows; server={server_ingest_ms:.0f}ms es={es_ingest_ms:.0f}ms")

            if is_warmup:
                continue

            # Measure each backend x shape.
            for shape_name in args.shapes:
                shape = QUERY_SHAPES[shape_name]
                backends = [("server", server_pid, lambda s=shape: _run_server_query(args, query_nodes, s))]
                for lbl, comp in ES_BACKENDS:
                    comp_val = args.es_large_compression if lbl == "es_large" else comp
                    backends.append(
                        (lbl, es_pid,
                         lambda s=shape, c=comp_val, e=epoch_idx: _run_es_query(args, query_nodes, s, c, e))
                    )

                for backend_label, pid, query_fn in backends:
                    result = measure_point(args, pid, query_fn)
                    lat = result["lat_stats"]
                    print(f"    {backend_label}: lat={lat['mean']:.2f}ms "
                          f"cpu/q={result['cpu_per_query_ms']:.3f}ms "
                          f"reps={result['repeat_count']} rss={result['rss_mean_mb']:.1f}MB")
                    summary_rows.append([
                        datetime.now(timezone.utc).isoformat(), run_idx, recorded_epoch, seed,
                        args.rows_per_epoch, len(query_nodes), PADDING_TASK_COUNT,
                        backend_label, shape_name, result["repeat_count"], args.warmup, args.idle_seconds,
                        f"{lat['mean']:.4f}", f"{lat['median']:.4f}", f"{lat['p90']:.4f}",
                        f"{lat['p100']:.4f}", f"{lat['min']:.4f}", f"{lat['max']:.4f}", f"{lat['std']:.4f}",
                        f"{result['cpu_per_query_ms']:.6f}", result["cpu_busy_ticks"],
                        f"{result['idle_cpu_rate_tps']:.4f}", f"{result['measure_window_s']:.4f}",
                        f"{result['rss_mean_mb']:.4f}", f"{result['rss_max_mb']:.4f}",
                        f"{result['vmhwm_mb']:.4f}", f"{result['rss_idle_mb']:.4f}",
                        f"{result['es_took_mean_ms']:.4f}" if result["es_took_mean_ms"] is not None else "",
                        f"{result['es_took_median_ms']:.4f}" if result["es_took_median_ms"] is not None else "",
                    ])
                    raw = {
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                        "run": run_idx, "epoch": recorded_epoch, "seed": seed,
                        "rows_per_epoch": args.rows_per_epoch,
                        "backend": backend_label, "query_shape": shape_name, "shape_spec": shape,
                        "query_node_count": len(query_nodes), "task_count": PADDING_TASK_COUNT,
                        "measured_pid": pid,
                        "env": pm.env_fingerprint(server_pid, es_pid),
                        "result": result,
                    }
                    raw_path = raw_dir / f"run{run_idx}_epoch{recorded_epoch}_{backend_label}_{shape_name}.json"
                    with open(raw_path, "w", encoding="utf-8") as fh:
                        json.dump(raw, fh, indent=2, default=str)
    finally:
        stop_server(server_proc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _write_csv(path: Path, header: List[str], rows: List[List[Any]], truncate: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = truncate or not path.exists()
    mode = "w" if truncate else "a"
    with open(path, mode, newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if new_file:
            writer.writerow(header)
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    query_nodes = parse_nodes_config(args.nodes_config)

    out_csv = resolve_repo_path(args.out_csv)
    ingest_csv = resolve_repo_path(args.ingestion_csv)
    raw_dir = resolve_repo_path(args.raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)

    assets = dyn._load_solver_assets(Path(args.solver_data_dir))
    context = dyn._build_solver_context(assets, query_nodes)

    es_pid = pm.resolve_es_pid()
    if es_pid is None:
        raise RuntimeError("Could not resolve Elasticsearch PID. Is ES running?")
    pm.pin_cpu(es_pid, args.es_cores)
    pm.pin_cpu(os.getpid(), args.client_cores)
    print(f"ES pid={es_pid} pinned to {args.es_cores}; client pid={os.getpid()} pinned to {args.client_cores}")
    print(f"HZ={pm.HZ} -> CPU resolution {1000.0/pm.HZ:.0f}ms; "
          f"adaptive repeats: min={args.min_repeats} max={args.max_repeats} "
          f"until >= {args.min_measure_seconds}s")
    print(f"Experiment: runs={args.runs} epochs={args.epochs} (+{args.warmup_epochs} warmup) "
          f"rows/epoch={args.rows_per_epoch}")

    summary_rows: List[List[Any]] = []
    ingest_rows_out: List[List[Any]] = []

    for run_idx in range(args.runs):
        seed = args.seed_base + run_idx
        run_single(args, run_idx, seed, query_nodes, context, es_pid,
                   summary_rows, ingest_rows_out, raw_dir)

    _write_csv(out_csv, CSV_HEADER, summary_rows, args.truncate_csv)
    _write_csv(ingest_csv, INGEST_CSV_HEADER, ingest_rows_out, args.truncate_csv)
    print(f"\nWrote {out_csv} ({len(summary_rows)} rows)")
    print(f"Wrote {ingest_csv} ({len(ingest_rows_out)} rows)")
    print(f"Raw sidecars in {raw_dir}")


if __name__ == "__main__":
    main()

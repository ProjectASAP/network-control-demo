#!/usr/bin/env python3
"""Multi-run, multi-epoch query breakdown benchmark.

For each run:
  - Reset ES index.
  - Restart sketch server.
  - Restart emulator with a per-run seed.
  - For each epoch:
    - Generate rows for that epoch.
    - Ingest once into sketch server + ES.
    - Measure repeated query timings for multiple query shapes.

This benchmark intentionally does not warm query caches. ES requests are sent
with request_cache=false, and no untimed warmup query is issued.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import random
import statistics
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from rtt_sweep_common import (
    add_common_args,
    parse_nodes_config,
    query_es_nodes_custom,
    query_server_batch_custom,
    reset_es_index,
    resolve_repo_path,
    start_server,
    stop_server,
    wait_for_server,
)

import run_dynamic_epoch_benchmark as dyn
import run_multi_run_epoch_benchmark as multi


REPO_ROOT = Path(__file__).resolve().parents[1]

QUERY_SHAPES = [
    {"name": "p50", "aggs": ["percentiles"], "percents": [50]},
    {"name": "p90", "aggs": ["percentiles"], "percents": [90]},
    {"name": "p100", "aggs": ["percentiles"], "percents": [100]},
    {"name": "all_quantile", "aggs": ["percentiles"], "percents": [50, 90, 100]},
    {"name": "sum", "aggs": ["sum"], "percents": None},
    {"name": "all_in", "aggs": ["percentiles", "sum"], "percents": [50, 90, 100]},
]

CSV_HEADER = [
    "timestamp_utc",
    "run_id",
    "epoch",
    "seed",
    "rows_ingested",
    "emulator_rows",
    "padding_rows",
    "backend",
    "query_shape",
    "es_large_compression",
    "cache_mode",
    "repeat_count",
    "mean_elapsed_ms",
    "median_elapsed_ms",
    "p90_elapsed_ms",
    "p100_elapsed_ms",
    "min_elapsed_ms",
    "max_elapsed_ms",
    "std_elapsed_ms",
    "mean_es_took_ms",
    "median_es_took_ms",
    "json_path",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Multi-run multi-epoch query breakdown benchmark with per-epoch re-ingestion."
    )
    add_common_args(parser)
    parser.set_defaults(
        server_log="logs/server_multi_run_query_breakdown.log",
        out_csv="data/multi_run_epoch_query_breakdown.csv",
    )
    parser.add_argument("--queries-dir", type=str, default="data/multi_run_epoch_query_breakdown_raw")
    parser.add_argument("--solver-data-dir", type=str, default=str(dyn.SOLVER_DUMMY_DIR))
    parser.add_argument("--n-runs", type=int, default=10)
    parser.add_argument("--n-epochs", type=int, default=10)
    parser.add_argument("--seed-base", type=int, default=1000)
    parser.add_argument("--rows-per-epoch", type=int, default=dyn.DEFAULT_ROWS_PER_EPOCH)
    parser.add_argument("--emulator-url", type=str, default=dyn.DEFAULT_EMULATOR_URL)
    parser.add_argument("--emulator-log", type=str, default="logs/emulator_multi_run_query_breakdown.log")
    parser.add_argument("--emulator-ready-timeout", type=float, default=30.0)
    parser.add_argument("--skip-emulator-start", action="store_true", default=False)
    parser.add_argument("--epoch-length-s", type=float, default=dyn.DEFAULT_EPOCH_LENGTH_S)
    parser.add_argument("--data-rate", type=int, default=1)
    parser.add_argument("--no-padding", action="store_true", default=False)
    parser.add_argument("--padding-cpu-ratio-max", type=float, default=dyn.DEFAULT_PADDING_CPU_RATIO_MAX)
    parser.add_argument("--padding-mem-ratio-max", type=float, default=dyn.DEFAULT_PADDING_MEM_RATIO_MAX)
    parser.add_argument("--padding-net-ratio-max", type=float, default=dyn.DEFAULT_PADDING_NET_RATIO_MAX)
    parser.add_argument("--es-large-compression", type=int, default=1000)
    parser.add_argument("--repeats", type=int, default=7)
    return parser.parse_args()


def _safe_std(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    return float(statistics.stdev(values))


def _pct(values: list[float], pct: float) -> float:
    if not values:
        return float("nan")
    return float(np.percentile(np.asarray(values, dtype=np.float64), pct, method="linear"))


def _flatten_es_took(samples: list[dict[str, Any]]) -> list[float]:
    tooks: list[float] = []
    for sample in samples:
        for per_node in sample.get("per_node", {}).values():
            took = per_node.get("took_ms")
            if took is not None:
                tooks.append(float(took))
    return tooks


def _summarize_samples(samples: list[dict[str, Any]]) -> dict[str, float]:
    elapsed = [float(sample["elapsed_ms"]) for sample in samples]
    es_tooks = _flatten_es_took(samples)
    return {
        "repeat_count": len(samples),
        "mean_elapsed_ms": float(statistics.mean(elapsed)),
        "median_elapsed_ms": float(statistics.median(elapsed)),
        "p90_elapsed_ms": _pct(elapsed, 90),
        "p100_elapsed_ms": max(elapsed),
        "min_elapsed_ms": min(elapsed),
        "max_elapsed_ms": max(elapsed),
        "std_elapsed_ms": _safe_std(elapsed),
        "mean_es_took_ms": float(statistics.mean(es_tooks)) if es_tooks else float("nan"),
        "median_es_took_ms": float(statistics.median(es_tooks)) if es_tooks else float("nan"),
    }


def _measure_server_shape(
    args: argparse.Namespace,
    query_nodes: list[str],
    shape: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    samples: list[dict[str, Any]] = []
    for _ in range(args.repeats):
        _, elapsed_ms, meta = query_server_batch_custom(
            args.server_url,
            query_nodes,
            args.connect_timeout,
            args.query_timeout,
            aggs=shape["aggs"],
            percents=shape["percents"],
        )
        samples.append(
            {
                "elapsed_ms": elapsed_ms,
                "x_server_timing_ms": meta.get("x_server_timing_ms"),
                "body_timing": meta.get("body_timing"),
                "request_payload": meta.get("request_payload"),
            }
        )
    return {"backend": "server", "cache_mode": "na"}, samples


def _measure_es_shape(
    args: argparse.Namespace,
    query_nodes: list[str],
    shape: dict[str, Any],
    epoch: int,
    backend: str,
    tdigest_compression: int | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    samples: list[dict[str, Any]] = []
    for _ in range(args.repeats):
        _, elapsed_ms, meta = query_es_nodes_custom(
            args.es_url,
            args.es_index,
            args.es_api_key,
            query_nodes,
            args.connect_timeout,
            args.es_timeout,
            aggs=shape["aggs"],
            percents=shape["percents"],
            epoch=epoch,
            tdigest_compression=tdigest_compression,
            request_cache=False,
        )
        samples.append(
            {
                "elapsed_ms": elapsed_ms,
                "request_cache": meta.get("request_cache"),
                "request_payloads": meta.get("request_payloads"),
                "per_node": meta.get("per_node"),
            }
        )
    return {"backend": backend, "cache_mode": "es_cache_disabled"}, samples


def _write_sidecar(
    queries_dir: Path,
    run_id: int,
    recorded_epoch: int,
    payload: dict[str, Any],
) -> Path:
    path = queries_dir / f"run{run_id:02d}_epoch{recorded_epoch:02d}.json"
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    return path


def main() -> None:
    args = parse_args()
    query_nodes = parse_nodes_config(args.nodes_config)
    out_csv = resolve_repo_path(args.out_csv)
    queries_dir = resolve_repo_path(args.queries_dir)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    if args.truncate_csv and queries_dir.exists():
        shutil.rmtree(queries_dir)
    queries_dir.mkdir(parents=True, exist_ok=True)

    assets = dyn._load_solver_assets(Path(args.solver_data_dir))
    context = dyn._build_solver_context(assets, query_nodes)
    if "N000" in context["nodes"]:
        raise RuntimeError("Node alignment failed: N000 present in solver context.")

    with open(out_csv, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(CSV_HEADER)

        for run_id in range(1, args.n_runs + 1):
            seed = args.seed_base + run_id
            rng = random.Random(seed)
            print(f"\n========== RUN {run_id}/{args.n_runs} (seed={seed}) ==========")

            reset_es_index(args.es_url, args.es_index, args.es_api_key, args.connect_timeout, args.es_timeout)
            server_log_path = None if args.server_log == "-" else resolve_repo_path(args.server_log)
            server_proc = start_server(server_log_path, truncate_log=args.truncate_server_log and run_id == 1)
            emulator_proc: subprocess.Popen | None = None

            try:
                wait_for_server(args.server_url, args.server_ready_timeout, args.connect_timeout, args.query_timeout)
                if not args.skip_emulator_start:
                    emulator_proc = multi.start_emulator_with_seed(args, seed)
                    dyn.wait_for_emulator(
                        args.emulator_url,
                        args.emulator_ready_timeout,
                        args.connect_timeout,
                        args.query_timeout,
                    )

                running_tasks: dict[str, object] = {}
                for epoch_idx in range(args.n_epochs):
                    recorded_epoch = epoch_idx + 1
                    print(f"\n--- run {run_id} / epoch-{recorded_epoch} ---")
                    rows, emulator_rows, padding_rows = multi.gather_epoch_rows(
                        args=args,
                        rng=rng,
                        context=context,
                        query_nodes=query_nodes,
                        epoch=epoch_idx,
                        running_tasks=running_tasks,
                    )
                    rows_ingested = len(rows)
                    if rows_ingested <= 0:
                        raise RuntimeError(f"Epoch {recorded_epoch} produced zero rows.")

                    server_ingest_ms, es_ingest_ms = multi.ingest_once(rows, epoch_idx, args)
                    print(
                        f"  ingested {rows_ingested} rows (emu={emulator_rows}, pad={padding_rows}); "
                        f"server={server_ingest_ms:.0f}ms es={es_ingest_ms:.0f}ms"
                    )

                    sidecar_results: list[dict[str, Any]] = []
                    for shape in QUERY_SHAPES:
                        print(f"Benchmarking shape={shape['name']} backend=server")
                        server_info, server_samples = _measure_server_shape(args, query_nodes, shape)
                        server_stats = _summarize_samples(server_samples)
                        sidecar_results.append({
                            "backend": server_info["backend"],
                            "query_shape": shape["name"],
                            "cache_mode": server_info["cache_mode"],
                            "samples": server_samples,
                            "summary": server_stats,
                        })

                        for backend, compression in [("es_default", None), ("es_large", args.es_large_compression)]:
                            print(f"Benchmarking shape={shape['name']} backend={backend}")
                            es_info, es_samples = _measure_es_shape(
                                args, query_nodes, shape, epoch_idx, backend, compression
                            )
                            es_stats = _summarize_samples(es_samples)
                            sidecar_results.append({
                                "backend": es_info["backend"],
                                "query_shape": shape["name"],
                                "cache_mode": es_info["cache_mode"],
                                "samples": es_samples,
                                "summary": es_stats,
                            })

                    sidecar = {
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                        "run_id": run_id,
                        "epoch": recorded_epoch,
                        "seed": seed,
                        "rows_ingested": rows_ingested,
                        "emulator_rows": emulator_rows,
                        "padding_rows": padding_rows,
                        "server_ingest_ms": server_ingest_ms,
                        "es_ingest_ms": es_ingest_ms,
                        "query_nodes": query_nodes,
                        "query_shapes": QUERY_SHAPES,
                        "args": vars(args),
                        "results": sidecar_results,
                    }
                    json_path = _write_sidecar(queries_dir, run_id, recorded_epoch, sidecar)

                    for result in sidecar_results:
                        summary = result["summary"]
                        writer.writerow([
                            datetime.now(timezone.utc).isoformat(),
                            run_id,
                            recorded_epoch,
                            seed,
                            rows_ingested,
                            emulator_rows,
                            padding_rows,
                            result["backend"],
                            result["query_shape"],
                            args.es_large_compression,
                            result["cache_mode"],
                            summary["repeat_count"],
                            f"{summary['mean_elapsed_ms']:.4f}",
                            f"{summary['median_elapsed_ms']:.4f}",
                            f"{summary['p90_elapsed_ms']:.4f}",
                            f"{summary['p100_elapsed_ms']:.4f}",
                            f"{summary['min_elapsed_ms']:.4f}",
                            f"{summary['max_elapsed_ms']:.4f}",
                            f"{summary['std_elapsed_ms']:.4f}",
                            f"{summary['mean_es_took_ms']:.4f}" if not np.isnan(summary["mean_es_took_ms"]) else "",
                            f"{summary['median_es_took_ms']:.4f}" if not np.isnan(summary["median_es_took_ms"]) else "",
                            str(json_path.relative_to(REPO_ROOT)),
                        ])
                    csv_file.flush()
            finally:
                dyn.stop_process(emulator_proc)
                stop_server(server_proc)

    print(f"Wrote {out_csv}")
    print(f"Wrote {queries_dir}/")


if __name__ == "__main__":
    main()

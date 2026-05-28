#!/usr/bin/env python3
"""Fixed-ingestion query breakdown benchmark.

Build one fixed dataset, ingest it once into sketch server + ES, then measure
repeated query timings for several query shapes without re-ingesting.
"""

from __future__ import annotations

import argparse
import csv
import json
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
    "seed",
    "rows_ingested",
    "emulator_rows",
    "padding_rows",
    "backend",
    "query_shape",
    "es_large_compression",
    "cache_mode",
    "repeat_count",
    "warmup_elapsed_ms",
    "mean_elapsed_ms",
    "median_elapsed_ms",
    "p90_elapsed_ms",
    "p100_elapsed_ms",
    "min_elapsed_ms",
    "max_elapsed_ms",
    "std_elapsed_ms",
    "mean_es_took_ms",
    "median_es_took_ms",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest one fixed dataset once, then benchmark repeated query breakdown timings."
    )
    add_common_args(parser)
    parser.set_defaults(
        server_log="logs/server_fixed_ingestion_query_breakdown.log",
        out_csv="data/fixed_ingestion_query_breakdown.csv",
    )
    parser.add_argument("--raw-json", type=str, default="data/fixed_ingestion_query_breakdown_raw/benchmark.json")
    parser.add_argument("--solver-data-dir", type=str, default=str(dyn.SOLVER_DUMMY_DIR))
    parser.add_argument("--rows-per-epoch", type=int, default=dyn.DEFAULT_ROWS_PER_EPOCH)
    parser.add_argument("--emulator-url", type=str, default=dyn.DEFAULT_EMULATOR_URL)
    parser.add_argument("--emulator-log", type=str, default="logs/emulator_fixed_ingestion_query_breakdown.log")
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


def _measure_server_shape(args: argparse.Namespace, query_nodes: list[str], shape: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    _, warmup_elapsed_ms, warmup_meta = query_server_batch_custom(
        args.server_url,
        query_nodes,
        args.connect_timeout,
        args.query_timeout,
        aggs=shape["aggs"],
        percents=shape["percents"],
    )

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
    return {
        "backend": "server",
        "cache_mode": "na",
        "warmup_elapsed_ms": warmup_elapsed_ms,
        "warmup_meta": warmup_meta,
    }, samples


def _measure_es_shape(
    args: argparse.Namespace,
    query_nodes: list[str],
    shape: dict[str, Any],
    backend: str,
    tdigest_compression: int | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    _, warmup_elapsed_ms, warmup_meta = query_es_nodes_custom(
        args.es_url,
        args.es_index,
        args.es_api_key,
        query_nodes,
        args.connect_timeout,
        args.es_timeout,
        aggs=shape["aggs"],
        percents=shape["percents"],
        epoch=0,
        tdigest_compression=tdigest_compression,
        request_cache=False,
    )

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
            epoch=0,
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
    return {
        "backend": backend,
        "cache_mode": "es_cache_disabled",
        "warmup_elapsed_ms": warmup_elapsed_ms,
        "warmup_meta": warmup_meta,
    }, samples


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    query_nodes = parse_nodes_config(args.nodes_config)
    out_csv = resolve_repo_path(args.out_csv)
    raw_json = resolve_repo_path(args.raw_json)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    raw_json.parent.mkdir(parents=True, exist_ok=True)

    assets = dyn._load_solver_assets(Path(args.solver_data_dir))
    context = dyn._build_solver_context(assets, query_nodes)
    if "N000" in context["nodes"]:
        raise RuntimeError("Node alignment failed: N000 present in solver context.")

    rows, emulator_rows, padding_rows = multi.gather_epoch_rows(
        args=args,
        rng=rng,
        context=context,
        query_nodes=query_nodes,
        epoch=0,
        running_tasks={},
    )
    rows_ingested = len(rows)
    if rows_ingested <= 0:
        raise RuntimeError("Fixed-ingestion benchmark produced zero rows. Disable --no-padding or supply data.")

    reset_es_index(args.es_url, args.es_index, args.es_api_key, args.connect_timeout, args.es_timeout)
    server_log_path = None if args.server_log == "-" else resolve_repo_path(args.server_log)
    server_proc = start_server(server_log_path, truncate_log=args.truncate_server_log)
    emulator_proc: subprocess.Popen | None = None

    try:
        wait_for_server(args.server_url, args.server_ready_timeout, args.connect_timeout, args.query_timeout)
        if not args.skip_emulator_start:
            emulator_proc = dyn.start_emulator(args)
            dyn.wait_for_emulator(
                args.emulator_url,
                args.emulator_ready_timeout,
                args.connect_timeout,
                args.query_timeout,
            )

        server_ingest_ms, es_ingest_ms = multi.ingest_once(rows, 0, args)

        summary_rows: list[list[Any]] = []
        raw_results: list[dict[str, Any]] = []
        for shape in QUERY_SHAPES:
            print(f"Benchmarking shape={shape['name']} backend=server")
            server_info, server_samples = _measure_server_shape(args, query_nodes, shape)
            server_stats = _summarize_samples(server_samples)
            summary_rows.append([
                datetime.now(timezone.utc).isoformat(),
                args.seed,
                rows_ingested,
                emulator_rows,
                padding_rows,
                server_info["backend"],
                shape["name"],
                args.es_large_compression,
                server_info["cache_mode"],
                server_stats["repeat_count"],
                f"{server_info['warmup_elapsed_ms']:.4f}",
                f"{server_stats['mean_elapsed_ms']:.4f}",
                f"{server_stats['median_elapsed_ms']:.4f}",
                f"{server_stats['p90_elapsed_ms']:.4f}",
                f"{server_stats['p100_elapsed_ms']:.4f}",
                f"{server_stats['min_elapsed_ms']:.4f}",
                f"{server_stats['max_elapsed_ms']:.4f}",
                f"{server_stats['std_elapsed_ms']:.4f}",
                "",
                "",
            ])
            raw_results.append({
                "backend": server_info["backend"],
                "query_shape": shape["name"],
                "cache_mode": server_info["cache_mode"],
                "warmup_elapsed_ms": server_info["warmup_elapsed_ms"],
                "warmup_meta": server_info["warmup_meta"],
                "samples": server_samples,
                "summary": server_stats,
            })

            for backend, compression in [("es_default", None), ("es_large", args.es_large_compression)]:
                print(f"Benchmarking shape={shape['name']} backend={backend}")
                es_info, es_samples = _measure_es_shape(args, query_nodes, shape, backend, compression)
                es_stats = _summarize_samples(es_samples)
                summary_rows.append([
                    datetime.now(timezone.utc).isoformat(),
                    args.seed,
                    rows_ingested,
                    emulator_rows,
                    padding_rows,
                    es_info["backend"],
                    shape["name"],
                    args.es_large_compression,
                    es_info["cache_mode"],
                    es_stats["repeat_count"],
                    f"{es_info['warmup_elapsed_ms']:.4f}",
                    f"{es_stats['mean_elapsed_ms']:.4f}",
                    f"{es_stats['median_elapsed_ms']:.4f}",
                    f"{es_stats['p90_elapsed_ms']:.4f}",
                    f"{es_stats['p100_elapsed_ms']:.4f}",
                    f"{es_stats['min_elapsed_ms']:.4f}",
                    f"{es_stats['max_elapsed_ms']:.4f}",
                    f"{es_stats['std_elapsed_ms']:.4f}",
                    f"{es_stats['mean_es_took_ms']:.4f}" if not np.isnan(es_stats["mean_es_took_ms"]) else "",
                    f"{es_stats['median_es_took_ms']:.4f}" if not np.isnan(es_stats["median_es_took_ms"]) else "",
                ])
                raw_results.append({
                    "backend": es_info["backend"],
                    "query_shape": shape["name"],
                    "cache_mode": es_info["cache_mode"],
                    "warmup_elapsed_ms": es_info["warmup_elapsed_ms"],
                    "warmup_meta": es_info["warmup_meta"],
                    "samples": es_samples,
                    "summary": es_stats,
                })

        with open(out_csv, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(CSV_HEADER)
            writer.writerows(summary_rows)

        sidecar = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "args": vars(args),
            "dataset": {
                "seed": args.seed,
                "rows_ingested": rows_ingested,
                "emulator_rows": emulator_rows,
                "padding_rows": padding_rows,
                "server_ingest_ms": server_ingest_ms,
                "es_ingest_ms": es_ingest_ms,
                "query_nodes": query_nodes,
            },
            "query_shapes": QUERY_SHAPES,
            "results": raw_results,
        }
        with open(raw_json, "w", encoding="utf-8") as fh:
            json.dump(sidecar, fh, indent=2)

        print(f"Wrote {out_csv}")
        print(f"Wrote {raw_json}")
    finally:
        dyn.stop_process(emulator_proc)
        stop_server(server_proc)


if __name__ == "__main__":
    main()

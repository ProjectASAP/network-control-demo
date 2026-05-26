#!/usr/bin/env python3
"""Sweep Elasticsearch ingest and query time across exponentially increasing row counts."""

from __future__ import annotations

import argparse
import csv
import random
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from rtt_sweep_common import (
    add_common_args,
    bulk_ingest_es,
    iter_batches,
    parse_nodes_config,
    query_es_nodes,
    reset_es_index,
    resolve_repo_path,
)


DEFAULT_START_ROWS = 1_000
DEFAULT_MAX_ROWS = 1_024_000
DEFAULT_REPEATS = 3
DEFAULT_OUT_CSV = "data/es_ingest_query_sweep.csv"
DEFAULT_OUT_SUMMARY_CSV = "data/es_ingest_query_sweep_summary.csv"
DEFAULT_OUT_PLOT = "plots/es_ingest_query_sweep.png"


@dataclass
class RepeatResult:
    rows: int
    repeat: int
    es_ingest_ms: float
    es_query_ms: float


@dataclass
class SummaryResult:
    rows: int
    median_ingest_ms: float
    median_query_ms: float
    min_ingest_ms: float
    max_ingest_ms: float
    min_query_ms: float
    max_query_ms: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark Elasticsearch ingest and query latency while sweeping row counts "
            "from 1,000 upward by doubling."
        )
    )
    parser.add_argument("--start-rows", type=int, default=DEFAULT_START_ROWS, help="Starting row count")
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS, help="Maximum row count")
    parser.add_argument("--multiplier", type=int, default=2, help="Row-count multiplier between sweep points")
    parser.add_argument("--repeats", type=int, default=DEFAULT_REPEATS, help="Runs per row count")
    parser.add_argument(
        "--summary-csv",
        type=str,
        default=DEFAULT_OUT_SUMMARY_CSV,
        help="Aggregated per-row-count summary CSV",
    )
    parser.add_argument(
        "--out-plot",
        type=str,
        default=DEFAULT_OUT_PLOT,
        help="Output plot filename",
    )
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


def write_detail_csv(path: Path, results: List[RepeatResult]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["timestamp_utc", "rows", "repeat", "es_ingest_ms", "es_query_ms"])
        for result in results:
            writer.writerow(
                [
                    datetime.now(timezone.utc).isoformat(),
                    result.rows,
                    result.repeat,
                    f"{result.es_ingest_ms:.4f}",
                    f"{result.es_query_ms:.4f}",
                ]
            )


def build_summary(results: List[RepeatResult]) -> List[SummaryResult]:
    grouped: Dict[int, List[RepeatResult]] = {}
    for result in results:
        grouped.setdefault(result.rows, []).append(result)

    summary_rows: List[SummaryResult] = []
    for rows in sorted(grouped):
        ingest = [item.es_ingest_ms for item in grouped[rows]]
        query = [item.es_query_ms for item in grouped[rows]]
        summary_rows.append(
            SummaryResult(
                rows=rows,
                median_ingest_ms=statistics.median(ingest),
                median_query_ms=statistics.median(query),
                min_ingest_ms=min(ingest),
                max_ingest_ms=max(ingest),
                min_query_ms=min(query),
                max_query_ms=max(query),
            )
        )
    return summary_rows


def write_summary_csv(path: Path, summary_rows: List[SummaryResult]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "rows",
                "median_ingest_ms",
                "median_query_ms",
                "min_ingest_ms",
                "max_ingest_ms",
                "min_query_ms",
                "max_query_ms",
            ]
        )
        for result in summary_rows:
            writer.writerow(
                [
                    result.rows,
                    f"{result.median_ingest_ms:.4f}",
                    f"{result.median_query_ms:.4f}",
                    f"{result.min_ingest_ms:.4f}",
                    f"{result.max_ingest_ms:.4f}",
                    f"{result.min_query_ms:.4f}",
                    f"{result.max_query_ms:.4f}",
                ]
            )


def plot_results(summary_rows: List[SummaryResult], out_path: Path) -> None:
    import matplotlib.pyplot as plt

    xs = [row.rows for row in summary_rows]
    ingest = [row.median_ingest_ms for row in summary_rows]
    query = [row.median_query_ms for row in summary_rows]

    plt.figure(figsize=(10, 6))
    plt.plot(xs, ingest, marker="o", label="ES ingest median (ms)")
    plt.plot(xs, query, marker="o", label="ES query median (ms)")
    plt.xscale("log", base=2)
    plt.xlabel("Rows")
    plt.ylabel("Latency (ms)")
    plt.title("Elasticsearch Ingest and Query Latency vs Row Count")
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
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_csv.parent.mkdir(parents=True, exist_ok=True)
    out_plot.parent.mkdir(parents=True, exist_ok=True)

    repeat_results: List[RepeatResult] = []

    for rows in row_counts:
        print(f"\n=== Sweep rows={rows} ===")
        for repeat in range(1, args.repeats + 1):
            print(f"  repeat {repeat}/{args.repeats}")
            reset_es_index(
                args.es_url,
                args.es_index,
                args.es_api_key,
                args.connect_timeout,
                args.es_timeout,
            )

            rng = random.Random(args.seed + rows * 10_000 + repeat)
            total_batches = (rows + args.batch_size - 1) // args.batch_size
            log_every = max(1, total_batches // 10)
            es_ingest_ms = 0.0

            try:
                for batch_idx, batch in enumerate(
                    iter_batches(rows, nodes, rng, args.batch_size), start=1
                ):
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
                            f"    ingest progress: {batch_idx}/{total_batches} batches "
                            f"({batch_idx * 100 // total_batches}%)"
                        )

                _, es_query_ms = query_es_nodes(
                    args.es_url,
                    args.es_index,
                    args.es_api_key,
                    nodes,
                    args.connect_timeout,
                    args.es_timeout,
                )
            except Exception:
                print(f"  failed at rows={rows} repeat={repeat}")
                raise

            repeat_results.append(
                RepeatResult(
                    rows=rows,
                    repeat=repeat,
                    es_ingest_ms=es_ingest_ms,
                    es_query_ms=es_query_ms,
                )
            )
            print(
                f"    ES ingest: {es_ingest_ms:.2f} ms | "
                f"ES query: {es_query_ms:.2f} ms"
            )

    summary_rows = build_summary(repeat_results)
    write_detail_csv(out_csv, repeat_results)
    write_summary_csv(summary_csv, summary_rows)
    plot_results(summary_rows, out_plot)
    print(f"\nWrote {out_csv}")
    print(f"Wrote {summary_csv}")
    print(f"Wrote {out_plot}")


if __name__ == "__main__":
    main()

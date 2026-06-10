#!/usr/bin/env python3
"""Sweep sketch server ingest and query time across exponentially increasing row counts."""

from __future__ import annotations

import argparse
import csv
import random
import statistics
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from rtt_sweep_common import (
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_INGEST_RETRIES,
    DEFAULT_INGEST_RETRY_BACKOFF,
    DEFAULT_QUERY_TIMEOUT,
    DEFAULT_SERVER_READY_TIMEOUT,
    ingest_server,
    iter_batches,
    parse_nodes_config,
    query_server_batch,
    resolve_repo_path,
    stop_server,
    wait_for_server,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_START_ROWS = 1_000
DEFAULT_MAX_ROWS = 1_024_000
DEFAULT_REPEATS = 3
DEFAULT_BATCH_SIZE = 1_000
DEFAULT_SERVER_URL = "http://127.0.0.1:10101"
DEFAULT_OUT_CSV = "data/sketch_ingest_query_sweep.csv"
DEFAULT_OUT_SUMMARY_CSV = "data/sketch_ingest_query_sweep_summary.csv"
DEFAULT_OUT_PLOT = "plots/sketch_ingest_query_sweep.png"
DEFAULT_SERVER_LOG = "logs/sketch_sweep_server.log"
DEFAULT_SERVER_BIN = (
    REPO_ROOT / "single_node_server/network-control-server/target/release/network-control-server"
)


@dataclass
class RepeatResult:
    rows: int
    repeat: int
    server_ingest_ms: float
    server_query_ms: float


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
            "Benchmark sketch-server ingest and query latency while sweeping row counts "
            "from 1,000 upward by doubling."
        )
    )
    parser.add_argument("--start-rows", type=int, default=DEFAULT_START_ROWS)
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS)
    parser.add_argument("--multiplier", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--server-url", type=str, default=DEFAULT_SERVER_URL)
    parser.add_argument("--server-bin", type=str, default=str(DEFAULT_SERVER_BIN))
    parser.add_argument("--server-log", type=str, default=DEFAULT_SERVER_LOG)
    parser.add_argument(
        "--skip-server-start",
        action="store_true",
        default=False,
        help="Use an already running sketch server instead of starting/stopping one per repeat",
    )
    parser.add_argument("--connect-timeout", type=float, default=DEFAULT_CONNECT_TIMEOUT)
    parser.add_argument("--ingest-timeout", type=float, default=60.0)
    parser.add_argument("--query-timeout", type=float, default=DEFAULT_QUERY_TIMEOUT)
    parser.add_argument("--server-ready-timeout", type=float, default=DEFAULT_SERVER_READY_TIMEOUT)
    parser.add_argument("--ingest-retries", type=int, default=DEFAULT_INGEST_RETRIES)
    parser.add_argument("--ingest-retry-backoff", type=float, default=DEFAULT_INGEST_RETRY_BACKOFF)
    parser.add_argument(
        "--nodes-config",
        type=str,
        default="single_node_server/network-control-server/server-config.yaml",
    )
    parser.add_argument("--out-csv", type=str, default=DEFAULT_OUT_CSV)
    parser.add_argument("--summary-csv", type=str, default=DEFAULT_OUT_SUMMARY_CSV)
    parser.add_argument("--out-plot", type=str, default=DEFAULT_OUT_PLOT)
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
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be > 0")


def generate_row_counts(start_rows: int, max_rows: int, multiplier: int) -> List[int]:
    rows = start_rows
    values: List[int] = []
    while rows <= max_rows:
        values.append(rows)
        rows *= multiplier
    return values


def start_release_server(server_bin: Path, server_log: Path | None) -> subprocess.Popen:
    stdout_target = None
    if server_log is not None:
        server_log.parent.mkdir(parents=True, exist_ok=True)
        stdout_target = open(server_log, "a", encoding="utf-8")
    proc = subprocess.Popen(
        [str(server_bin)],
        cwd=server_bin.parent.parent.parent,
        stdout=stdout_target or subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        text=True,
    )
    if stdout_target is not None:
        proc._log_fh = stdout_target
    return proc


def write_detail_csv(path: Path, results: List[RepeatResult]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["timestamp_utc", "rows", "repeat", "server_ingest_ms", "server_query_ms"])
        for result in results:
            writer.writerow(
                [
                    datetime.now(timezone.utc).isoformat(),
                    result.rows,
                    result.repeat,
                    f"{result.server_ingest_ms:.4f}",
                    f"{result.server_query_ms:.4f}",
                ]
            )


def build_summary(results: List[RepeatResult]) -> List[SummaryResult]:
    grouped: Dict[int, List[RepeatResult]] = {}
    for result in results:
        grouped.setdefault(result.rows, []).append(result)

    summary_rows: List[SummaryResult] = []
    for rows in sorted(grouped):
        ingest = [item.server_ingest_ms for item in grouped[rows]]
        query = [item.server_query_ms for item in grouped[rows]]
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
    plt.plot(xs, ingest, marker="o", label="Sketch ingest median (ms)")
    plt.plot(xs, query, marker="o", label="Sketch query median (ms)")
    plt.xscale("log", base=2)
    plt.xlabel("Rows")
    plt.ylabel("Latency (ms)")
    plt.title("Sketch Server Ingest and Query Latency vs Row Count")
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
    server_bin = Path(args.server_bin)
    server_log = None if args.server_log == "-" else resolve_repo_path(args.server_log)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_csv.parent.mkdir(parents=True, exist_ok=True)
    out_plot.parent.mkdir(parents=True, exist_ok=True)

    if not server_bin.exists():
        raise FileNotFoundError(f"server binary not found: {server_bin}")

    repeat_results: List[RepeatResult] = []

    for rows in row_counts:
        print(f"\n=== Sweep rows={rows} ===", flush=True)
        for repeat in range(1, args.repeats + 1):
            print(f"  repeat {repeat}/{args.repeats}", flush=True)
            proc = None
            try:
                if not args.skip_server_start:
                    proc = start_release_server(server_bin, server_log)
                wait_for_server(
                    args.server_url,
                    args.server_ready_timeout,
                    args.connect_timeout,
                    args.query_timeout,
                )

                rng = random.Random(args.seed + rows * 10_000 + repeat)
                total_batches = (rows + args.batch_size - 1) // args.batch_size
                log_every = max(1, total_batches // 10)
                server_ingest_ms = 0.0

                for batch_idx, batch in enumerate(
                    iter_batches(rows, nodes, rng, args.batch_size), start=1
                ):
                    t0 = time.perf_counter()
                    ingest_server(
                        args.server_url,
                        batch,
                        0,
                        args.connect_timeout,
                        args.ingest_timeout,
                        args.ingest_retries,
                        args.ingest_retry_backoff,
                    )
                    server_ingest_ms += (time.perf_counter() - t0) * 1000.0
                    if batch_idx % log_every == 0 or batch_idx == total_batches:
                        print(
                            f"    ingest progress: {batch_idx}/{total_batches} batches "
                            f"({batch_idx * 100 // total_batches}%)",
                            flush=True,
                        )

                _, server_query_ms = query_server_batch(
                    args.server_url,
                    nodes,
                    args.connect_timeout,
                    args.query_timeout,
                )
            except Exception:
                print(f"  failed at rows={rows} repeat={repeat}", flush=True)
                raise
            finally:
                if proc is not None:
                    stop_server(proc)

            repeat_results.append(
                RepeatResult(
                    rows=rows,
                    repeat=repeat,
                    server_ingest_ms=server_ingest_ms,
                    server_query_ms=server_query_ms,
                )
            )
            print(
                f"    sketch ingest: {server_ingest_ms:.2f} ms | "
                f"sketch query: {server_query_ms:.2f} ms",
                flush=True,
            )

    summary_rows = build_summary(repeat_results)
    write_detail_csv(out_csv, repeat_results)
    write_summary_csv(summary_csv, summary_rows)
    plot_results(summary_rows, out_plot)
    print(f"\nWrote {out_csv}", flush=True)
    print(f"Wrote {summary_csv}", flush=True)
    print(f"Wrote {out_plot}", flush=True)


if __name__ == "__main__":
    main()

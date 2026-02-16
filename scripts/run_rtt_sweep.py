#!/usr/bin/env python3
"""Run ingestion/query RTT sweep for server + Elasticsearch."""

from __future__ import annotations

import argparse
import csv
import random
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import List

from rtt_sweep_common import (
    DEFAULT_BATCH_SIZE,
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


DEFAULT_SERVER_LOG = "logs/server.log"


@dataclass
class SweepResult:
    rows: int
    server_rtt_ms: float
    es_rtt_ms: float
    max_pct_diff: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=10_000, help="Start row count")
    parser.add_argument("--end", type=int, default=1_000_000, help="End row count")
    parser.add_argument("--step", type=int, default=10_000, help="Row count step")
    add_common_args(parser)
    parser.set_defaults(server_log=DEFAULT_SERVER_LOG, out_csv="data/rtt_results.csv")
    parser.add_argument(
        "--out-plot",
        type=str,
        default="plots/query_rtt_plot.png",
        help="Output plot filename",
    )
    return parser.parse_args()


def plot_results(results: List[SweepResult], out_path: Path) -> None:
    import matplotlib.pyplot as plt

    xs = [r.rows for r in results]
    server = [r.server_rtt_ms for r in results]
    es = [r.es_rtt_ms for r in results]

    plt.figure(figsize=(10, 6))
    plt.plot(xs, server, label="Server RTT (ms)")
    plt.plot(xs, es, label="ES RTT (ms)")
    plt.xlabel("Rows")
    plt.ylabel("RTT (ms)")
    plt.title("Query RTT vs Data Size")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path)


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
        writer.writerow(["timestamp_utc", "rows", "server_rtt_ms", "es_rtt_ms"])
        csv_file.flush()

    for rows in range(args.start, args.end + 1, args.step):
        print(f"\n=== Sweep rows={rows} ===")
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
            total_batches = (rows + args.batch_size - 1) // args.batch_size
            log_every = max(1, total_batches // 10)
            for batch_idx, batch in enumerate(
                iter_batches(rows, nodes, rng, args.batch_size), start=1
            ):
                ingest_server(
                    args.server_url,
                    batch,
                    0,
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
                nodes,
                args.connect_timeout,
                args.query_timeout,
            )
            es_json, es_rtt = query_es_nodes(
                args.es_url,
                args.es_index,
                args.es_api_key,
                nodes,
                args.connect_timeout,
                args.es_timeout,
            )

            max_diff = compare_results(server_json, es_json)
            results.append(
                SweepResult(rows=rows, server_rtt_ms=server_rtt, es_rtt_ms=es_rtt, max_pct_diff=max_diff)
            )
            print(f"server RTT: {server_rtt:.2f} ms | ES RTT: {es_rtt:.2f} ms | max diff >=2%: {max_diff:.2f}%")
            print("comparisons:")
            for line in format_compact(server_json, es_json, nodes):
                print(line)
            writer.writerow(
                [
                    datetime.now(timezone.utc).isoformat(),
                    rows,
                    f"{server_rtt:.4f}",
                    f"{es_rtt:.4f}",
                ]
            )
            csv_file.flush()
        finally:
            stop_server(proc)

    csv_file.close()

    plot_results(results, out_plot)
    print(f"\nWrote {out_csv} and {out_plot}")


if __name__ == "__main__":
    main()

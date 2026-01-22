#!/usr/bin/env python3
"""Generate cluster metrics CSV with timestamps."""

from __future__ import annotations

import argparse
import csv
import os
import random
from datetime import datetime, timedelta, timezone


CLUSTERS = ["N001", "N002", "N003", "N004"]
TASKS = ["T0001", "T0002", "T0003", "T0004", "T0005", "T0006"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=2_000_000, help="Total rows to generate")
    parser.add_argument("--chunk-size", type=int, default=100_000, help="Rows per write batch")
    parser.add_argument("--start", type=str, default="2025-01-01T00:00:00Z", help="Start timestamp (ISO-8601)")
    parser.add_argument("--out", type=str, default="~/cluster-metrics.csv", help="Output CSV filename")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    return parser.parse_args()


def parse_start(value: str) -> datetime:
    value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    start_dt = parse_start(args.start)

    total = args.rows
    chunk = max(1, args.chunk_size)

    out_path = os.path.expanduser(args.out)
    with open(out_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["timestamp", "cluster", "task", "cpu_cores", "memory_gb", "network_mbps"])

        written = 0
        while written < total:
            n = min(chunk, total - written)
            for i in range(n):
                ts = (start_dt + timedelta(seconds=written + i)).strftime("%Y-%m-%dT%H:%M:%SZ")
                cluster = rng.choice(CLUSTERS)
                task = rng.choice(TASKS)
                cpu = f"{rng.uniform(0.05, 16.0):.3f}"
                mem = f"{rng.uniform(0.1, 128.0):.3f}"
                net = f"{rng.uniform(0.5, 40000.0):.2f}"
                writer.writerow([ts, cluster, task, cpu, mem, net])
            written += n


if __name__ == "__main__":
    main()

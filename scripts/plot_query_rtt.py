#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt

REPO_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=int, default=30, help="Number of nodes queried")
    parser.add_argument(
        "--rows-per-second",
        type=int,
        default=100,
        help="Rows per second in generated data",
    )
    parser.add_argument(
        "--in-csv",
        type=str,
        default="solver_experimental/query_rtt.csv",
        help="Input CSV with query RTT logs",
    )
    parser.add_argument(
        "--out-plot",
        type=str,
        default="plots/query_rtt_plot.png",
        help="Output plot filename",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    path = Path(args.in_csv)
    if not path.is_absolute():
        path = REPO_ROOT / path
    server_ms = []
    es_ms = []
    x_server = []
    x_es = []

    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        idx = 0
        for row in reader:
            idx += 1
            target = row.get("target")
            try:
                dur = float(row.get("duration_ms", "nan"))
            except ValueError:
                continue
            if target == "server":
                x_server.append(idx)
                server_ms.append(dur)
            elif target == "es":
                x_es.append(idx)
                es_ms.append(dur)

    plt.figure(figsize=(10, 4))
    plt.plot(x_server, server_ms, label="server", linewidth=1)
    plt.plot(x_es, es_ms, label="es", linewidth=1)
    plt.xlabel("index")
    plt.ylabel("duration_ms")
    plt.title(
        f"Query RTT (row order) | nodes={args.nodes}, rows/sec={args.rows_per_second}"
    )
    plt.legend()
    plt.tight_layout()
    out_path = Path(args.out_plot)
    if not out_path.is_absolute():
        out_path = REPO_ROOT / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=int, default=30, help="Number of nodes queried")
    parser.add_argument(
        "--rows-per-second",
        type=int,
        default=100,
        help="Rows per second in generated data",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parent
    path = root / "solver_experimental" / "query_rtt.csv"
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
    out_path = Path(
        f"solver_experimental/query_rtt_plot_nodes{args.nodes}_rps{args.rows_per_second}.png"
    )
    plt.savefig(out_path, dpi=150)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()

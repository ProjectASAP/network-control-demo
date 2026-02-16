#!/usr/bin/env python3
"""Plot cumulative RTT over epochs from rtt_results_epoch.csv."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import List, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--in-csv",
        type=str,
        default="data/rtt_results_epoch.csv",
        help="Input CSV with epoch RTT data",
    )
    parser.add_argument(
        "--out-plot",
        type=str,
        default="plots/query_rtt_plot_epoch_cumulative.png",
        help="Output plot filename",
    )
    return parser.parse_args()


def load_rows(path: Path) -> Tuple[List[int], List[float], List[float]]:
    epochs: List[int] = []
    server_rtt: List[float] = []
    es_rtt: List[float] = []

    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if not row:
                continue
            epoch_str = row.get("epoch")
            server_str = row.get("server_rtt_ms")
            es_str = row.get("es_rtt_ms")
            if epoch_str is None or server_str is None or es_str is None:
                continue
            epochs.append(int(epoch_str))
            server_rtt.append(float(server_str))
            es_rtt.append(float(es_str))

    return epochs, server_rtt, es_rtt


def cumulative(values: List[float]) -> List[float]:
    out: List[float] = []
    total = 0.0
    for v in values:
        total += v
        out.append(total)
    return out


def plot_cumulative(epochs: List[int], server: List[float], es: List[float], out_path: Path) -> None:
    import matplotlib.pyplot as plt

    plt.figure(figsize=(10, 6))
    plt.plot(epochs, server, label="Server RTT cumulative (ms)")
    plt.plot(epochs, es, label="ES RTT cumulative (ms)")
    plt.xlabel("Epoch")
    plt.ylabel("Cumulative RTT (ms)")
    plt.title("Cumulative Query RTT vs Epoch")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path)


def main() -> None:
    args = parse_args()
    in_csv = Path(args.in_csv)
    out_plot = Path(args.out_plot)
    out_plot.parent.mkdir(parents=True, exist_ok=True)

    epochs, server_rtt, es_rtt = load_rows(in_csv)
    if not epochs:
        raise SystemExit(f"No data found in {in_csv}")

    server_cum = cumulative(server_rtt)
    es_cum = cumulative(es_rtt)

    plot_cumulative(epochs, server_cum, es_cum, out_plot)
    print(f"Wrote {out_plot}")


if __name__ == "__main__":
    main()

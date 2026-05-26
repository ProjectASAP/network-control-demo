#!/usr/bin/env python3
"""Per-epoch query latency bar charts with cross-run error bars.

Three plots:
  A) all three: Approximate / Elastic Search / Elastic Search (compression 1000)
  B) Approximate vs Elastic Search          (ES default compression; not labeled as such)
  C) Approximate vs Elastic Search (compression 1000)

Style matches scripts/plot_query_solver_only.py::plot_query_only — log y bars.
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[1]

TITLE_FS = 17
LABEL_FS = 15
TICK_FS = 13
LEGEND_FS = 13


def _bar_with_err(ax, x, mean, std, label, color, bar_w):
    ax.bar(
        x, mean, bar_w,
        yerr=std, label=label, color=color,
        capsize=3, edgecolor="black", linewidth=0.4,
        error_kw={"linewidth": 1, "ecolor": "black"},
    )


def plot_chart(by_epoch, epochs, n_runs, series, out_path: Path) -> None:
    """`series` is a list of (label, csv_key, color) tuples."""
    x = np.arange(len(epochs))
    n = len(series)
    bar_w = 0.78 / n if n > 2 else 0.35
    offsets = (np.arange(n) - (n - 1) / 2) * bar_w

    fig, ax = plt.subplots(figsize=(7.6 if n <= 2 else 8.4, 4.8))
    for (label, key, color), off in zip(series, offsets):
        mean = np.array([np.mean(by_epoch[e][key]) for e in epochs])
        std  = np.array([np.std(by_epoch[e][key], ddof=1) for e in epochs])
        _bar_with_err(ax, x + off, mean, std, label, color, bar_w)

    ax.set_xlabel("Epoch", fontsize=LABEL_FS, labelpad=8)
    ax.set_ylabel("Query Time (ms)", fontsize=LABEL_FS)
    ax.set_yscale("log")
    ax.grid(axis="y", alpha=0.3, which="major")
    ax.tick_params(axis="both", labelsize=TICK_FS)
    ax.set_title(
        f"Query Time Comparison\nApproximate VS Exact (mean ± std, n={n_runs} runs)",
        fontsize=TITLE_FS,
        pad=14,
    )
    ax.set_xticks(x)
    ax.set_xticklabels([str(e) for e in epochs])

    legend = ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.22),
        ncol=1,
        fontsize=LEGEND_FS,
        frameon=False,
    )

    plt.savefig(out_path, dpi=220, bbox_inches="tight", bbox_extra_artists=[legend])
    plt.close(fig)
    print(f"Saved: {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=str, default="data/multi_run_epoch_benchmark.csv")
    parser.add_argument("--out-all", type=str,
                        default="plots/multi_run_query_latency_all.png")
    parser.add_argument("--out-default", type=str,
                        default="plots/multi_run_query_latency_default.png")
    parser.add_argument("--out-large", type=str,
                        default="plots/multi_run_query_latency_large.png")
    args = parser.parse_args()

    csv_path = REPO_ROOT / args.csv

    by_epoch: dict[int, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    with open(csv_path) as f:
        for r in csv.DictReader(f):
            ep = int(r["epoch"])
            by_epoch[ep]["server"].append(float(r["server_query_ms"]))
            by_epoch[ep]["es_default"].append(float(r["es_default_query_ms"]))
            by_epoch[ep]["es_large"].append(float(r["es_large_query_ms"]))

    epochs = sorted(by_epoch.keys())
    n_runs = len(by_epoch[epochs[0]]["server"])

    out_all = REPO_ROOT / args.out_all
    out_default = REPO_ROOT / args.out_default
    out_large = REPO_ROOT / args.out_large
    out_all.parent.mkdir(parents=True, exist_ok=True)

    plot_chart(by_epoch, epochs, n_runs, [
        ("Approximate Query",                       "server",     "#2a9d8f"),
        ("Elastic Search Query",                    "es_default", "#f28e2b"),
        ("Elastic Search Query (compression 1000)", "es_large",   "#b07aa1"),
    ], out_all)

    plot_chart(by_epoch, epochs, n_runs, [
        ("Approximate Query",    "server",     "#2a9d8f"),
        ("Elastic Search Query", "es_default", "#f28e2b"),
    ], out_default)

    plot_chart(by_epoch, epochs, n_runs, [
        ("Approximate Query",                       "server",   "#2a9d8f"),
        ("Elastic Search Query (compression 1000)", "es_large", "#b07aa1"),
    ], out_large)


if __name__ == "__main__":
    main()

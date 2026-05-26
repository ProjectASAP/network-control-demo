#!/usr/bin/env python3
"""Two per-epoch query latency bar charts (sketch vs ES), with cross-run error bars.

Plot A: Approximate Query vs Elastic Search Query             (ES default compression)
Plot B: Approximate Query vs Elastic Search Query (compression 1000)

Style matches scripts/plot_query_solver_only.py::plot_query_only — log y, 2 bars per epoch.
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


def plot_pair(by_epoch, epochs, n_runs, es_key, es_label, out_path: Path) -> None:
    x = np.arange(len(epochs))
    bar_w = 0.35

    fig, ax = plt.subplots(figsize=(7.2, 4.6))

    s_mean = np.array([np.mean(by_epoch[e]["server"]) for e in epochs])
    s_std  = np.array([np.std(by_epoch[e]["server"], ddof=1) for e in epochs])
    e_mean = np.array([np.mean(by_epoch[e][es_key]) for e in epochs])
    e_std  = np.array([np.std(by_epoch[e][es_key], ddof=1) for e in epochs])

    ax.bar(x - bar_w / 2, s_mean, bar_w, yerr=s_std,
           label="Approximate Query", color="#2a9d8f",
           capsize=3, edgecolor="black", linewidth=0.4,
           error_kw={"linewidth": 1, "ecolor": "black"})
    ax.bar(x + bar_w / 2, e_mean, bar_w, yerr=e_std,
           label=es_label, color="#f28e2b",
           capsize=3, edgecolor="black", linewidth=0.4,
           error_kw={"linewidth": 1, "ecolor": "black"})

    ax.set_xlabel("Epoch", fontsize=LABEL_FS)
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

    ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.18),
        ncol=1,
        fontsize=LEGEND_FS,
        frameon=False,
    )

    plt.tight_layout(rect=[0, 0.12, 1, 0.97])
    plt.savefig(out_path, dpi=220)
    plt.close(fig)
    print(f"Saved: {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=str, default="data/multi_run_epoch_benchmark.csv")
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

    out_default = REPO_ROOT / args.out_default
    out_large = REPO_ROOT / args.out_large
    out_default.parent.mkdir(parents=True, exist_ok=True)

    plot_pair(by_epoch, epochs, n_runs, "es_default",
              "Elastic Search Query", out_default)
    plot_pair(by_epoch, epochs, n_runs, "es_large",
              "Elastic Search Query (compression 1000)", out_large)


if __name__ == "__main__":
    main()

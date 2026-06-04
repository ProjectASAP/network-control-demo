#!/usr/bin/env python3
"""Per-epoch query / (query + solver) ratio for ES default and ES large.

Reads data/multi_run_epoch_benchmark.csv. For each (run, epoch) computes the
ratio per-run, then plots mean ± std across runs.

Three plots:
  A) both ES default and ES large
  B) only ES default
  C) only ES large (compression 1000)
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
    """`series` is a list of (label, csv_key_prefix, color) tuples.

    Each series reads two columns: `<prefix>_query_ms` and `<prefix>_solver_ms`,
    and computes the per-run ratio query / (query + solver).
    """
    x = np.arange(len(epochs))
    n = len(series)
    bar_w = 0.78 / n if n > 2 else 0.35
    offsets = (np.arange(n) - (n - 1) / 2) * bar_w

    fig, ax = plt.subplots(figsize=(7.6 if n <= 2 else 8.4, 4.8))
    for (label, prefix, color), off in zip(series, offsets):
        ratios_by_epoch = []
        for e in epochs:
            q = np.array(by_epoch[e][f"{prefix}_query_ms"], dtype=float)
            s = np.array(by_epoch[e][f"{prefix}_solver_ms"], dtype=float)
            ratio = q / (q + s) * 100
            ratios_by_epoch.append(ratio)
        mean = np.array([r.mean() for r in ratios_by_epoch])
        std = np.array(
            [r.std(ddof=1) if len(r) > 1 else 0.0 for r in ratios_by_epoch]
        )
        _bar_with_err(ax, x + off, mean, std, label, color, bar_w)

    ax.set_xlabel("Epoch", fontsize=LABEL_FS, labelpad=8)
    ax.set_ylabel("Query / (Query + Solver) (%)", fontsize=LABEL_FS)
    ax.set_ylim(0, 100)
    ax.grid(axis="y", alpha=0.3, which="major")
    ax.tick_params(axis="both", labelsize=TICK_FS)
    ax.set_title(
        f"Query Share of Query+Solver Time\n(mean ± std, n={n_runs} runs)",
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
    parser.add_argument("--out-both", type=str,
                        default="plots/multi_run_query_ratio_both.png")
    parser.add_argument("--out-default", type=str,
                        default="plots/multi_run_query_ratio_default.png")
    parser.add_argument("--out-large", type=str,
                        default="plots/multi_run_query_ratio_large.png")
    args = parser.parse_args()

    csv_path = REPO_ROOT / args.csv

    by_epoch: dict[int, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    with open(csv_path) as f:
        for r in csv.DictReader(f):
            ep = int(r["epoch"])
            by_epoch[ep]["es_default_query_ms"].append(float(r["es_default_query_ms"]))
            by_epoch[ep]["es_default_solver_ms"].append(float(r["es_default_solver_ms"]))
            by_epoch[ep]["es_large_query_ms"].append(float(r["es_large_query_ms"]))
            by_epoch[ep]["es_large_solver_ms"].append(float(r["es_large_solver_ms"]))

    epochs = sorted(by_epoch.keys())
    n_runs = len(by_epoch[epochs[0]]["es_default_query_ms"])

    out_both = REPO_ROOT / args.out_both
    out_default = REPO_ROOT / args.out_default
    out_large = REPO_ROOT / args.out_large
    out_both.parent.mkdir(parents=True, exist_ok=True)

    plot_chart(by_epoch, epochs, n_runs, [
        ("Elastic Search (default)",                "es_default", "#f28e2b"),
        ("Elastic Search (compression 1000)",       "es_large",   "#b07aa1"),
    ], out_both)

    plot_chart(by_epoch, epochs, n_runs, [
        ("Elastic Search (default)", "es_default", "#f28e2b"),
    ], out_default)

    plot_chart(by_epoch, epochs, n_runs, [
        ("Elastic Search (compression 1000)", "es_large", "#b07aa1"),
    ], out_large)


if __name__ == "__main__":
    main()

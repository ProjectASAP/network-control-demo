#!/usr/bin/env python3
"""Per-shape query latency bar charts for the breakdown benchmark.

Reads data/multi_run_epoch_query_breakdown.csv. For each (run, epoch, backend,
shape), `mean_elapsed_ms` is the within-repeat mean from one ingest; across
n_runs runs we plot the cross-run mean and std (error bar).

Outputs three layouts into plots/multi_run_query_breakdown/:
  A) per-shape: 6 PNGs, each with sketch / ES default / ES large bars per epoch
  B) per-backend: 3 PNGs, each with 6 shape bars per epoch
  C) combined: 1 PNG with 2x3 subplots, one subplot per shape (3 backend bars per epoch)
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[1]

SHAPES = ["p50", "p90", "p100", "all_quantile", "sum", "all_in"]
SHAPE_LABEL = {
    "p50": "p50",
    "p90": "p90",
    "p100": "p100",
    "all_quantile": "p50+p90+p100",
    "sum": "sum",
    "all_in": "p50+p90+p100+sum",
}

BACKENDS = ["server", "es_default", "es_large"]
BACKEND_LABEL = {
    "server": "Approximate Query",
    "es_default": "Elastic Search Query",
    "es_large": "Elastic Search Query (compression 1000)",
}
BACKEND_COLOR = {
    "server": "#2a9d8f",
    "es_default": "#f28e2b",
    "es_large": "#b07aa1",
}
SHAPE_COLORS = ["#2a9d8f", "#f28e2b", "#b07aa1", "#4e79a7", "#e15759", "#59a14f"]

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


def _load(csv_path: Path):
    """Returns (means, stds, n_repeats, runs) keyed by (epoch, backend, shape).

    means[k]: mean across runs of mean_elapsed_ms
    stds[k]:  if multiple runs, std across runs of mean_elapsed_ms;
              if single run, the within-repeat std_elapsed_ms (query-level noise)
    n_repeats[k]: repeats per measurement (from repeat_count column)
    """
    rows_by_key: dict[tuple[int, str, str], list[tuple[float, float, int]]] = defaultdict(list)
    runs: set[int] = set()
    with open(csv_path) as fh:
        for row in csv.DictReader(fh):
            ep = int(row["epoch"])
            key = (ep, row["backend"], row["query_shape"])
            runs.add(int(row["run_id"]))
            rows_by_key[key].append((
                float(row["mean_elapsed_ms"]),
                float(row["std_elapsed_ms"]),
                int(row["repeat_count"]),
            ))

    means: dict = {}
    stds: dict = {}
    n_repeats: dict = {}
    for key, entries in rows_by_key.items():
        per_run_means = [e[0] for e in entries]
        means[key] = float(np.mean(per_run_means))
        if len(per_run_means) > 1:
            stds[key] = float(np.std(per_run_means, ddof=1))
        else:
            stds[key] = entries[0][1]
        n_repeats[key] = entries[0][2]
    return means, stds, n_repeats, sorted(runs)


def _epochs(keyed_dict) -> list[int]:
    return sorted({k[0] for k in keyed_dict})


def _err_label(n_runs: int, n_repeats: int) -> str:
    if n_runs > 1:
        return f"mean ± std across n={n_runs} runs"
    return f"mean ± std across n={n_repeats} repeats"


def plot_per_shape(means, stds, n_repeats_map, epochs, n_runs, out_dir: Path) -> None:
    """One PNG per shape; bars are the 3 backends per epoch."""
    x = np.arange(len(epochs))
    bar_w = 0.78 / 3
    offsets = (np.arange(3) - 1) * bar_w

    for shape in SHAPES:
        fig, ax = plt.subplots(figsize=(8.4, 4.8))
        any_repeats = n_repeats_map.get((epochs[0], BACKENDS[0], shape), 0)
        for backend, off in zip(BACKENDS, offsets):
            mean = np.array([means[(e, backend, shape)] for e in epochs])
            std = np.array([stds[(e, backend, shape)] for e in epochs])
            _bar_with_err(ax, x + off, mean, std,
                          BACKEND_LABEL[backend], BACKEND_COLOR[backend], bar_w)
        ax.set_xlabel("Epoch", fontsize=LABEL_FS, labelpad=8)
        ax.set_ylabel("Query Time (ms)", fontsize=LABEL_FS)
        ax.set_yscale("log")
        ax.grid(axis="y", alpha=0.3, which="major")
        ax.tick_params(axis="both", labelsize=TICK_FS)
        ax.set_title(
            f"Query Time Comparison — shape={SHAPE_LABEL[shape]}\n"
            f"Approximate VS Exact ({_err_label(n_runs, any_repeats)})",
            fontsize=TITLE_FS, pad=14,
        )
        ax.set_xticks(x)
        ax.set_xticklabels([str(e) for e in epochs])
        legend = ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.22),
                           ncol=1, fontsize=LEGEND_FS, frameon=False)
        out_path = out_dir / f"by_shape_{shape}.png"
        plt.savefig(out_path, dpi=220, bbox_inches="tight",
                    bbox_extra_artists=[legend])
        plt.close(fig)
        print(f"Saved: {out_path}")


def plot_per_backend(means, stds, n_repeats_map, epochs, n_runs, out_dir: Path) -> None:
    """One PNG per backend; bars are the 6 shapes per epoch."""
    x = np.arange(len(epochs))
    bar_w = 0.85 / len(SHAPES)
    offsets = (np.arange(len(SHAPES)) - (len(SHAPES) - 1) / 2) * bar_w

    for backend in BACKENDS:
        fig, ax = plt.subplots(figsize=(10.5, 5.0))
        any_repeats = n_repeats_map.get((epochs[0], backend, SHAPES[0]), 0)
        for shape, color, off in zip(SHAPES, SHAPE_COLORS, offsets):
            mean = np.array([means[(e, backend, shape)] for e in epochs])
            std = np.array([stds[(e, backend, shape)] for e in epochs])
            _bar_with_err(ax, x + off, mean, std,
                          SHAPE_LABEL[shape], color, bar_w)
        ax.set_xlabel("Epoch", fontsize=LABEL_FS, labelpad=8)
        ax.set_ylabel("Query Time (ms)", fontsize=LABEL_FS)
        ax.set_yscale("log")
        ax.grid(axis="y", alpha=0.3, which="major")
        ax.tick_params(axis="both", labelsize=TICK_FS)
        ax.set_title(
            f"Query Shape Breakdown — {BACKEND_LABEL[backend]}\n"
            f"({_err_label(n_runs, any_repeats)})",
            fontsize=TITLE_FS, pad=14,
        )
        ax.set_xticks(x)
        ax.set_xticklabels([str(e) for e in epochs])
        legend = ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.20),
                           ncol=3, fontsize=LEGEND_FS, frameon=False)
        out_path = out_dir / f"by_backend_{backend}.png"
        plt.savefig(out_path, dpi=220, bbox_inches="tight",
                    bbox_extra_artists=[legend])
        plt.close(fig)
        print(f"Saved: {out_path}")


def plot_backend_compare(means, stds, n_repeats_map, epochs, n_runs,
                          backends: list[str], out_path: Path) -> None:
    """Stack one by_backend-style subplot per backend in `backends`, shared X."""
    x = np.arange(len(epochs))
    bar_w = 0.85 / len(SHAPES)
    offsets = (np.arange(len(SHAPES)) - (len(SHAPES) - 1) / 2) * bar_w
    n = len(backends)
    fig, axes = plt.subplots(n, 1, figsize=(11.0, 3.6 * n + 0.6), sharex=True)
    if n == 1:
        axes = [axes]
    any_repeats = n_repeats_map.get((epochs[0], backends[0], SHAPES[0]), 0)

    for ax, backend in zip(axes, backends):
        for shape, color, off in zip(SHAPES, SHAPE_COLORS, offsets):
            mean = np.array([means[(e, backend, shape)] for e in epochs])
            std = np.array([stds[(e, backend, shape)] for e in epochs])
            _bar_with_err(ax, x + off, mean, std,
                          SHAPE_LABEL[shape], color, bar_w)
        ax.set_ylabel("Query Time (ms)", fontsize=LABEL_FS)
        ax.set_yscale("log")
        ax.grid(axis="y", alpha=0.3, which="major")
        ax.tick_params(axis="both", labelsize=TICK_FS - 1)
        ax.set_title(BACKEND_LABEL[backend], fontsize=LABEL_FS)

    axes[-1].set_xlabel("Epoch", fontsize=LABEL_FS, labelpad=8)
    axes[-1].set_xticks(x)
    axes[-1].set_xticklabels([str(e) for e in epochs])

    handles, labels = axes[0].get_legend_handles_labels()
    legend = fig.legend(handles, labels, loc="lower center",
                        bbox_to_anchor=(0.5, -0.02), ncol=len(SHAPES),
                        fontsize=LEGEND_FS, frameon=False)
    fig.suptitle(
        f"Query Shape Breakdown — Backend Comparison "
        f"({_err_label(n_runs, any_repeats)})",
        fontsize=TITLE_FS,
    )
    fig.tight_layout(rect=(0, 0.04, 1, 0.96))
    plt.savefig(out_path, dpi=200, bbox_inches="tight",
                bbox_extra_artists=[legend])
    plt.close(fig)
    print(f"Saved: {out_path}")


def plot_combined(means, stds, n_repeats_map, epochs, n_runs, out_dir: Path) -> None:
    """One PNG with 2x3 subplots, one subplot per shape."""
    x = np.arange(len(epochs))
    bar_w = 0.78 / 3
    offsets = (np.arange(3) - 1) * bar_w
    any_repeats = n_repeats_map.get((epochs[0], BACKENDS[0], SHAPES[0]), 0)

    fig, axes = plt.subplots(2, 3, figsize=(18, 9), sharex=True, sharey=True)
    for ax, shape in zip(axes.flat, SHAPES):
        for backend, off in zip(BACKENDS, offsets):
            mean = np.array([means[(e, backend, shape)] for e in epochs])
            std = np.array([stds[(e, backend, shape)] for e in epochs])
            _bar_with_err(ax, x + off, mean, std,
                          BACKEND_LABEL[backend], BACKEND_COLOR[backend], bar_w)
        ax.set_yscale("log")
        ax.grid(axis="y", alpha=0.3, which="major")
        ax.tick_params(axis="both", labelsize=TICK_FS - 1)
        ax.set_title(f"shape = {SHAPE_LABEL[shape]}", fontsize=LABEL_FS)
        ax.set_xticks(x)
        ax.set_xticklabels([str(e) for e in epochs])

    for ax in axes[-1, :]:
        ax.set_xlabel("Epoch", fontsize=LABEL_FS)
    for ax in axes[:, 0]:
        ax.set_ylabel("Query Time (ms)", fontsize=LABEL_FS)

    handles, labels = axes[0, 0].get_legend_handles_labels()
    legend = fig.legend(handles, labels, loc="lower center",
                        bbox_to_anchor=(0.5, -0.02), ncol=3,
                        fontsize=LEGEND_FS, frameon=False)
    fig.suptitle(
        f"Query Time Breakdown by Shape — Approximate VS Exact "
        f"({_err_label(n_runs, any_repeats)})",
        fontsize=TITLE_FS,
    )
    fig.tight_layout(rect=(0, 0.03, 1, 0.96))
    out_path = out_dir / "combined_2x3.png"
    plt.savefig(out_path, dpi=200, bbox_inches="tight",
                bbox_extra_artists=[legend])
    plt.close(fig)
    print(f"Saved: {out_path}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", default="data/multi_run_epoch_query_breakdown.csv")
    p.add_argument("--out-dir", default="plots/multi_run_query_breakdown")
    args = p.parse_args()

    csv_path = REPO_ROOT / args.csv
    out_dir = REPO_ROOT / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    means, stds, n_repeats_map, runs = _load(csv_path)
    epochs = _epochs(means)
    n_runs = len(runs)
    print(f"Loaded {csv_path}: epochs={epochs}, n_runs={n_runs}")

    plot_per_shape(means, stds, n_repeats_map, epochs, n_runs, out_dir)
    plot_per_backend(means, stds, n_repeats_map, epochs, n_runs, out_dir)
    plot_combined(means, stds, n_repeats_map, epochs, n_runs, out_dir)

    plot_backend_compare(means, stds, n_repeats_map, epochs, n_runs,
                         ["server", "es_default"],
                         out_dir / "compare_server_vs_es_default.png")
    plot_backend_compare(means, stds, n_repeats_map, epochs, n_runs,
                         ["server", "es_large"],
                         out_dir / "compare_server_vs_es_large.png")
    plot_backend_compare(means, stds, n_repeats_map, epochs, n_runs,
                         ["server", "es_default", "es_large"],
                         out_dir / "compare_server_vs_es_default_vs_es_large.png")


if __name__ == "__main__":
    main()

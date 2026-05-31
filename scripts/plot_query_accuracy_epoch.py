#!/usr/bin/env python3
"""Per-epoch query accuracy bar charts (mean +/- std across runs).

Reads the per-(run, epoch) query JSON files under
``data/multi_run_epoch_benchmark_queries/``. Each JSON has four parallel
result blocks keyed by node id:

* ``ground_truth`` -- true values from the telemetry generator
* ``server``       -- sketch server result
* ``es_default``   -- Elasticsearch HDR percentile (default compression)
* ``es_large``     -- Elasticsearch HDR percentile (compression=1000)

For each statistic we compute relative error vs ground truth:

    rel_err = |backend - gt| / max(|gt|, EPS)

Per-run step 1: collapse 30 nodes into a single number via either ``np.mean``
(Mean version) or ``np.median`` (Median version). Step 2 then takes
``mean +/- std`` across the 10 per-run values for the bar height + error bar.

The script emits 13 plots:

* 4x p50/p90 Mean        (grid + per-metric CPU/Mem/Net)
* 4x all-percentile Mean (grid + per-metric)
* 4x all-percentile Median (grid + per-metric)
* sum accuracy (unchanged, 3-panel)
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]

TITLE_FS = 30
LABEL_FS = 27
TICK_FS = 24
LEGEND_FS = 22

EPS = 1e-9

METRICS = ["cpu_cores", "memory_gb", "network_mbps"]
METRIC_LABELS = {
    "cpu_cores": "CPU (cores)",
    "memory_gb": "Memory (GB)",
    "network_mbps": "Network (Mbps)",
}
METRIC_SHORT = {
    "cpu_cores": "cpu",
    "memory_gb": "mem",
    "network_mbps": "net",
}
PERCENTILES = ["50", "90", "100"]
PERCENTILES_NO_P100 = ["50", "90"]
BACKENDS = [
    ("server",     "Approximate (Sketch)",                       "#2a9d8f"),
    ("es_default", "Elastic Search (default compression)",       "#f28e2b"),
    ("es_large",   "Elastic Search (compression 1000)",          "#b07aa1"),
]

FNAME_RE = re.compile(r"run(\d+)_epoch(\d+)\.json$")


# ---------------------------------------------------------------------------
# Loading + per-record error extraction
# ---------------------------------------------------------------------------

def _rel_err(backend_val: float, gt_val: float) -> float:
    return abs(backend_val - gt_val) / max(abs(gt_val), EPS)


def load_errors(queries_dir: Path):
    """Return a nested dict of relative errors.

    Schema (lists are over the 30 nodes within one (run, epoch) file):
        errors[run][epoch][backend]["pct"][metric][pct] -> list[float]
        errors[run][epoch][backend]["sum"][metric]      -> list[float]
    """
    errors: dict = defaultdict(lambda: defaultdict(
        lambda: {b[0]: {"pct": defaultdict(lambda: defaultdict(list)),
                        "sum": defaultdict(list)}
                 for b in BACKENDS}))

    files = sorted(queries_dir.glob("run*_epoch*.json"))
    if not files:
        raise FileNotFoundError(f"No query JSONs under {queries_dir}")

    for fp in files:
        m = FNAME_RE.search(fp.name)
        if not m:
            continue
        run = int(m.group(1))
        epoch = int(m.group(2))

        with open(fp) as f:
            d = json.load(f)

        gt = d["ground_truth"]
        for backend_key, _, _ in BACKENDS:
            block = d.get(backend_key)
            if block is None:
                continue
            for node_id, gt_node in gt.items():
                got_node = block.get(node_id)
                if got_node is None:
                    continue
                for metric in METRICS:
                    if metric not in gt_node or metric not in got_node:
                        continue
                    gt_m = gt_node[metric]
                    got_m = got_node[metric]
                    # Percentiles
                    gt_pcts = gt_m.get("percentiles", {})
                    got_pcts = got_m.get("percentiles", {})
                    for pct in PERCENTILES:
                        if pct in gt_pcts and pct in got_pcts:
                            err = _rel_err(got_pcts[pct], gt_pcts[pct])
                            errors[run][epoch][backend_key]["pct"][metric][pct].append(err)
                    # Sum
                    if "sum" in gt_m and "sum" in got_m:
                        errors[run][epoch][backend_key]["sum"][metric].append(
                            _rel_err(got_m["sum"], gt_m["sum"])
                        )
    return errors


# ---------------------------------------------------------------------------
# Aggregation helpers: collapse 30 nodes -> 1 number per (run, epoch, ...)
# ---------------------------------------------------------------------------

def _per_run_value(per_node_lists: list[list[float]]) -> float:
    """Mean over all node values (flatten then mean)."""
    flat = [v for lst in per_node_lists for v in lst]
    return float(np.mean(flat)) if flat else float("nan")


def runs_epochs(errors) -> tuple[list[int], list[int]]:
    runs = sorted(errors.keys())
    epochs = sorted({e for r in runs for e in errors[r].keys()})
    return runs, epochs


# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------

def _bar_with_err(ax, x, mean, std, label, color, bar_w):
    ax.bar(
        x, mean, bar_w,
        yerr=std, label=label, color=color,
        capsize=3, edgecolor="black", linewidth=0.4,
        error_kw={"linewidth": 1, "ecolor": "black"},
    )


def _grouped_bar(ax, epochs, series_data, ylabel, title, log=False):
    """series_data: list of (label, color, mean[], std[])."""
    x = np.arange(len(epochs))
    n = len(series_data)
    bar_w = 0.78 / n if n > 2 else 0.35
    offsets = (np.arange(n) - (n - 1) / 2) * bar_w

    for (label, color, mean, std), off in zip(series_data, offsets):
        _bar_with_err(ax, x + off, mean, std, label, color, bar_w)

    ax.set_xlabel("Epoch", fontsize=LABEL_FS, labelpad=6)
    ax.set_ylabel(ylabel, fontsize=LABEL_FS)
    if log:
        ax.set_yscale("log")
    ax.grid(axis="y", alpha=0.3, which="major")
    ax.tick_params(axis="both", labelsize=TICK_FS)
    ax.set_title(title, fontsize=TITLE_FS, pad=10)
    ax.set_xticks(x)
    ax.set_xticklabels([str(e) for e in epochs])


def _series_from(per_run_values: dict[str, dict[int, list[float]]],
                 epochs: list[int]) -> list[tuple]:
    """Convert {backend: {epoch: [per-run-values]}} -> series_data list."""
    out = []
    for backend_key, label, color in BACKENDS:
        means, stds = [], []
        for ep in epochs:
            vals = per_run_values[backend_key].get(ep, [])
            if vals:
                means.append(float(np.mean(vals)))
                stds.append(float(np.std(vals, ddof=1)) if len(vals) > 1 else 0.0)
            else:
                means.append(float("nan"))
                stds.append(0.0)
        out.append((label, color, np.array(means) * 100.0, np.array(stds) * 100.0))
    return out


def _shared_legend(fig, axes_with_handles):
    handles, labels = axes_with_handles.get_legend_handles_labels()
    if len(handles) == 3:
        handles = [handles[0], handles[2], handles[1]]
        labels = [labels[0], labels[2], labels[1]]
    fig.legend(
        handles, labels,
        loc="lower center",
        ncol=2,
        fontsize=LEGEND_FS,
        frameon=False,
        bbox_to_anchor=(0.5, -0.04),
    )


# ---------------------------------------------------------------------------
# Plot A: grid metric x percentile
# ---------------------------------------------------------------------------

def _build_per_run(errors, runs, epochs, metric, pct, agg):
    """Step 1: per (run, epoch, backend) collapse 30 nodes via `agg`."""
    per_run: dict[str, dict[int, list[float]]] = {
        b[0]: defaultdict(list) for b in BACKENDS
    }
    for run in runs:
        for ep in epochs:
            for backend_key, _, _ in BACKENDS:
                bucket = errors[run][ep][backend_key]["pct"][metric][pct]
                if bucket:
                    per_run[backend_key][ep].append(float(agg(bucket)))
    return per_run


def plot_grid(errors, runs, epochs, out_path: Path,
              percentiles=PERCENTILES, log=False,
              agg=np.mean, agg_label="Mean"):
    fig, axes = plt.subplots(len(METRICS), len(percentiles),
                             figsize=(6.6 * len(percentiles), 4.4 * len(METRICS)),
                             sharex=True, squeeze=False)

    for i, metric in enumerate(METRICS):
        for j, pct in enumerate(percentiles):
            ax = axes[i, j]
            per_run = _build_per_run(errors, runs, epochs, metric, pct, agg)
            series = _series_from(per_run, epochs)
            _grouped_bar(
                ax, epochs, series,
                ylabel="",
                title=f"{METRIC_LABELS[metric]} - p{pct}",
                log=log,
            )

    fig.supylabel(f"{agg_label} rel. error (%)", fontsize=LABEL_FS)
    _shared_legend(fig, axes[0, 0])
    fig.suptitle(
        f"{agg_label} Rel. Error vs Ground Truth\n"
        f"(per-run {agg_label.lower()} over 30 nodes; mean +- std across n={len(runs)} runs)",
        fontsize=TITLE_FS, y=1.01,
    )
    fig.tight_layout(rect=[0, 0.06, 1, 0.97])
    fig.savefig(out_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out_path}")


def plot_per_metric(errors, runs, epochs, out_dir: Path,
                    percentiles, filename_template: str, log=False,
                    agg=np.mean, agg_label="Mean"):
    """One PNG per metric, layout 1 x len(percentiles).

    `filename_template` is formatted with `short=<cpu|mem|net>`.
    """
    for metric in METRICS:
        fig, axes = plt.subplots(1, len(percentiles),
                                 figsize=(6.6 * len(percentiles), 5.2),
                                 squeeze=False)
        for j, pct in enumerate(percentiles):
            ax = axes[0, j]
            per_run = _build_per_run(errors, runs, epochs, metric, pct, agg)
            series = _series_from(per_run, epochs)
            _grouped_bar(
                ax, epochs, series,
                ylabel=f"{agg_label} rel. error (%)" if j == 0 else "",
                title=f"p{pct}",
                log=log,
            )

        _shared_legend(fig, axes[0, 0])
        fig.suptitle(
            f"{METRIC_LABELS[metric]} -- {agg_label} Rel. Error\n"
            f"(per-run {agg_label.lower()} over 30 nodes; mean +- std across n={len(runs)} runs)",
            fontsize=TITLE_FS - 1, y=1.02,
        )
        fig.tight_layout(rect=[0, 0.20, 1, 0.92])
        out_path = out_dir / filename_template.format(short=METRIC_SHORT[metric])
        fig.savefig(out_path, dpi=220, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved: {out_path}")


# ---------------------------------------------------------------------------
# Plot B: sum accuracy (3 subplots per metric)
# ---------------------------------------------------------------------------

def plot_sum(errors, runs, epochs, out_path: Path, log=True):
    fig, axes = plt.subplots(1, len(METRICS),
                             figsize=(6.6 * len(METRICS), 5.0),
                             sharey=True)

    for i, metric in enumerate(METRICS):
        ax = axes[i]
        per_run: dict[str, dict[int, list[float]]] = {b[0]: defaultdict(list) for b in BACKENDS}
        for run in runs:
            for ep in epochs:
                for backend_key, _, _ in BACKENDS:
                    bucket = errors[run][ep][backend_key]["sum"][metric]
                    if bucket:
                        per_run[backend_key][ep].append(float(np.mean(bucket)))
        series = _series_from(per_run, epochs)
        _grouped_bar(
            ax, epochs, series,
            ylabel="Rel. error (%)" if i == 0 else "",
            title=METRIC_LABELS[metric],
            log=log,
        )

    _shared_legend(fig, axes[0])
    fig.suptitle(
        f"Sum Accuracy vs Ground Truth\n(mean +- std, n={len(runs)} runs)",
        fontsize=TITLE_FS, y=1.02,
    )
    fig.tight_layout(rect=[0, 0.20, 1, 0.94])
    fig.savefig(out_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--queries-dir",
        type=str,
        default="data/multi_run_epoch_benchmark_queries",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default="plots/multi_run_accuracy",
    )
    parser.add_argument(
        "--log-y",
        action="store_true",
        help="Use log scale on the y-axis for percentile plots.",
    )
    args = parser.parse_args()

    queries_dir = REPO_ROOT / args.queries_dir
    out_dir = REPO_ROOT / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    errors = load_errors(queries_dir)
    runs, epochs = runs_epochs(errors)
    print(f"Loaded {len(runs)} runs x {len(epochs)} epochs from {queries_dir}")

    # p50/p90 only -- Mean version (4 files)
    plot_grid(errors, runs, epochs,
              out_dir / "accuracy_grid_p50p90_mean.png",
              percentiles=PERCENTILES_NO_P100,
              agg=np.mean, agg_label="Mean", log=args.log_y)
    plot_per_metric(errors, runs, epochs, out_dir,
                    percentiles=PERCENTILES_NO_P100,
                    filename_template="accuracy_{short}_p50p90_mean.png",
                    agg=np.mean, agg_label="Mean", log=args.log_y)

    # all percentiles -- Mean version (4 files)
    plot_grid(errors, runs, epochs,
              out_dir / "accuracy_grid_percentile_mean.png",
              percentiles=PERCENTILES,
              agg=np.mean, agg_label="Mean", log=args.log_y)
    plot_per_metric(errors, runs, epochs, out_dir,
                    percentiles=PERCENTILES,
                    filename_template="accuracy_{short}_percentile_mean.png",
                    agg=np.mean, agg_label="Mean", log=args.log_y)

    # all percentiles -- Median version (4 files)
    plot_grid(errors, runs, epochs,
              out_dir / "accuracy_grid_percentile_median.png",
              percentiles=PERCENTILES,
              agg=np.median, agg_label="Median", log=args.log_y)
    plot_per_metric(errors, runs, epochs, out_dir,
                    percentiles=PERCENTILES,
                    filename_template="accuracy_{short}_percentile_median.png",
                    agg=np.median, agg_label="Median", log=args.log_y)

    plot_sum(errors, runs, epochs,
             out_dir / "accuracy_sum_per_metric.png",
             log=True)


if __name__ == "__main__":
    main()

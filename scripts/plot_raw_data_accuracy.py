#!/usr/bin/env python3
"""Paper Fig. 6: mean relative quantile error vs ground truth.

Reads the CSV written by `run_raw_data_accuracy.py` and draws the paper's
layout: one panel per (metric, quantile), grouped bars per epoch, one bar per
estimator, error bars = sd across runs of the per-run mean over keys.

    python scripts/plot_raw_data_accuracy.py --csv data/raw_data_accuracy_kll.csv

`--extra-sketch-csv` adds the sketch arm of a *second* run as a fourth series,
which is how the KLL and DDSketch backends land in one figure against the same
two Elasticsearch baselines. The two runs share a seed, so their Elasticsearch
arms see identical values -- the script checks that and warns if they diverge.

Colours are the archive's: teal = approximate layer, orange = Elasticsearch,
mauve = the high-compression Elasticsearch arm, green = the second sketch.
"""

from __future__ import annotations

import argparse
import collections
import csv
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]

SKETCH_COLOR = "#2a9d8f"
ES_COLOR = "#f28e2b"
ES_HI_COLOR = "#b07aa1"
EXTRA_COLOR = "#59a14f"

METRIC_LABEL = {
    "cpu_cores": "CPU (cores)",
    "memory_gb": "Memory (GB)",
    "network_mbps": "Network (Mbps)",
}
METRIC_ORDER = ["cpu_cores", "memory_gb", "network_mbps"]

TITLE_FS = 15
LABEL_FS = 13
TICK_FS = 11
LEGEND_FS = 12
DPI = 200


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--csv", type=Path, default=REPO_ROOT / "data" / "kll" / "raw_data_accuracy.csv")
    p.add_argument("--sketch-label", default="Approximate (Sketch)")
    p.add_argument("--extra-sketch-csv", type=Path, default=None,
                   help="Second accuracy CSV; its sketch arm is added as a fourth series.")
    p.add_argument("--extra-sketch-label", default="Approximate (second sketch)")
    p.add_argument("--out", type=Path, default=REPO_ROOT / "plots" / "kll" / "raw_data" / "fig6_accuracy.png")
    p.add_argument("--summary-csv", type=Path,
                   default=REPO_ROOT / "data" / "kll" / "raw_data_accuracy_summary.csv")
    p.add_argument("--log-y", action="store_true",
                   help="Log y axis -- readable when the arms differ by 100x.")
    args = p.parse_args()
    for field in ("csv", "extra_sketch_csv", "out", "summary_csv"):
        val = getattr(args, field)
        if val is not None and not val.is_absolute():
            setattr(args, field, REPO_ROOT / val)
    return args


def load(path: Path) -> list[dict]:
    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        raise SystemExit(f"{path} has no data rows")
    for r in rows:
        r["run"] = int(r["run"])
        r["epoch"] = int(r["epoch"])
        r["quantile"] = float(r["quantile"])
        r["rel_err_pct"] = float(r["rel_err_pct"])
    return rows


def series_stats(rows: list[dict], backend: str, metric: str, q: float, epochs: list[int]):
    """Per epoch: mean over keys within a run, then mean +- sd across runs."""
    means, sds = [], []
    for epoch in epochs:
        per_run = collections.defaultdict(list)
        for r in rows:
            if (r["backend"] == backend and r["metric"] == metric
                    and r["quantile"] == q and r["epoch"] == epoch):
                per_run[r["run"]].append(r["rel_err_pct"])
        run_means = [statistics.fmean(v) for v in per_run.values()]
        means.append(statistics.fmean(run_means) if run_means else float("nan"))
        sds.append(statistics.stdev(run_means) if len(run_means) > 1 else 0.0)
    return np.asarray(means), np.asarray(sds)


def main() -> None:
    args = parse_args()
    rows = load(args.csv)
    extra = load(args.extra_sketch_csv) if args.extra_sketch_csv else None

    metrics = [m for m in METRIC_ORDER if any(r["metric"] == m for r in rows)]
    quantiles = sorted({r["quantile"] for r in rows})
    epochs = sorted({r["epoch"] for r in rows})
    n_runs = len({r["run"] for r in rows})

    series = [("sketch", args.sketch_label, SKETCH_COLOR, rows),
              ("es_default", "Elasticsearch (default compression)", ES_COLOR, rows),
              ("es_compression", "Elasticsearch (compression 1000)", ES_HI_COLOR, rows)]
    if extra is not None:
        series.insert(1, ("sketch", args.extra_sketch_label, EXTRA_COLOR, extra))
        # Same seed => same ingested values => the ES arms must agree. If they
        # do not, the two CSVs are not comparable and the figure would be a lie.
        for backend in ("es_default", "es_compression"):
            a, _ = series_stats(rows, backend, metrics[0], quantiles[0], epochs)
            b, _ = series_stats(extra, backend, metrics[0], quantiles[0], epochs)
            if not np.allclose(a, b, rtol=0.05, atol=1e-6, equal_nan=True):
                print(f"WARNING: {backend} differs between the two CSVs "
                      f"({a[0]:.4f}% vs {b[0]:.4f}% at epoch {epochs[0]}); "
                      f"they were not run over identical values.")

    fig, axes = plt.subplots(len(metrics), len(quantiles),
                             figsize=(5.6 * len(quantiles), 3.1 * len(metrics)),
                             squeeze=False)
    width = 0.8 / len(series)
    x = np.arange(len(epochs), dtype=float)

    for row_i, metric in enumerate(metrics):
        for col_i, q in enumerate(quantiles):
            ax = axes[row_i][col_i]
            for k, (backend, label, color, src) in enumerate(series):
                means, sds = series_stats(src, backend, metric, q, epochs)
                ax.bar(x + (k - (len(series) - 1) / 2) * width, means, width * 0.92,
                       yerr=sds, label=label if (row_i == 0 and col_i == 0) else None,
                       color=color, edgecolor="white", linewidth=0.6,
                       error_kw={"linewidth": 1.0, "ecolor": "#333333"}, capsize=2.5)
            ax.set_title(f"{METRIC_LABEL.get(metric, metric)} - p{q:.0f}", fontsize=TITLE_FS)
            ax.set_xticks(x)
            ax.set_xticklabels([str(e) for e in epochs], fontsize=TICK_FS)
            ax.tick_params(axis="y", labelsize=TICK_FS)
            ax.set_xlabel("Epoch", fontsize=LABEL_FS)
            if args.log_y:
                ax.set_yscale("log")
            ax.grid(axis="y", color="#dddddd", linewidth=0.6)
            ax.set_axisbelow(True)
            for side in ("top", "right"):
                ax.spines[side].set_visible(False)

    fig.supylabel("Mean rel. error (%)", fontsize=LABEL_FS + 1)
    fig.suptitle("Mean Rel. Error vs Ground Truth\n"
                 f"(per-run mean over {len({r['key'] for r in rows})} keys; "
                 f"mean +- sd across n={n_runs} runs)", fontsize=TITLE_FS + 2)
    # Lay the axes out first, then park the legend and the note in the margin
    # the rect reserved for them. `bbox_inches="tight"` is deliberately not used:
    # it re-crops the figure and drags the two into the panels.
    fig.tight_layout(rect=(0.035, 0.085, 1.0, 0.93))
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=min(3, len(series)),
               fontsize=LEGEND_FS, frameon=False, bbox_to_anchor=(0.5, 0.032))
    note = ("network is synthetic: raw_data has no per-node network metric; "
            "CPU and memory are drawn from the real trace")
    fig.text(0.5, 0.008, note, ha="center", fontsize=LEGEND_FS - 2, color="#666666")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out, dpi=DPI)
    plt.close(fig)
    print(f"wrote {args.out}")

    with open(args.summary_csv, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["series", "backend", "metric", "quantile", "mean_rel_err_pct",
                    "sd_across_runs_pct"])
        for backend, label, _color, src in series:
            for metric in metrics:
                for q in quantiles:
                    means, sds = series_stats(src, backend, metric, q, epochs)
                    w.writerow([label, backend, metric, f"{q:.0f}",
                                f"{np.nanmean(means):.6f}", f"{np.nanmean(sds):.6f}"])
    print(f"wrote {args.summary_csv}")

    print(f"\n{'series':<34}{'metric':<15}{'q':>4}{'mean err':>11}{'sd':>9}")
    for backend, label, _color, src in series:
        for metric in metrics:
            for q in quantiles:
                means, sds = series_stats(src, backend, metric, q, epochs)
                print(f"{label:<34}{METRIC_LABEL.get(metric, metric):<15}{q:>4.0f}"
                      f"{np.nanmean(means):>10.4f}%{np.nanmean(sds):>8.4f}%")


if __name__ == "__main__":
    main()

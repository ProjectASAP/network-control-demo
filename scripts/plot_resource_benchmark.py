#!/usr/bin/env python3
"""Plots for the resource (CPU + RSS) benchmark (epoch-loop experiment).

Reads data/resource_benchmark.csv and data/resource_ingestion.csv (produced by
run_resource_benchmark.py: N runs x M epochs at --rows-per-epoch) and emits a
clean, non-redundant figure set into plots/resource/:

  Comparison bars (query-comparison style: per-epoch grouped bars, cross-run
  std error bars, log-y), three variants each (all / default / large):
    query_cpu_bars_*       -- CPU per query: Sketch vs ES (the headline gap)
    ingestion_cpu_bars_*   -- ingestion CPU per epoch: Sketch vs ES
                              (ES ingestion is compression-independent, so its
                               default/large variants are identical by nature)

  Sketch-server-only absolute footprint -- "adding a sketch server costs little":
    sketch_query.png / sketch_ingestion.png -- per-context CPU+RSS stacked pair
    sketch_cpu.png    -- horizontal pair: ingestion CPU | query CPU (log y)
    sketch_memory.png -- horizontal pair: memory after ingestion | during query
    sketch_resource.png -- 1x2 combined: CPU (log) | memory, ingestion vs query
                           bars grouped per epoch

ES RSS is intentionally NOT plotted as a memory baseline: it is just ES's fixed
pre-allocated JVM heap (-Xms=-Xmx), so it reflects provisioning, not usage.

Style mirrors scripts/plot_multi_run_query.py (csv + numpy + matplotlib).
"""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]

BAR_TITLE_FS = 17
BAR_LABEL_FS = 15
BAR_TICK_FS = 13
BAR_LEGEND_FS = 12

# Colors mirror scripts/plot_multi_run_query.py for visual consistency.
SERVER_COLOR = "#2a9d8f"
ES_DEFAULT_COLOR = "#f28e2b"
ES_LARGE_COLOR = "#b07aa1"

# Series variants for the comparison bar charts.
QUERY_VARIANTS = {
    "all": [("server", "Approximate", SERVER_COLOR),
            ("es_default", "Elastic Search", ES_DEFAULT_COLOR),
            ("es_large", "Elastic Search (compression 1000)", ES_LARGE_COLOR)],
    "default": [("server", "Approximate", SERVER_COLOR),
                ("es_default", "Elastic Search", ES_DEFAULT_COLOR)],
    "large": [("server", "Approximate", SERVER_COLOR),
              ("es_large", "Elastic Search (compression 1000)", ES_LARGE_COLOR)],
}
# Ingestion is compression-independent (ES ingests one copy; t-digest applies
# only at query time), so its three variants share the single "es" series and
# differ only in the ES legend label.
INGEST_VARIANTS = {
    "all": [("server", "Approximate", SERVER_COLOR),
            ("es", "Elastic Search", ES_DEFAULT_COLOR)],
    "default": [("server", "Approximate", SERVER_COLOR),
                ("es", "Elastic Search", ES_DEFAULT_COLOR)],
    "large": [("server", "Approximate", SERVER_COLOR),
              ("es", "Elastic Search (compression 1000)", ES_LARGE_COLOR)],
}


def _resolve(path_str: str) -> Path:
    p = Path(path_str)
    return p if p.is_absolute() else REPO_ROOT / p


def _fnum(s: str) -> float:
    if s is None or s == "":
        return float("nan")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def _fmt_rows(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:g}M"
    if n >= 1_000:
        return f"{n / 1_000:g}k"
    return str(n)


def load_rows(path: Path, shape: str | None = None) -> List[dict]:
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    if shape is not None:
        rows = [r for r in rows if r.get("query_shape") == shape]
    return rows


def by_epoch_metric(rows: List[dict], col: str) -> Dict[int, Dict[str, List[float]]]:
    d: Dict[int, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        v = _fnum(r.get(col, ""))
        if not math.isnan(v):
            d[int(r["epoch"])][r["backend"]].append(v)
    return d


# ---------------------------------------------------------------------------
# Comparison bars (per-epoch grouped, query-comparison style)
# ---------------------------------------------------------------------------

def plot_epoch_bars(by_epoch: Dict[int, Dict[str, List[float]]],
                    series: List[Tuple[str, str, str]], ylabel: str,
                    title_prefix: str, out_path: Path, log_y: bool = True) -> None:
    epochs = sorted(by_epoch.keys())
    if not epochs:
        print(f"[plot] no data for {out_path.name}; skipping")
        return
    first_key = series[0][0]
    n_runs = len(by_epoch[epochs[0]].get(first_key, []))
    x = np.arange(len(epochs))
    n = len(series)
    bar_w = 0.78 / n if n > 2 else 0.35
    offsets = (np.arange(n) - (n - 1) / 2) * bar_w

    fig, ax = plt.subplots(figsize=(8.4 if n > 2 else 7.6, 4.8))
    for (key, label, color), off in zip(series, offsets):
        vals = [by_epoch[e].get(key, []) for e in epochs]
        mean = np.array([np.mean(v) if v else np.nan for v in vals])
        std = np.array([np.std(v, ddof=1) if len(v) > 1 else 0.0 for v in vals])
        ax.bar(x + off, mean, bar_w, yerr=std, label=label, color=color,
               capsize=3, edgecolor="black", linewidth=0.4,
               error_kw={"linewidth": 1, "ecolor": "black"})

    ax.set_xlabel("Epoch", fontsize=BAR_LABEL_FS, labelpad=8)
    ax.set_ylabel(ylabel, fontsize=BAR_LABEL_FS)
    if log_y:
        ax.set_yscale("log")
    ax.grid(axis="y", alpha=0.3, which="major")
    ax.tick_params(axis="both", labelsize=BAR_TICK_FS)
    ax.set_title(f"{title_prefix}\nApproximate VS Exact (mean ± std, n={n_runs} runs)",
                 fontsize=BAR_TITLE_FS, pad=14)
    ax.set_xticks(x)
    ax.set_xticklabels([str(e) for e in epochs])
    legend = ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.22),
                       ncol=min(3, n), fontsize=BAR_LEGEND_FS, frameon=False)
    plt.savefig(out_path, dpi=220, bbox_inches="tight", bbox_extra_artists=[legend])
    plt.close(fig)
    print(f"[plot] wrote {out_path}")


def emit_bar_family(by_epoch, variants: dict, ylabel: str, title_prefix: str,
                    out_dir: Path, stem: str, log_y: bool = True) -> None:
    for variant, series in variants.items():
        plot_epoch_bars(by_epoch, series, ylabel, title_prefix,
                        out_dir / f"{stem}_{variant}.png", log_y=log_y)


def _label_ms(m: float) -> str:
    """Readable bar label for a CPU-time value in milliseconds."""
    if m >= 1000:
        return f"{m:.0f} ms\n({m / 1000:.1f} s)"
    return f"{m:.1f} ms"


def plot_headline(rows: List[dict], col: str, series: List[Tuple[str, str, str]],
                  ylabel: str, title: str, out_path: Path) -> None:
    """Single aggregated comparison (mean over all run x epoch samples) with
    value labels on each bar, so the magnitudes (e.g. ES 54s) are readable."""
    by: Dict[str, List[float]] = defaultdict(list)
    for r in rows:
        v = _fnum(r.get(col, ""))
        if not math.isnan(v):
            by[r["backend"]].append(v)
    present = [(k, lbl, c) for k, lbl, c in series if by.get(k)]
    if not present:
        print(f"[plot] no data for {out_path.name}; skipping")
        return
    means = [float(np.mean(by[k])) for k, _, _ in present]
    stds = [float(np.std(by[k], ddof=1)) if len(by[k]) > 1 else 0.0 for k, _, _ in present]
    n = len(by[present[0][0]])

    fig, ax = plt.subplots(figsize=(7.2, 5.2))
    xs = np.arange(len(present))
    ax.bar(xs, means, 0.6, yerr=stds, color=[c for _, _, c in present], capsize=4,
           edgecolor="black", linewidth=0.5, error_kw={"linewidth": 1, "ecolor": "black"})
    ax.set_yscale("log")
    ax.set_ylim(top=max(m + s for m, s in zip(means, stds)) * 2.4)
    for x, m, s in zip(xs, means, stds):
        ax.text(x, (m + s) * 1.08, _label_ms(m), ha="center", va="bottom", fontsize=11)
    ax.set_xticks(xs)
    ax.set_xticklabels([lbl for _, lbl, _ in present], fontsize=BAR_TICK_FS, rotation=10, ha="right")
    ax.set_ylabel(ylabel, fontsize=BAR_LABEL_FS)
    ax.set_title(f"{title}\n(mean ± std over {n} run x epoch samples)", fontsize=BAR_TITLE_FS, pad=12)
    ax.grid(axis="y", alpha=0.3, which="major")
    ax.tick_params(axis="y", labelsize=BAR_TICK_FS)
    plt.savefig(out_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot] wrote {out_path}")


# ---------------------------------------------------------------------------
# Sketch-server-only absolute footprint (the meaningful resource story)
# ---------------------------------------------------------------------------

def _draw_sketch_metric(ax, rows: List[dict], col: str, ylabel: str, title: str,
                        font_scale: float = 1.0, log_y: bool = False) -> int:
    """Draw one sketch-server-only metric vs epoch on `ax`. Returns n_runs.

    Absolute value; x tick labels carry the cumulative ingested rows so the
    "stays tiny/bounded as data grows" point is explicit. Linear axis from 0
    by default, or log axis with `log_y=True`.
    """
    srv = [r for r in rows if r["backend"] == "server"]
    if not srv:
        ax.set_visible(False)
        return 0
    by: Dict[int, List[float]] = defaultdict(list)
    for r in srv:
        v = _fnum(r.get(col, ""))
        if not math.isnan(v):
            by[int(r["epoch"])].append(v)
    epochs = sorted(by.keys())
    means = [float(np.mean(by[e])) for e in epochs]
    stds = [float(np.std(by[e], ddof=1)) if len(by[e]) > 1 else 0.0 for e in epochs]
    n_runs = len(by[epochs[0]])

    ax.bar(epochs, means, 0.6, yerr=stds, color=SERVER_COLOR, capsize=3,
           edgecolor="black", linewidth=0.4, error_kw={"linewidth": 1, "ecolor": "black"})
    if log_y:
        ax.set_yscale("log")
        ymax = max(m + s for m, s in zip(means, stds)) * 3.0
        ymin = max(min(m for m in means if m > 0) * 0.5, 1e-3)
        ax.set_ylim(ymin, ymax)
        for e, m, s in zip(epochs, means, stds):
            ax.text(e, (m + s) * 1.10, f"{m:.1f}", ha="center", va="bottom",
                    fontsize=9 * font_scale)
    else:
        ymax = max(m + s for m, s in zip(means, stds)) * 1.18
        pad = ymax * 0.02
        for e, m, s in zip(epochs, means, stds):
            ax.text(e, m + s + pad, f"{m:.1f}", ha="center", va="bottom",
                    fontsize=9 * font_scale)
        ax.set_ylim(0, ymax)
    ax.set_ylabel(ylabel, fontsize=BAR_LABEL_FS * font_scale)
    ax.set_xlabel("Epoch", fontsize=BAR_LABEL_FS * font_scale, labelpad=6)
    ax.set_xticks(epochs)
    ax.set_xticklabels([str(e) for e in epochs], fontsize=BAR_TICK_FS * font_scale)
    ax.tick_params(axis="y", labelsize=BAR_TICK_FS * font_scale)
    ax.set_title(title, fontsize=(BAR_TITLE_FS - 2) * font_scale, pad=8)
    ax.grid(axis="y", alpha=0.3, which="major")
    return n_runs


def plot_sketch_pair(top: dict, bottom: dict, suptitle: str, out_path: Path) -> None:
    """Combine two sketch-server-only metrics (CPU + memory) stacked vertically.

    Each of `top`/`bottom` is a dict with keys rows, col, ylabel, title. Stacked
    (2 rows x 1 col) so each panel is wide -- reads well as a single-column paper
    figure.
    """
    fig, axes = plt.subplots(2, 1, figsize=(11, 9.2))
    n = 0
    for ax, spec in zip(axes, (top, bottom)):
        n = _draw_sketch_metric(ax, **spec) or n
    fig.suptitle(f"{suptitle}  (mean ± std, n={n} runs)",
                 fontsize=BAR_TITLE_FS, y=1.005)
    fig.tight_layout()
    plt.savefig(out_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot] wrote {out_path}")


INGEST_COLOR = "#264653"


def _draw_sketch_two_series(ax, ingest: dict, query: dict, ylabel: str,
                            title: str, font_scale: float = 1.0,
                            log_y: bool = False, show_legend: bool = True) -> int:
    """Grouped sketch-server-only bars: one bar for ingestion, one for query
    per epoch. Both inputs are dicts with keys `rows` and `col`. Returns n_runs.
    """
    def collect(rows: List[dict], col: str) -> Dict[int, List[float]]:
        d: Dict[int, List[float]] = defaultdict(list)
        for r in rows:
            if r["backend"] != "server":
                continue
            v = _fnum(r.get(col, ""))
            if not math.isnan(v):
                d[int(r["epoch"])].append(v)
        return d

    ing_by = collect(ingest["rows"], ingest["col"])
    qry_by = collect(query["rows"], query["col"])
    epochs = sorted(set(ing_by) | set(qry_by))
    if not epochs:
        ax.set_visible(False)
        return 0

    def series(by: Dict[int, List[float]]):
        means = np.array([float(np.mean(by[e])) if by.get(e) else np.nan for e in epochs])
        stds = np.array([float(np.std(by[e], ddof=1)) if len(by.get(e, [])) > 1 else 0.0
                         for e in epochs])
        return means, stds

    ing_mean, ing_std = series(ing_by)
    qry_mean, qry_std = series(qry_by)
    n_runs = max(len(ing_by[epochs[0]]) if ing_by.get(epochs[0]) else 0,
                 len(qry_by[epochs[0]]) if qry_by.get(epochs[0]) else 0)

    x = np.arange(len(epochs))
    bar_w = 0.32
    gap = 0.04  # small gap between the two bars within an epoch group
    off = bar_w / 2 + gap / 2
    ax.bar(x - off, ing_mean, bar_w, yerr=ing_std, label="Ingestion",
           color=INGEST_COLOR, capsize=3, edgecolor="black", linewidth=0.4,
           error_kw={"linewidth": 1, "ecolor": "black"})
    ax.bar(x + off, qry_mean, bar_w, yerr=qry_std, label="Query",
           color=SERVER_COLOR, capsize=3, edgecolor="black", linewidth=0.4,
           error_kw={"linewidth": 1, "ecolor": "black"})

    # Value labels are slanted (45°) rather than fully vertical so they read
    # more naturally; ha="left" anchors each label at its bar and lets the text
    # run up-and-to-the-right, keeping adjacent ingestion / query labels apart.
    label_fs = 9 * font_scale

    def label_above(xs, means, stds, mult, pad):
        for xi, m, s in zip(xs, means, stds):
            if not np.isnan(m):
                ax.text(xi, (m + s) * mult if log_y else m + s + pad,
                        f"{m:.1f}", ha="left", va="bottom", rotation=45,
                        rotation_mode="anchor", fontsize=label_fs)

    if log_y:
        ax.set_yscale("log")
        positives = [v for v in np.concatenate([ing_mean, qry_mean]) if v > 0]
        top = max((m + s) for m, s in zip(np.concatenate([ing_mean, qry_mean]),
                                            np.concatenate([ing_std, qry_std]))
                  if not np.isnan(m))
        ax.set_ylim(min(positives) * 0.4, top * 8.0)
        label_above(x - off, ing_mean, ing_std, 1.15, 0)
        label_above(x + off, qry_mean, qry_std, 1.15, 0)
    else:
        top = np.nanmax(np.concatenate([ing_mean + ing_std, qry_mean + qry_std]))
        ax.set_ylim(0, top * 1.30)
        pad = top * 0.02
        label_above(x - off, ing_mean, ing_std, 0, pad)
        label_above(x + off, qry_mean, qry_std, 0, pad)

    ax.set_ylabel(ylabel, fontsize=BAR_LABEL_FS * font_scale)
    ax.set_xlabel("Epoch", fontsize=BAR_LABEL_FS * font_scale, labelpad=6)
    ax.set_xticks(x)
    ax.set_xticklabels([str(e) for e in epochs], fontsize=BAR_TICK_FS * font_scale)
    ax.tick_params(axis="y", labelsize=BAR_TICK_FS * font_scale)
    ax.set_title(title, fontsize=(BAR_TITLE_FS - 2) * min(font_scale, 2.2), pad=12)
    ax.grid(axis="y", alpha=0.3, which="major")
    if show_legend:
        ax.legend(fontsize=BAR_LEGEND_FS * font_scale, frameon=False, loc="best")
    return n_runs


def plot_sketch_resource(ingest_cpu: dict, query_cpu: dict,
                         ingest_mem: dict, query_mem: dict,
                         out_path: Path) -> None:
    """Two-panel sketch-server-only figure: CPU (log y) on the left, memory on
    the right; each panel groups ingestion vs query bars per epoch.
    """
    font_scale = 2.0
    fig, axes = plt.subplots(2, 1, figsize=(16, 13))
    n_top = _draw_sketch_two_series(
        axes[0], ingest=ingest_cpu, query=query_cpu,
        ylabel="CPU time (all threads, ms)", title="CPU",
        font_scale=font_scale, log_y=True, show_legend=False)
    n_bot = _draw_sketch_two_series(
        axes[1], ingest=ingest_mem, query=query_mem,
        ylabel="Whole-process RSS (MB)", title="Memory",
        font_scale=font_scale, log_y=False, show_legend=False)
    # Single shared legend centered below both panels.
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, fontsize=BAR_LEGEND_FS * font_scale,
               frameon=False, loc="lower center", ncol=2,
               bbox_to_anchor=(0.5, -0.01))
    fig.tight_layout(rect=(0, 0.04, 1, 1.0))
    plt.savefig(out_path, dpi=220, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    print(f"[plot] wrote {out_path}")


def plot_sketch_horizontal_pair(left: dict, right: dict,
                                suptitle: str, out_path: Path,
                                log_y: bool = False) -> None:
    """Sketch-server-only 1x2 footprint: two panels side by side.

    Each panel spec is a dict with keys rows, col, ylabel, title.
    """
    font_scale = 2.0
    fig, axes = plt.subplots(1, 2, figsize=(26, 10))
    n = 0
    for ax, spec in ((axes[0], left), (axes[1], right)):
        n = _draw_sketch_metric(ax, font_scale=font_scale, log_y=log_y, **spec) or n
    fig.suptitle(f"{suptitle}  (mean ± std, n={n} runs)",
                 fontsize=BAR_TITLE_FS * font_scale)
    fig.tight_layout()
    fig.subplots_adjust(wspace=0.28)
    plt.savefig(out_path, dpi=220, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    print(f"[plot] wrote {out_path}")


def plot_sketch_quad(top_left: dict, top_right: dict,
                     bottom_left: dict, bottom_right: dict,
                     suptitle: str, out_path: Path) -> None:
    """Sketch-server-only 2x2 footprint: CPU on top row, memory on bottom row;
    ingestion in the left column, query in the right column.

    Each panel spec is a dict with keys rows, col, ylabel, title.
    """
    font_scale = 2.0
    fig, axes = plt.subplots(2, 2, figsize=(26, 17))
    panels = ((axes[0, 0], top_left), (axes[0, 1], top_right),
              (axes[1, 0], bottom_left), (axes[1, 1], bottom_right))
    n = 0
    for ax, spec in panels:
        n = _draw_sketch_metric(ax, font_scale=font_scale, **spec) or n
    fig.suptitle(f"{suptitle}  (mean ± std, n={n} runs)",
                 fontsize=BAR_TITLE_FS * font_scale, y=0.995)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.subplots_adjust(wspace=0.28, hspace=0.42)
    plt.savefig(out_path, dpi=220, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot] wrote {out_path}")


# ---------------------------------------------------------------------------
# Expected output set (used to prune stale files the script no longer produces)
# ---------------------------------------------------------------------------

EXPECTED_STEMS = (
    ["query_cpu_headline", "ingestion_cpu_headline"]
    + [f"query_cpu_bars_{v}" for v in ("all", "default", "large")]
    + [f"ingestion_cpu_bars_{v}" for v in ("all", "default", "large")]
    + ["sketch_query", "sketch_ingestion", "sketch_cpu", "sketch_memory",
       "sketch_resource"]
)


def prune_stale(out_dir: Path) -> None:
    expected = {f"{s}.png" for s in EXPECTED_STEMS}
    for png in out_dir.glob("*.png"):
        if png.name not in expected:
            png.unlink()
            print(f"[plot] removed stale {png.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot resource benchmark figures")
    parser.add_argument("--summary-csv", default="data/kll/resource_benchmark.csv")
    parser.add_argument("--ingestion-csv", default="data/kll/resource_ingestion.csv")
    parser.add_argument("--out-dir", default="plots/kll/resource")
    parser.add_argument("--shape", default="default", help="query_shape to plot")
    parser.add_argument("--no-prune", action="store_true",
                        help="Keep files the script no longer produces (default: prune)")
    args = parser.parse_args()

    summary_csv = _resolve(args.summary_csv)
    ingest_csv = _resolve(args.ingestion_csv)
    out_dir = _resolve(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = load_rows(summary_csv, args.shape)
    if not rows:
        raise SystemExit(f"No rows for shape={args.shape} in {summary_csv}")

    ing_rows: List[dict] = []
    if ingest_csv.exists():
        ing_rows = [r for r in load_rows(ingest_csv) if r.get("is_warmup", "0") in ("0", "")]

    # --- headline comparisons with readable value labels (sketch vs ES) ---
    plot_headline(rows, "cpu_per_query_ms",
                  [("server", "Approximate", SERVER_COLOR),
                   ("es_default", "Elastic Search", ES_DEFAULT_COLOR),
                   ("es_large", "Elastic Search (comp 1000)", ES_LARGE_COLOR)],
                  "CPU time per query (all threads, ms)", "CPU per Query: Sketch vs Elasticsearch",
                  out_dir / "query_cpu_headline.png")
    if ing_rows:
        plot_headline(ing_rows, "ingest_cpu_ms",
                      [("server", "Approximate", SERVER_COLOR),
                       ("es", "Elastic Search", ES_DEFAULT_COLOR)],
                      "Ingestion CPU time per epoch (all threads, ms)",
                      "Ingestion CPU: Sketch vs Elasticsearch",
                      out_dir / "ingestion_cpu_headline.png")

    # --- comparison CPU bars per epoch (sketch vs ES) ---
    emit_bar_family(by_epoch_metric(rows, "cpu_per_query_ms"), QUERY_VARIANTS,
                    "CPU time per query (all threads, ms)", "CPU per Query Comparison",
                    out_dir, "query_cpu_bars")
    if ing_rows:
        emit_bar_family(by_epoch_metric(ing_rows, "ingest_cpu_ms"), INGEST_VARIANTS,
                        "Ingestion CPU time per epoch (all threads, ms)", "Ingestion CPU Comparison",
                        out_dir, "ingestion_cpu_bars")

    # --- sketch-server-only absolute footprint (CPU + memory, query + ingestion) ---
    plot_sketch_pair(
        top={"rows": rows, "col": "cpu_per_query_ms",
             "ylabel": "CPU time per query (all threads, ms)", "title": "CPU during query"},
        bottom={"rows": rows, "col": "rss_mean_mb",
                "ylabel": "Whole-process RSS (MB)", "title": "Memory during query"},
        suptitle="Approximate Layer resource usage during query", out_path=out_dir / "sketch_query.png")
    if ing_rows:
        plot_sketch_pair(
            top={"rows": ing_rows, "col": "ingest_cpu_ms",
                 "ylabel": "Ingestion CPU time per epoch (all threads, ms)", "title": "CPU during ingestion"},
            bottom={"rows": rows, "col": "rss_idle_mb",
                    "ylabel": "Whole-process RSS (MB)", "title": "Memory after ingestion"},
            suptitle="Approximate Layer resource usage during ingestion", out_path=out_dir / "sketch_ingestion.png")

    # --- combined horizontal views: one figure for CPU (log scale), one for memory ---
    if ing_rows:
        plot_sketch_horizontal_pair(
            left={"rows": ing_rows, "col": "ingest_cpu_ms",
                  "ylabel": "Ingestion CPU time per epoch (all threads, ms)",
                  "title": "CPU during ingestion"},
            right={"rows": rows, "col": "cpu_per_query_ms",
                   "ylabel": "CPU time per query (all threads, ms)",
                   "title": "CPU during query"},
            suptitle="Approximate Layer CPU usage",
            out_path=out_dir / "sketch_cpu.png", log_y=True)
    plot_sketch_horizontal_pair(
        left={"rows": rows, "col": "rss_idle_mb",
              "ylabel": "Whole-process RSS (MB)",
              "title": "Memory after ingestion"},
        right={"rows": rows, "col": "rss_mean_mb",
               "ylabel": "Whole-process RSS (MB)",
               "title": "Memory during query"},
        suptitle="Approximate Layer memory usage",
        out_path=out_dir / "sketch_memory.png")

    # --- combined two-panel resource figure: CPU (log) | memory, each with
    #     ingestion vs query bars grouped per epoch ---
    if ing_rows:
        plot_sketch_resource(
            ingest_cpu={"rows": ing_rows, "col": "ingest_cpu_ms"},
            query_cpu={"rows": rows, "col": "cpu_per_query_ms"},
            ingest_mem={"rows": rows, "col": "rss_idle_mb"},
            query_mem={"rows": rows, "col": "rss_mean_mb"},
            out_path=out_dir / "sketch_resource.png")

    if not args.no_prune:
        prune_stale(out_dir)


if __name__ == "__main__":
    main()

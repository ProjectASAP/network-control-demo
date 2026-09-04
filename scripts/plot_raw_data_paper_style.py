#!/usr/bin/env python3
"""Paper-style figures for the raw_data experiments.

The completion figures (8/9/10) are drawn to match the paper's own figures as
published: a *single* panel per figure, plain default matplotlib (white
background, all four spines, no grid, default font), cumulative "Tasks
Completed" vs epoch, a boxed legend in the upper left, and each series' final
value printed in bold at the right end of its line in the series' colour.

The x axis is labelled ``t = Epochs Elapsed (300 s)``: the paper's own epochs
are 150 s, ours really are 300 s (``synthetic_cpu_var.csv`` spans exactly one
300 s epoch).

Inputs (already on disk; nothing is re-run):
  data/raw_data_assignment.csv   -- 1 run x 43 epochs, sketch vs Elasticsearch
  data/raw_data_completion.csv   -- 3 runs x 30 epochs x 9 scenarios

Outputs -> plots/raw_data/paper_style/
  fig4_query_latency.png    fig7_solver_runtime.png
  fig8_completion.png       fig9_sketch_vs_es.png       fig10_update_rules.png
"""

from __future__ import annotations

import argparse
import collections
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]

# --- house style for the latency figures (verbatim from the archived scripts)
TITLE_FS = 19
LABEL_FS = 17
TICK_FS = 15
LEGEND_FS = 14
DPI = 220

SERVER_COLOR = "#2a9d8f"   # "Approximate" everywhere in the archive
ES_COLOR = "#f28e2b"       # "Elastic Search"
PURPLE = "#b07aa1"
GREEN = "#59a14f"

BAR_KW = dict(capsize=3, edgecolor="black", linewidth=0.4,
              error_kw={"linewidth": 1, "ecolor": "black"})

# --- paper style for the completion figures --------------------------------
PAPER_FIGSIZE = (9.0, 6.5)
PAPER_DPI = 150
XLABEL = "t = Epochs Elapsed (300 s)"
YLABEL = "Tasks Completed"


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load(path: Path) -> list[dict]:
    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        raise SystemExit(f"{path} has no data rows")
    return rows


def by_epoch(rows: list[dict], column: str) -> tuple[list[int], np.ndarray, np.ndarray]:
    """mean +- std across runs, per epoch."""
    buckets: dict[int, list[float]] = collections.defaultdict(list)
    for r in rows:
        buckets[int(r["epoch"])].append(float(r[column]))
    epochs = sorted(buckets)
    mean = np.array([float(np.mean(buckets[e])) for e in epochs])
    std = np.array([float(np.std(buckets[e], ddof=1)) if len(buckets[e]) > 1 else 0.0
                    for e in epochs])
    return epochs, mean, std


def _bar_with_err(ax, x, mean, std, label, color, bar_w):
    ax.bar(x, mean, bar_w, yerr=std, label=label, color=color, **BAR_KW)


def _epoch_ticks(ax, epochs: list[int], x: np.ndarray, stride: int) -> None:
    ax.set_xticks(x[::stride])
    ax.set_xticklabels([str(e) for e in epochs[::stride]])


# ---------------------------------------------------------------------------
# Fig. 4 / Fig. 7 -- per-epoch latency bars (plot_multi_run_query.py style)
# ---------------------------------------------------------------------------

def plot_latency(rows: list[dict], series, ylabel: str, title_prefix: str,
                 subtitle: str, out_path: Path, time_limit_ms: float | None = None,
                 stride: int = 3) -> None:
    epochs = sorted({int(r["epoch"]) for r in rows})
    x = np.arange(len(epochs), dtype=float)
    n = len(series)
    bar_w = 0.78 / n if n > 2 else 0.35
    offsets = (np.arange(n) - (n - 1) / 2) * bar_w

    fig_w = max(7.6, 0.30 * len(epochs) + 2.0)
    fig, ax = plt.subplots(figsize=(fig_w, 4.0))

    means = {}
    for (label, key, color), off in zip(series, offsets):
        _, mean, std = by_epoch(rows, key)
        _bar_with_err(ax, x + off, mean, std, label, color, bar_w)
        means[label] = float(np.mean(mean))

    if time_limit_ms is not None:
        ax.axhline(time_limit_ms, color="black", linewidth=1.1, linestyle=(0, (5, 4)),
                   zorder=4)
        ax.text(x[-1], time_limit_ms * 1.10,
                f"MILP time limit {time_limit_ms / 1000:.0f} s",
                ha="right", va="bottom", fontsize=11)

    ax.set_xlabel("Epoch", fontsize=LABEL_FS, labelpad=8)
    ax.set_ylabel(ylabel, fontsize=LABEL_FS)
    ax.set_yscale("log")
    ax.grid(axis="y", alpha=0.3, which="major")
    ax.tick_params(axis="both", labelsize=TICK_FS)
    ax.set_title(f"{title_prefix}\nApproximate Layer vs Elasticsearch\n{subtitle}",
                 fontsize=TITLE_FS, pad=14)
    _epoch_ticks(ax, epochs, x, stride)
    ax.set_ylim(top=ax.get_ylim()[1] * (3.0 if time_limit_ms is not None else 1.6))

    legend = ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.22),
                       ncol=min(3, n), fontsize=LEGEND_FS, frameon=False)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=DPI, bbox_inches="tight", bbox_extra_artists=[legend])
    plt.close(fig)
    print(f"[plot] wrote {out_path}  (means: "
          + ", ".join(f"{k} {v:,.1f} ms" for k, v in means.items()) + ")")


# ---------------------------------------------------------------------------
# Fig. 8 / 9 / 10 -- cumulative completions, one panel, paper layout
# ---------------------------------------------------------------------------

def _cumulative(rows: list[dict], scenario: str):
    sub = [r for r in rows if r["scenario"] == scenario]
    if not sub:
        raise SystemExit(f"scenario {scenario!r} not present in the completion CSV")
    return by_epoch(sub, "completed_total")


def _spread_labels(values: list[float], min_gap: float) -> list[float]:
    """Label y positions: the series' final values, with the lowest anchored on
    its line and any label too close to the one below nudged up just enough to
    clear it (several of our scenarios land within 0.3% of one another)."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    ys = list(values)
    for k, i in enumerate(order):
        if k:
            ys[i] = max(ys[i], ys[order[k - 1]] + min_gap)
    return ys


def plot_completion(rows: list[dict], scenarios, title: str, out_path: Path,
                    errorbars: bool = False, fmt: str = "{:.0f}",
                    caption: str | None = None) -> None:
    """One panel: cumulative mean curves, boxed upper-left legend, bold
    coloured final values at the right end of each line -- the paper's layout."""
    fig, ax = plt.subplots(figsize=PAPER_FIGSIZE)

    finals, colors, labels = [], [], []
    for label, scenario, color in scenarios:
        epochs, mean, std = _cumulative(rows, scenario)
        if errorbars:
            ax.errorbar(epochs, mean, yerr=std, label=label, color=color,
                        capsize=3, linewidth=1.4, elinewidth=1.0)
        else:
            ax.plot(epochs, mean, label=label, color=color, linewidth=1.4)
        finals.append(float(mean[-1]))
        colors.append(color)
        labels.append(label)

    last_epoch = max(epochs)
    ax.set_xlabel(XLABEL)
    ax.set_ylabel(YLABEL)
    ax.set_title(title)
    ax.legend(loc="upper left")

    # Room at the right for the value labels, and enough headroom that the
    # topmost (possibly nudged) label still sits inside the axes.
    ax.set_xlim(right=last_epoch + 0.13 * last_epoch)
    y0, y1 = ax.get_ylim()
    ys = _spread_labels(finals, 0.032 * (y1 - y0))
    ax.set_ylim(y0, max(y1, max(ys) + 0.035 * (y1 - y0)))

    for value, y, color in zip(finals, ys, colors):
        ax.annotate(fmt.format(value), xy=(last_epoch + 0.015 * last_epoch, y),
                    ha="left", va="center", color=color, fontweight="bold")

    if caption:
        ax.text(0.5, -0.135, caption, transform=ax.transAxes, ha="center",
                va="top", fontsize=9)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=PAPER_DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"[plot] wrote {out_path}  (final: "
          + ", ".join(f"{l} {v:,.1f}" for l, v in zip(labels, finals)) + ")")


# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--assignment-csv", type=Path,
                   default=REPO_ROOT / "data" / "raw_data_assignment.csv")
    p.add_argument("--completion-csv", type=Path,
                   default=REPO_ROOT / "data" / "raw_data_completion.csv")
    p.add_argument("--out-dir", type=Path,
                   default=REPO_ROOT / "plots" / "raw_data" / "paper_style")
    p.add_argument("--solver-time-limit-ms", type=float, default=60_000.0)
    args = p.parse_args()

    assign = load(args.assignment_csv)
    comp = load(args.completion_csv)
    n_runs = len({r["run"] for r in comp})
    n_epochs = len({int(r["epoch"]) for r in assign})
    rows_per_epoch = int(assign[0]["rows"])

    # Fig. 4 -- query latency comparison.
    plot_latency(
        assign,
        [("Approximate Query", "sketch_query_ms", SERVER_COLOR),
         ("Elasticsearch Query", "es_query_ms", ES_COLOR)],
        ylabel="Query Time (ms)",
        title_prefix="Query Time Comparison",
        subtitle=(f"({n_epochs} epochs, {rows_per_epoch:,} rows/epoch; "
                  f"mean 5.5 ms vs 942 ms)"),
        out_path=args.out_dir / "fig4_query_latency.png",
    )

    # Fig. 7 -- solver runtime on approximate vs ES-derived telemetry.
    plot_latency(
        assign,
        [("Solver (Approximate Input)", "sketch_solver_ms", GREEN),
         ("Solver (Elastic Search Input)", "es_solver_ms", PURPLE)],
        ylabel="Solver Time (ms)",
        title_prefix="Solver Time Comparison",
        subtitle=f"({n_epochs} epochs; bars at the dashed line hit the MILP deadline)",
        out_path=args.out_dir / "fig7_solver_runtime.png",
        time_limit_ms=args.solver_time_limit_ms,
    )

    # Fig. 8 -- effect of real-time telemetry / reassignments.
    plot_completion(
        comp,
        [("static (no reassignments)", "static", "blue"),
         ("static (reassignments)", "reassign", "cyan"),
         ("dynamic (no reassignments)", "dynamic", "orange"),
         ("dynamic (reassignments)", "dynamic+reassign", "red")],
        title="Effect of Real-time Telemetry Information on Task Completion Throughput",
        out_path=args.out_dir / "fig8_completion.png",
    )

    # Fig. 9 -- approximate layer vs Elasticsearch as the telemetry source.
    plot_completion(
        comp,
        [("static", "reassign", "blue"),
         ("elastic (compression = 100)", "es", "teal"),
         ("approx", "dynamic+reassign", "orange")],
        title=("Tasks Completed Using Drop-in Layer vs Elasticsearch Quantiles "
               f"(mean ± std, n = {n_runs} runs)"),
        out_path=args.out_dir / "fig9_sketch_vs_es.png",
        errorbars=True,
        fmt="{:.1f}",
    )

    # Fig. 10 -- dynamic telemetry update rules.
    plot_completion(
        comp,
        [("no rule", "static", "blue"),
         ("p50", "dynamic+reassign", "cornflowerblue"),
         ("p50 + 1.2x alloc", "p50-1.2xalloc", "orange"),
         ("avg", "window-avg", "purple"),
         ("p90 (ours, not in paper)", "p90", "olive"),
         ("per-epoch avg (ours, oracle)", "avg", "grey")],
        title="Effect of Dynamic Telemetry Update Rule on Number of Tasks Completed",
        out_path=args.out_dir / "fig10_update_rules.png",
        caption=("The last two rules are ours, not the paper's; the paper's "
                 "avg(p50, p75) + 1.2x alloc rule is not implemented on our side."),
    )


if __name__ == "__main__":
    main()

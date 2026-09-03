#!/usr/bin/env python3
"""Plots for the raw_data sketch-vs-ES assignment experiment.

Reads data/raw_data_assignment.csv (see run_raw_data_assignment.py) and writes:

  plots/raw_data/query_solver.png  -- per-epoch query and solver latency
  plots/raw_data/assignment.png    -- per-epoch tasks assigned + cumulative completed

Solver bars that hit the MILP time limit are hatched: those solves returned a
feasible-but-not-proven-optimal solution, so their assignment counts are not
attributable to the telemetry source.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]

# dataviz reference palette, categorical slots 1 and 2 (validated as a pair:
# CVD dE 24.7, normal-vision dE 33.6, both >= 3:1 on the light surface).
SKETCH = "#2a78d6"
ES = "#eb6834"
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#d9d8d4"

BAR_W = 0.38


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--csv", type=Path, default=REPO_ROOT / "data" / "raw_data_assignment.csv")
    p.add_argument("--out-dir", type=Path, default=REPO_ROOT / "plots" / "raw_data")
    p.add_argument(
        "--solver-time-limit-ms", type=float, default=60_000.0,
        help="Solves within 1%% of this are marked as time-limited.",
    )
    return p.parse_args()


def style(ax: plt.Axes, ylabel: str, title: str) -> None:
    ax.set_facecolor(SURFACE)
    ax.set_title(title, color=INK, fontsize=11, pad=10, loc="left")
    ax.set_ylabel(ylabel, color=INK_2, fontsize=9)
    ax.set_xlabel("Epoch", color=INK_2, fontsize=9)
    ax.tick_params(colors=INK_2, labelsize=9, length=3)
    ax.grid(axis="y", color=GRID, linewidth=0.7, alpha=0.9)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
        ax.spines[side].set_linewidth(0.7)


def label_bars(ax: plt.Axes, xs, values, fmt: str, log: bool, dy: float = 2.0) -> None:
    for x, v in zip(xs, values):
        if v <= 0:
            continue
        ax.annotate(fmt.format(v), (x, v), ha="center", va="bottom",
                    fontsize=7.5, color=INK_2, xytext=(0, dy), textcoords="offset points")


def plot_query_solver(rows: list[dict], out: Path, limit_ms: float) -> None:
    epochs = [int(r["epoch"]) for r in rows]
    x = np.arange(len(epochs))
    sq = [float(r["sketch_query_ms"]) for r in rows]
    eq = [float(r["es_query_ms"]) for r in rows]
    ss = [float(r["sketch_solver_ms"]) for r in rows]
    es_ = [float(r["es_solver_ms"]) for r in rows]

    def capped(v: float) -> bool:
        return v >= limit_ms * 0.99

    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.4), facecolor=SURFACE)

    ax = axes[0]
    ax.bar(x - BAR_W / 2, sq, BAR_W, color=SKETCH, label="Sketch server", zorder=3)
    ax.bar(x + BAR_W / 2, eq, BAR_W, color=ES, label="Elasticsearch", zorder=3)
    ax.set_yscale("log")
    label_bars(ax, x - BAR_W / 2, sq, "{:.1f}", True, dy=11)
    label_bars(ax, x + BAR_W / 2, eq, "{:.0f}", True, dy=2)
    style(ax, "Query latency (ms, log)", "Telemetry query — 44 nodes, 996,800 rows/epoch")
    ax.set_xticks(x, epochs)
    ax.set_ylim(1, max(eq) * 3)

    ax = axes[1]
    b1 = ax.bar(x - BAR_W / 2, ss, BAR_W, color=SKETCH, label="Sketch server", zorder=3)
    b2 = ax.bar(x + BAR_W / 2, es_, BAR_W, color=ES, label="Elasticsearch", zorder=3)
    for bars, vals in ((b1, ss), (b2, es_)):
        for bar, v in zip(bars, vals):
            if capped(v):
                bar.set_hatch("////")
                bar.set_edgecolor(SURFACE)
                bar.set_linewidth(0.0)
    ax.set_yscale("log")
    label_bars(ax, x - BAR_W / 2, ss, "{:.0f}", True, dy=11)
    label_bars(ax, x + BAR_W / 2, es_, "{:.0f}", True, dy=2)
    ax.axhline(limit_ms, color=INK_2, linewidth=1.0, linestyle=(0, (4, 3)), zorder=4)
    ax.annotate(f"MILP time limit {limit_ms / 1000:.0f}s", (len(epochs) - 0.45, limit_ms),
                ha="right", va="bottom", fontsize=8, color=INK_2,
                xytext=(0, 4), textcoords="offset points")
    style(ax, "Solver runtime (ms, log)", "MILP solve — SCIP, same pending set")
    ax.set_xticks(x, epochs)
    ax.set_ylim(1, limit_ms * 30)

    fig.legend(
        handles=[
            Patch(facecolor=SKETCH, label="Sketch server"),
            Patch(facecolor=ES, label="Elasticsearch"),
            Patch(facecolor="none", edgecolor=INK_2, hatch="////",
                  label="solver hit the time limit — feasible, not proven optimal"),
        ],
        frameon=False, fontsize=9, labelcolor=INK_2,
        loc="lower center", ncol=3, bbox_to_anchor=(0.5, -0.01),
    )
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=170, facecolor=SURFACE)
    plt.close(fig)
    print(f"wrote {out}")


def plot_assignment(rows: list[dict], out: Path, limit_ms: float) -> None:
    epochs = [int(r["epoch"]) for r in rows]
    x = np.arange(len(epochs))
    sa = [int(r["sketch_assigned"]) for r in rows]
    ea = [int(r["es_assigned"]) for r in rows]
    sc = [int(r["sketch_completed"]) for r in rows]
    ec = [int(r["es_completed"]) for r in rows]
    capped = [
        float(r["sketch_solver_ms"]) >= limit_ms * 0.99 or float(r["es_solver_ms"]) >= limit_ms * 0.99
        for r in rows
    ]

    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.4), facecolor=SURFACE)

    ax = axes[0]
    b1 = ax.bar(x - BAR_W / 2, sa, BAR_W, color=SKETCH, label="Sketch server", zorder=3)
    b2 = ax.bar(x + BAR_W / 2, ea, BAR_W, color=ES, label="Elasticsearch", zorder=3)
    for bars in (b1, b2):
        for bar, cap in zip(bars, capped):
            if cap:
                bar.set_hatch("////")
                bar.set_edgecolor(SURFACE)
    label_bars(ax, x - BAR_W / 2, sa, "{:.0f}", False)
    label_bars(ax, x + BAR_W / 2, ea, "{:.0f}", False)
    style(ax, "Tasks assigned this epoch", "Per-epoch assignments — 300 tasks, 44 nodes")
    ax.set_xticks(x, epochs)
    ax.set_ylim(0, max(max(sa), max(ea)) * 1.18)
    ax.legend(
        handles=[
            Patch(facecolor=SKETCH, label="Sketch server"),
            Patch(facecolor=ES, label="Elasticsearch"),
            Patch(facecolor="none", edgecolor=INK_2, hatch="////", label="solver hit time limit"),
        ],
        frameon=False, fontsize=8.5, labelcolor=INK_2, loc="upper right",
    )

    ax = axes[1]
    ax.plot(x, sc, color=SKETCH, linewidth=2.0, marker="o", markersize=6,
            markeredgecolor=SURFACE, markeredgewidth=1.5, label="Sketch server", zorder=3)
    ax.plot(x, ec, color=ES, linewidth=2.0, marker="o", markersize=6,
            markeredgecolor=SURFACE, markeredgewidth=1.5, label="Elasticsearch", zorder=3)
    ax.axhline(300, color=INK_2, linewidth=1.0, linestyle=(0, (4, 3)), zorder=2)
    ax.annotate("all 300 tasks", (len(epochs) - 0.5, 300), ha="right", va="bottom",
                fontsize=8, color=INK_2, xytext=(0, 3), textcoords="offset points")
    for xi, s, e in zip(x, sc, ec):
        if s != e:
            ax.annotate(f"{s}", (xi, s), fontsize=7.5, color=SKETCH,
                        xytext=(0, 8), textcoords="offset points", ha="center")
            ax.annotate(f"{e}", (xi, e), fontsize=7.5, color=ES,
                        xytext=(0, -13), textcoords="offset points", ha="center")
    style(ax, "Tasks completed (cumulative)", "Completion progress")
    ax.set_xticks(x, epochs)
    ax.set_ylim(0, 340)
    ax.legend(frameon=False, fontsize=9, labelcolor=INK_2, loc="lower right")

    fig.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=170, facecolor=SURFACE)
    plt.close(fig)
    print(f"wrote {out}")


def main() -> None:
    args = parse_args()
    with open(args.csv, newline="") as fh:
        rows = list(csv.DictReader(fh))
    plot_query_solver(rows, args.out_dir / "query_solver.png", args.solver_time_limit_ms)
    plot_assignment(rows, args.out_dir / "assignment.png", args.solver_time_limit_ms)


if __name__ == "__main__":
    main()

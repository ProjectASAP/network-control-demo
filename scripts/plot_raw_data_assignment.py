#!/usr/bin/env python3
"""Plots for the raw_data sketch-vs-ES assignment experiment.

Reads data/raw_data_assignment.csv (see run_raw_data_assignment.py) and writes:

  plots/raw_data/query_solver.png  -- per-epoch query and solver latency
  plots/raw_data/assignment.png    -- arrivals vs assignments, queue, completions

With `--runs > 1` in the experiment, each line is the mean over runs and the
band around it is the run-to-run range (min..max). That band is the point of
repeating: a sketch-vs-ES gap smaller than it is noise, not a telemetry effect.

Epochs where a solve did not reach `OPTIMAL` are marked. Those stopped at the
MILP time limit with a feasible-but-unproven solution, so their assignment
counts are not attributable to the telemetry source.
"""

from __future__ import annotations

import argparse
import collections
import csv
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.patheffects as pe  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.ticker import MaxNLocator  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]

# dataviz reference palette, categorical slots 1 and 2 (validated as a pair:
# CVD dE 24.7, normal-vision dE 33.6, both >= 3:1 on the light surface).
# Arrivals are a reference quantity, not a third series, so they wear muted ink.
SKETCH = "#2a78d6"
ES = "#eb6834"
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#d9d8d4"

# The two backends produce near-identical assignment counts, so a plain pair of
# 2px lines would have one erase the other. Sketch is drawn wide and
# Elasticsearch narrow on top of it: where they coincide, both are visible.
BACKENDS = (("sketch", SKETCH, "Sketch server", 3.2), ("es", ES, "Elasticsearch", 1.6))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--csv", type=Path, default=REPO_ROOT / "data" / "raw_data_assignment.csv")
    p.add_argument("--out-dir", type=Path, default=REPO_ROOT / "plots" / "raw_data")
    p.add_argument("--summary-csv", type=Path,
                   default=REPO_ROOT / "data" / "raw_data_assignment_summary.csv")
    p.add_argument("--solver-time-limit-ms", type=float, default=60_000.0,
                   help="Drawn as the MILP deadline; must match the experiment.")
    p.add_argument("--tasks-jsonl", type=Path,
                   default=REPO_ROOT / "data" / "raw_topology" / "tasks.jsonl",
                   help="Only used to draw the 'all tasks' reference line.")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

class Series:
    """One quantity per epoch, aggregated over runs."""

    def __init__(self, rows: list[dict], column: str, cast=float):
        by_epoch: dict[int, list[float]] = collections.defaultdict(list)
        for r in rows:
            by_epoch[int(r["epoch"])].append(cast(r[column]))
        self.epochs = sorted(by_epoch)
        self.x = np.asarray(self.epochs, dtype=float)
        self.mean = np.asarray([statistics.fmean(by_epoch[e]) for e in self.epochs])
        self.lo = np.asarray([min(by_epoch[e]) for e in self.epochs])
        self.hi = np.asarray([max(by_epoch[e]) for e in self.epochs])
        self.n = max(len(v) for v in by_epoch.values())


def not_optimal_epochs(rows: list[dict], backend: str) -> list[int]:
    bad = {int(r["epoch"]) for r in rows if r.get(f"{backend}_solver_status", "OPTIMAL") != "OPTIMAL"}
    return sorted(bad)


# ---------------------------------------------------------------------------
# Drawing
# ---------------------------------------------------------------------------

def style(ax: plt.Axes, ylabel: str, title: str) -> None:
    ax.set_facecolor(SURFACE)
    ax.set_title(title, color=INK, fontsize=10.5, pad=10, loc="left")
    ax.set_ylabel(ylabel, color=INK_2, fontsize=9)
    ax.set_xlabel("Epoch", color=INK_2, fontsize=9)
    ax.tick_params(colors=INK_2, labelsize=9, length=3)
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.grid(axis="y", color=GRID, linewidth=0.7, alpha=0.9)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
        ax.spines[side].set_linewidth(0.7)


def band(ax: plt.Axes, s: Series, color: str, label: str, width: float = 2.0) -> None:
    if s.n > 1:
        ax.fill_between(s.x, s.lo, s.hi, color=color, alpha=0.16, linewidth=0, zorder=2)
    ax.plot(s.x, s.mean, color=color, linewidth=width, label=label, zorder=3,
            solid_capstyle="round")


def end_label(ax: plt.Axes, s: Series, color: str, text: str, dy: float = 4.0) -> None:
    ax.annotate(text, (s.x[-1], s.mean[-1]), color=color, fontsize=8.5,
                xytext=(6, dy), textcoords="offset points", ha="left", va="center")


def mark_not_optimal(ax: plt.Axes, s: Series, epochs: list[int], color: str) -> None:
    if not epochs:
        return
    keep = [(e, v) for e, v in zip(s.epochs, s.mean) if e in set(epochs)]
    if not keep:
        return
    ax.plot([e for e, _ in keep], [v for _, v in keep], linestyle="none",
            marker="x", markersize=7, markeredgewidth=1.6, color=color, zorder=5)


def legend(ax: plt.Axes, loc: str, extra: list[Line2D] | None = None) -> None:
    handles = [Line2D([], [], color=c, linewidth=min(w, 2.4), label=lbl)
               for _, c, lbl, w in BACKENDS]
    handles += extra or []
    ax.legend(handles=handles, frameon=False, fontsize=8.5, labelcolor=INK_2, loc=loc)


def plot_query_solver(rows: list[dict], out: Path, limit_ms: float, rows_per_epoch: int,
                      n_nodes: int, backend_name: str) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.3), facecolor=SURFACE)

    ax = axes[0]
    for key, color, label, width in BACKENDS:
        s = Series(rows, f"{key}_query_ms")
        band(ax, s, color, label, width)
        end_label(ax, s, color, f"{s.mean[-1]:,.0f} ms" if s.mean[-1] >= 10 else f"{s.mean[-1]:.1f} ms")
    ax.set_yscale("log")
    style(ax, "Query latency (ms, log)",
          f"Telemetry query — {n_nodes} nodes, {rows_per_epoch:,} rows/epoch")
    legend(ax, "center right")

    ax = axes[1]
    for key, color, label, width in BACKENDS:
        s = Series(rows, f"{key}_solver_ms")
        band(ax, s, color, label, width)
        mark_not_optimal(ax, s, not_optimal_epochs(rows, key), color)
    ax.axhline(limit_ms, color=INK_2, linewidth=1.0, linestyle=(0, (4, 3)), zorder=4)
    ax.annotate(f"MILP time limit {limit_ms / 1000:.0f}s",
                (ax.get_xlim()[1], limit_ms), ha="right", va="bottom", fontsize=8,
                color=INK_2, xytext=(-2, 4), textcoords="offset points")
    ax.set_yscale("log")
    style(ax, "Solver runtime (ms, log)", f"MILP solve — {backend_name}, same pending set")
    legend(ax, "lower right",
           [Line2D([], [], color=INK_2, linestyle="none", marker="x", markersize=7,
                   label="not proven optimal")])

    fig.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=170, facecolor=SURFACE)
    plt.close(fig)
    print(f"wrote {out}")


def plot_assignment(rows: list[dict], out: Path, total_tasks: int | None) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(15.5, 4.3), facecolor=SURFACE)

    ax = axes[0]
    arr = Series(rows, "arrivals") if "arrivals" in rows[0] else None
    if arr is not None:
        ax.plot(arr.x, arr.mean, color=INK_2, linewidth=1.4, linestyle=(0, (4, 3)),
                zorder=2, label="Arrivals")
        end_label(ax, arr, INK_2, "arrivals")
    for key, color, label, width in BACKENDS:
        s = Series(rows, f"{key}_assigned", int)
        band(ax, s, color, label, width)
        mark_not_optimal(ax, s, not_optimal_epochs(rows, key), color)
    style(ax, "Tasks assigned this epoch", "Placements per epoch")
    legend(ax, "upper right",
           [Line2D([], [], color=INK_2, linewidth=1.4, linestyle=(0, (4, 3)), label="Arrivals"),
            Line2D([], [], color=INK_2, linestyle="none", marker="x", markersize=7,
                   label="not proven optimal")])

    ax = axes[1]
    for key, color, label, width in BACKENDS:
        s = Series(rows, f"{key}_pending_before", int)
        band(ax, s, color, label, width)
        end_label(ax, s, color, f"{s.mean[-1]:.0f}")
    style(ax, "Tasks waiting at solve time", "Pending queue — is there a real backlog?")
    legend(ax, "upper left")

    # The two backends coincide, so the informative view is the gap itself, on a
    # scale where one task is visible. Both series are task counts, one axis.
    ax = axes[2]
    s_asg = Series(rows, "sketch_assigned", int)
    e_asg = Series(rows, "es_assigned", int)
    s_cmp = Series(rows, "sketch_completed", int)
    e_cmp = Series(rows, "es_completed", int)
    xs = s_asg.x
    d_asg = s_asg.mean - e_asg.mean
    d_cmp = s_cmp.mean - e_cmp.mean
    halo = [pe.Stroke(linewidth=4.0, foreground=SURFACE), pe.Normal()]
    ax.axhline(0, color=INK_2, linewidth=1.0, zorder=2)
    ax.plot(xs, d_asg, color=SKETCH, linewidth=2.0, zorder=3,
            solid_capstyle="round", path_effects=halo)
    ax.plot(xs, d_cmp, color=ES, linewidth=2.0, zorder=3, linestyle=(0, (5, 3)),
            solid_capstyle="round", path_effects=halo)
    ax.annotate(f"{d_asg[-1]:+.0f}", (xs[-1], d_asg[-1]), color=SKETCH, fontsize=8.5,
                xytext=(6, 0), textcoords="offset points", ha="left", va="center")
    ax.annotate(f"{d_cmp[-1]:+.0f}", (xs[-1], d_cmp[-1]), color=ES, fontsize=8.5,
                xytext=(6, 0), textcoords="offset points", ha="left", va="center")
    style(ax, "Sketch − Elasticsearch (tasks)", "The gap, on its own scale")
    ax.set_xlim(right=xs[-1] + max(1.0, len(xs) * 0.06))
    ax.legend(handles=[
        Line2D([], [], color=SKETCH, linewidth=2.0, label="assigned this epoch"),
        Line2D([], [], color=ES, linewidth=2.0, linestyle=(0, (5, 3)),
               label="completed (cumulative)"),
    ], frameon=False, fontsize=8.5, labelcolor=INK_2, loc="upper left")

    fig.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=170, facecolor=SURFACE)
    plt.close(fig)
    print(f"wrote {out}")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def summarise(rows: list[dict], out_csv: Path) -> None:
    """Per-run totals, then mean/sd across runs -- the n>1 comparison."""
    runs = sorted({int(r.get("run", 0)) for r in rows})
    per_run: dict[str, dict[int, dict[str, float]]] = {}
    for key, _, _, _ in BACKENDS:
        per_run[key] = {}
        for run in runs:
            rr = [r for r in rows if int(r.get("run", 0)) == run]
            per_run[key][run] = {
                "assigned": sum(int(r[f"{key}_assigned"]) for r in rr),
                "completed": max(int(r[f"{key}_completed"]) for r in rr),
                "final_pending": int(rr[-1][f"{key}_pending_before"]),
                "query_ms": statistics.fmean(float(r[f"{key}_query_ms"]) for r in rr),
                "solver_ms": statistics.fmean(float(r[f"{key}_solver_ms"]) for r in rr),
                "ingest_ms": statistics.fmean(float(r[f"{key}_ingest_ms"]) for r in rr),
                "not_optimal": sum(
                    1 for r in rr if r.get(f"{key}_solver_status", "OPTIMAL") != "OPTIMAL"
                ),
                "epochs": len(rr),
            }

    metrics = ["assigned", "completed", "final_pending", "query_ms", "solver_ms",
               "ingest_ms", "not_optimal", "epochs"]

    def sd(vals: list[float]) -> float:
        return statistics.stdev(vals) if len(vals) > 1 else 0.0

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["metric", "sketch_mean", "sketch_sd", "es_mean", "es_sd", "runs"])
        for m in metrics:
            sv = [per_run["sketch"][r][m] for r in runs]
            ev = [per_run["es"][r][m] for r in runs]
            w.writerow([m, f"{statistics.fmean(sv):.4f}", f"{sd(sv):.4f}",
                        f"{statistics.fmean(ev):.4f}", f"{sd(ev):.4f}", len(runs)])
    print(f"wrote {out_csv}")

    print(f"\n{len(runs)} run(s), {per_run['sketch'][runs[0]]['epochs']} epochs each")
    print(f"{'metric':<16}{'sketch':>22}{'es':>22}")
    for m in metrics:
        sv = [per_run["sketch"][r][m] for r in runs]
        ev = [per_run["es"][r][m] for r in runs]
        fmt = "{:>14.1f} ±{:<6.1f}"
        print(f"{m:<16}" + fmt.format(statistics.fmean(sv), sd(sv))
              + fmt.format(statistics.fmean(ev), sd(ev)))

    bad = sum(per_run[k][r]["not_optimal"] for k, _, _, _ in BACKENDS for r in runs)
    if bad:
        print(f"\n!! {bad} solve(s) did not reach OPTIMAL — those epochs' assignment "
              f"counts are time-limit artefacts, not telemetry effects.")


def main() -> None:
    args = parse_args()
    with open(args.csv, newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        raise SystemExit(f"{args.csv} has no data rows")

    total_tasks = None
    if args.tasks_jsonl.exists():
        with open(args.tasks_jsonl) as fh:
            total_tasks = sum(1 for line in fh if line.strip())

    # The experiment writes one solver backend and one row count per file.
    backend_name = rows[0].get("solver_backend", "MILP")
    rows_per_epoch = int(rows[0]["rows"])
    n_nodes = 0
    nodes_csv = args.csv.with_name(args.csv.stem + "_nodes.csv")
    if nodes_csv.exists():
        with open(nodes_csv, newline="") as fh:
            n_nodes = len({r["node"] for r in csv.DictReader(fh)})

    plot_query_solver(rows, args.out_dir / "query_solver.png", args.solver_time_limit_ms,
                      rows_per_epoch, n_nodes, backend_name)
    plot_assignment(rows, args.out_dir / "assignment.png", total_tasks)
    summarise(rows, args.summary_csv)


if __name__ == "__main__":
    main()

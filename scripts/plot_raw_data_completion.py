#!/usr/bin/env python3
"""Plots for the completion experiment (paper Fig. 8/9/10) on the raw_data cluster.

Reads data/raw_data_completion.csv (see run_raw_data_completion.py) and writes
one figure per paper figure -- plots/raw_data/completion_fig{8,9,10}.png -- plus
data/raw_data_completion_summary.csv.

One figure per comparison rather than all scenarios at once, because a single
panel holding nine series would have to cycle the categorical palette, and a
cycled hue no longer identifies anything. Each figure keeps its own baseline:
Fig. 8 measures against a static controller with *no* reassignments (the paper's
reference), Fig. 9 and 10 against a static controller that has them, so the
telemetry effect is isolated from the reassignment effect.

Four panels, in the order the argument is made:

  completions over time      the headline -- cumulative tasks finished
  improvement over static    the same thing as the paper's percentages
  estimate error             *why* -- static charges the request, the backends
                             measure the usage
  contention                 the cost of the tighter packing that buys it

Scenario colours come from the dataviz reference palette's categorical order
(slots 1-5, the pre-validated order for line charts). The static baseline is
drawn as muted ink rather than a sixth hue: it is the reference the other lines
are measured against, not a competing series. Slots 3-5 sit below 3:1 contrast
on the light surface, so every line carries a direct end label (the relief
rule).
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
import matplotlib.patheffects as pe  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.ticker import MaxNLocator  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]

# dataviz reference palette, categorical slots 1-5 (light mode).
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#d9d8d4"


# scenario name in the CSV -> label in the figure. The run script dedups
# scenarios shared between figures, so Fig. 9/10's static-with-reassignments
# baseline is stored under the name Fig. 8 gives it ("reassign").
FIGURES: dict[str, dict] = {
    "8": {
        "title": "Dynamic telemetry and reassignments",
        "baseline": "static",
        "series": {"static": "static (no reassign)", "reassign": "reassignments only",
                   "dynamic": "dynamic telemetry", "dynamic+reassign": "dynamic + reassign"},
    },
    "9": {
        "title": "Approximate vs exact percentiles",
        "baseline": "reassign",
        "series": {"reassign": "static", "dynamic+reassign": "sketch layer (p50)",
                   "es": "Elasticsearch (p50)"},
    },
    "10": {
        "title": "Sensitivity to the telemetry update rule",
        "baseline": "reassign",
        "series": {"reassign": "static", "dynamic+reassign": "p50", "p90": "p90",
                   "avg": "avg (oracle)", "p50-1.2xalloc": "p50, 1.2x alloc",
                   "window-avg": "window avg"},
    },
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--csv", type=Path, default=REPO_ROOT / "data" / "raw_data_completion.csv")
    p.add_argument("--out-dir", type=Path, default=REPO_ROOT / "plots" / "raw_data")
    p.add_argument("--summary-csv", type=Path,
                   default=REPO_ROOT / "data" / "raw_data_completion_summary.csv")
    args = p.parse_args()
    # Relative paths are resolved against the repo root, not the CWD, so the
    # script behaves the same whether it is run from the repo root or from
    # solver_experimental/ (which is where the uv env lives).
    for field in ("csv", "out_dir", "summary_csv"):
        val = getattr(args, field)
        if not val.is_absolute():
            setattr(args, field, REPO_ROOT / val)
    return args


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

class Scen:
    """One scenario's per-epoch series, averaged over runs."""

    def __init__(self, name: str, rows: list[dict]):
        self.name = name
        self.rows = rows
        first = rows[0]
        self.estimator = first["estimator"]
        self.rule = first["rule"]
        self.gamma = int(first["gamma"])
        self.lam = float(first["lam"])
        self.runs = sorted({int(r["run"]) for r in rows})
        self.is_static = self.estimator == "static"

    def series(self, column: str, cast=float):
        by_epoch: dict[int, list[float]] = collections.defaultdict(list)
        for r in self.rows:
            v = r[column]
            if v == "":
                continue
            by_epoch[int(r["epoch"])].append(cast(v))
        epochs = sorted(by_epoch)
        return (np.asarray(epochs, dtype=float),
                np.asarray([statistics.fmean(by_epoch[e]) for e in epochs]),
                np.asarray([min(by_epoch[e]) for e in epochs]),
                np.asarray([max(by_epoch[e]) for e in epochs]))

    def final_completed(self) -> list[float]:
        """Total completions per run -- the quantity the paper's % is computed on."""
        out = []
        for run in self.runs:
            vals = [int(r["completed_total"]) for r in self.rows if int(r["run"]) == run]
            out.append(max(vals) if vals else 0)
        return out

    def mean_err(self) -> float | None:
        vals = [float(r["est_cpu_err_mean"]) for r in self.rows if r["est_cpu_err_mean"] != ""]
        return statistics.fmean(vals) if vals else None

    def label(self) -> str:
        bits = [self.name]
        if self.gamma:
            bits.append(f"γ={self.gamma}, λ={self.lam:g}")
        return "  ".join(bits)


def style(ax: plt.Axes, ylabel: str, title: str, xlabel: str = "Epoch") -> None:
    ax.set_facecolor(SURFACE)
    ax.set_title(title, color=INK, fontsize=10.5, pad=10, loc="left")
    ax.set_ylabel(ylabel, color=INK_2, fontsize=9)
    ax.set_xlabel(xlabel, color=INK_2, fontsize=9)
    ax.tick_params(colors=INK_2, labelsize=9, length=3)
    if xlabel == "Epoch":
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.grid(axis="y", color=GRID, linewidth=0.7, alpha=0.9)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
        ax.spines[side].set_linewidth(0.7)


def draw_line(ax: plt.Axes, sc: Scen, column: str, color: str, dashed: bool,
              label_fmt: str = "{:.0f}") -> None:
    x, mean, lo, hi = sc.series(column, int)
    if len(x) == 0:
        return
    if len(sc.runs) > 1:
        ax.fill_between(x, lo, hi, color=color, alpha=0.15, linewidth=0, zorder=2)
    ax.plot(x, mean, color=color, linewidth=2.0, zorder=3, solid_capstyle="round",
            linestyle=(0, (5, 3)) if dashed else "-",
            path_effects=[pe.Stroke(linewidth=4.0, foreground=SURFACE), pe.Normal()])
    ax.annotate(label_fmt.format(mean[-1]), (x[-1], mean[-1]), color=color, fontsize=8.5,
                xytext=(6, 0), textcoords="offset points", ha="left", va="center")


def draw_figure(scens: list[Scen], baseline: Scen, labels: dict[str, str],
                title: str, out: Path) -> None:
    """Four panels for one comparison: completions, gain, why, cost."""
    colors: dict[str, str] = {}
    i = 0
    for sc in scens:
        if sc is baseline:
            colors[sc.name] = INK_2
        else:
            colors[sc.name] = SERIES[i]
            i += 1
    base_total = statistics.fmean(baseline.final_completed())
    n_runs = max(len(sc.runs) for sc in scens)

    fig, axes = plt.subplots(2, 2, figsize=(13.5, 9.0), facecolor=SURFACE)

    # --- 1. cumulative completions -----------------------------------------
    ax = axes[0][0]
    for sc in scens:
        draw_line(ax, sc, "completed_total", colors[sc.name], sc is baseline)
    style(ax, "Tasks completed (cumulative)",
          f"{title} — {n_runs} run(s)" + (", band = run range" if n_runs > 1 else ""))
    ax.set_xlim(right=ax.get_xlim()[1] * 1.08)

    # --- 2. improvement over this figure's baseline -------------------------
    ax = axes[0][1]
    others = [sc for sc in scens if sc is not baseline]
    gains = [(statistics.fmean(sc.final_completed()) - base_total) / base_total * 100
             for sc in others]
    y = np.arange(len(others))
    ax.barh(y, gains, height=0.6, color=[colors[sc.name] for sc in others], zorder=3)
    ax.set_yticks(y, [labels[sc.name] for sc in others], fontsize=8.5)
    ax.invert_yaxis()
    ax.axvline(0, color=INK_2, linewidth=1.0, zorder=4)
    for yi, g in zip(y, gains):
        ax.annotate(f"{g:+.1f}%", (g, yi), color=INK_2, fontsize=8.5,
                    xytext=(4 if g >= 0 else -4, 0), textcoords="offset points",
                    ha="left" if g >= 0 else "right", va="center")
    pad = max(abs(min(gains)), abs(max(gains)), 1.0) * 0.3
    ax.set_xlim(min(0, min(gains)) - pad, max(0, max(gains)) + pad)
    style(ax, "", f"vs {labels[baseline.name]} ({base_total:.0f} tasks)",
          xlabel="Change in tasks completed (%)")
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRID, linewidth=0.7, alpha=0.9)

    # --- 3. why: estimate error --------------------------------------------
    ax = axes[1][0]
    errs = [(sc, sc.mean_err()) for sc in scens]
    errs = [(sc, e) for sc, e in errs if e is not None]
    if errs:
        x = np.arange(len(errs))
        ax.bar(x, [max(e, 1e-3) for _, e in errs], width=0.6,
               color=[colors[sc.name] for sc, _ in errs], zorder=3)
        ax.set_yscale("log")
        for xi, (_, e) in zip(x, errs):
            ax.annotate(f"{e:.2f}%" if e >= 0.005 else "0%", (xi, max(e, 1e-3)),
                        color=INK_2, fontsize=8.5, xytext=(0, 3),
                        textcoords="offset points", ha="center", va="bottom")
        ax.set_xticks(x, [labels[sc.name] for sc, _ in errs], fontsize=8,
                      rotation=20, ha="right")
        style(ax, "Mean relative CPU estimate error (%, log)",
              "Why: what the controller believes each running task uses", xlabel="")

    # --- 4. the cost: contention -------------------------------------------
    ax = axes[1][1]
    for sc in scens:
        x, mean, _, _ = sc.series("constrained_tasks", int)
        if len(x) == 0:
            continue
        cum = np.cumsum(mean)
        ax.plot(x, cum, color=colors[sc.name], linewidth=2.0, zorder=3,
                linestyle=(0, (5, 3)) if sc is baseline else "-", solid_capstyle="round",
                path_effects=[pe.Stroke(linewidth=4.0, foreground=SURFACE), pe.Normal()])
        ax.annotate(f"{cum[-1]:.0f}", (x[-1], cum[-1]), color=colors[sc.name], fontsize=8.5,
                    xytext=(6, 0), textcoords="offset points", ha="left", va="center")
    style(ax, "Constrained task-epochs (cumulative)",
          "The cost: tasks slowed by an over-committed node")
    ax.set_xlim(right=ax.get_xlim()[1] * 1.08)

    handles = [
        Line2D([], [], color=colors[sc.name], linewidth=2.0,
               linestyle=(0, (5, 3)) if sc is baseline else "-", label=labels[sc.name])
        for sc in scens
    ]
    fig.legend(handles=handles, frameon=False, fontsize=9, labelcolor=INK_2,
               loc="lower center", ncol=min(len(handles), 4), bbox_to_anchor=(0.5, -0.005))
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=170, facecolor=SURFACE)
    plt.close(fig)
    print(f"wrote {out}")


def write_summary(scens: list[Scen], baseline: Scen, out_csv: Path) -> None:
    base_total = statistics.fmean(baseline.final_completed())
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["scenario", "estimator", "rule", "gamma", "lam", "runs",
                    "completed_mean", "completed_sd", "vs_static_pct",
                    "cpu_est_err_pct", "constrained_task_epochs", "moves", "evicted",
                    "not_optimal_solves"])
        for sc in scens:
            totals = sc.final_completed()
            sd = statistics.stdev(totals) if len(totals) > 1 else 0.0
            gain = (statistics.fmean(totals) - base_total) / base_total * 100
            constrained = sum(int(r["constrained_tasks"]) for r in sc.rows) / len(sc.runs)
            moves = sum(int(r["moves_this_epoch"]) for r in sc.rows) / len(sc.runs)
            evicted = max(int(r["evicted_total"]) for r in sc.rows)
            bad = sum(1 for r in sc.rows if r["solver_status"] != "OPTIMAL")
            err = sc.mean_err()
            w.writerow([sc.name, sc.estimator, sc.rule, sc.gamma, sc.lam, len(sc.runs),
                        f"{statistics.fmean(totals):.2f}", f"{sd:.2f}", f"{gain:+.2f}",
                        f"{err:.4f}" if err is not None else "",
                        f"{constrained:.1f}", f"{moves:.1f}", evicted, bad])
    print(f"wrote {out_csv}")

    print(f"\n{'scenario':<18}{'completed':>12}{'±sd':>8}{'vs static':>11}{'est err':>10}"
          f"{'constrained':>13}{'moves':>8}{'non-opt':>9}")
    for sc in scens:
        totals = sc.final_completed()
        sd = statistics.stdev(totals) if len(totals) > 1 else 0.0
        gain = (statistics.fmean(totals) - base_total) / base_total * 100
        err = sc.mean_err()
        bad = sum(1 for r in sc.rows if r["solver_status"] != "OPTIMAL")
        print(f"{sc.name:<18}{statistics.fmean(totals):>12.1f}{sd:>8.1f}"
              f"{(gain if sc is not baseline else 0.0):>+10.1f}%"
              f"{(f'{err:.2f}%' if err is not None else '-'):>10}"
              f"{sum(int(r['constrained_tasks']) for r in sc.rows) / len(sc.runs):>13.0f}"
              f"{sum(int(r['moves_this_epoch']) for r in sc.rows) / len(sc.runs):>8.0f}"
              f"{bad:>9}")


def main() -> None:
    args = parse_args()
    with open(args.csv, newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        raise SystemExit(f"{args.csv} has no data rows")

    order: list[str] = []
    grouped: dict[str, list[dict]] = collections.defaultdict(list)
    for r in rows:
        if r["scenario"] not in grouped:
            order.append(r["scenario"])
        grouped[r["scenario"]].append(r)
    # `run_raw_data_completion.py --figure 9` names its arms static/sketch/es,
    # while `--figure all` dedups them into the names Fig. 8 gives them. Map the
    # split-run names onto the canonical ones so either CSV plots. In the split
    # run "static" already carries gamma/lambda, i.e. it is Fig. 8's `reassign`.
    if {"sketch", "es"} <= set(order) and "dynamic+reassign" not in order:
        alias = {"static": "reassign", "sketch": "dynamic+reassign"}
        order = [alias.get(n, n) for n in order]
        grouped = {alias.get(n, n): v for n, v in grouped.items()}
    # The oracle rule is written as `avg-epoch`; Fig. 10's spec calls it `avg`.
    if "avg-epoch" in grouped and "avg" not in grouped:
        order = ["avg" if n == "avg-epoch" else n for n in order]
        grouped["avg"] = grouped.pop("avg-epoch")

    scens = {n: Scen(n, grouped[n]) for n in order}

    drawn = 0
    for fig_id, spec in FIGURES.items():
        present = [n for n in spec["series"] if n in scens]
        # Every series a figure names must be present. A partial match would
        # silently overwrite that figure with a subset of its arms -- the Fig. 9
        # CSV contains two of Fig. 10's series, and vice versa.
        if len(present) < len(spec["series"]):
            continue
        # More than five non-baseline series would force a repeated hue, and a
        # repeated hue identifies nothing. Trim rather than cycle.
        picked = [n for n in present if n != spec["baseline"]][:len(SERIES)]
        ordered = [scens[spec["baseline"]]] + [scens[n] for n in picked]
        draw_figure(ordered, scens[spec["baseline"]], spec["series"],
                    spec["title"], args.out_dir / f"completion_fig{fig_id}.png")
        drawn += 1
    if not drawn:
        raise SystemExit("no known scenario set found in the CSV")

    all_scens = [scens[n] for n in order]
    statics = sorted((sc for sc in all_scens if sc.is_static), key=lambda sc: sc.gamma)
    write_summary(all_scens, statics[0] if statics else all_scens[0], args.summary_csv)


if __name__ == "__main__":
    main()

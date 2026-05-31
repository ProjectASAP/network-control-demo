#!/usr/bin/env python3
"""ES query / (query + solver) ratio across ingest row counts.

Reads:
  - data/es_ingest_query_sweep_summary.csv -> per-rows query and solver latency

Output: bar plot, x=rows (log spacing), y=query_p50 / (query_p50 + solver_p50).
Error bars use min/max query latencies combined with the per-row-count solver p50.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[1]

TITLE_FS = 17
LABEL_FS = 15
TICK_FS = 13
LEGEND_FS = 13


def _read_sweep_summary(path: Path) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.append({
                "rows": int(r["rows"]),
                "query_p50": float(r["median_query_ms"]),
                "query_min": float(r["min_query_ms"]),
                "query_max": float(r["max_query_ms"]),
                "solver_p50": float(r["median_solver_ms"]),
            })
    rows.sort(key=lambda x: x["rows"])
    return rows


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--summary-csv", type=str,
                   default="data/es_ingest_query_sweep_summary.csv")
    p.add_argument("--out", type=str,
                   default="plots/es_sweep_query_ratio.png")
    p.add_argument("--task-count", type=int, default=30,
                   help="Used only for plot annotation")
    args = p.parse_args()

    sweep = _read_sweep_summary(REPO_ROOT / args.summary_csv)

    rows = np.array([s["rows"] for s in sweep])
    q_p50 = np.array([s["query_p50"] for s in sweep])
    q_min = np.array([s["query_min"] for s in sweep])
    q_max = np.array([s["query_max"] for s in sweep])
    s_p50 = np.array([s["solver_p50"] for s in sweep])

    ratio_p50 = q_p50 / (q_p50 + s_p50) * 100
    ratio_min = q_min / (q_min + s_p50) * 100
    ratio_max = q_max / (q_max + s_p50) * 100
    err_lo = ratio_p50 - ratio_min
    err_hi = ratio_max - ratio_p50

    x = np.arange(len(rows))
    bar_w = 0.55

    fig, ax = plt.subplots(figsize=(8.2, 4.8))

    ax.bar(
        x, ratio_p50, bar_w,
        yerr=[err_lo, err_hi],
        color="#f28e2b",
        capsize=3,
        edgecolor="black",
        linewidth=0.4,
        error_kw={"linewidth": 1, "ecolor": "black"},
    )

    ax.set_xlabel("Rows Ingested", fontsize=LABEL_FS, labelpad=8)
    ax.set_ylabel("Query / (Query + Solver) (%)", fontsize=LABEL_FS)
    ax.set_ylim(0, 100)
    ax.grid(axis="y", alpha=0.3)
    ax.tick_params(axis="both", labelsize=TICK_FS)
    ax.set_title(
        "Query bottleneck in edge resource allocation",
        fontsize=TITLE_FS,
        pad=14,
    )
    ax.set_xticks(x)
    ax.set_xticklabels([f"{r:,}" for r in rows], rotation=30, ha="right")

    out = REPO_ROOT / args.out
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=220, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out}")
    print(f"solver_p50_ms per rows = {[f'{v:.1f}' for v in s_p50]}")
    print(f"ratios (p50, %): {[f'{r:.1f}' for r in ratio_p50]}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Bar charts showing query + solver time only (no ingestion) for OR-Tools backends."""

import csv
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


def load_csv(path: Path):
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows


def plot_query_solver(csv_path: Path, out_path: Path, backend: str) -> None:
    rows = load_csv(csv_path)

    epochs   = [int(r["epoch"]) for r in rows]
    s_query  = [float(r["server_query_ms"]) for r in rows]
    s_solver = [float(r["server_solver_ms"]) for r in rows]
    e_query  = [float(r["es_query_ms"]) for r in rows]
    e_solver = [float(r["es_solver_ms"]) for r in rows]

    x = np.arange(len(epochs))
    bar_w = 0.35

    fig, ax = plt.subplots(figsize=(max(10, len(epochs) * 1.5), 6))

    ax.bar(x - bar_w / 2, s_query,  bar_w, label="Server query",  color="#76b7b2")
    ax.bar(x - bar_w / 2, s_solver, bar_w, bottom=s_query,
           label="Server solver", color="#59a14f")

    ax.bar(x + bar_w / 2, e_query,  bar_w, label="ES query",  color="#f28e2b")
    ax.bar(x + bar_w / 2, e_solver, bar_w, bottom=e_query,
           label="ES solver", color="#b07aa1")

    ax.set_xticks(x)
    ax.set_xticklabels([str(e) for e in epochs])
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Time (ms)")
    ax.set_title(
        f"Per-epoch query + solver breakdown: Sketch vs ES (OR-Tools/{backend})\n"
        f"(ingestion time excluded)"
    )
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    data_dir = Path(__file__).resolve().parent.parent / "data"
    plot_dir = Path(__file__).resolve().parent.parent / "plots"
    plot_dir.mkdir(exist_ok=True)

    plot_query_solver(
        data_dir / "rtt_results_epoch_full_ortools_cbc_30nodes.csv",
        plot_dir / "rtt_epoch_query_solver_ortools_cbc_30nodes.png",
        "CBC",
    )
    plot_query_solver(
        data_dir / "rtt_results_epoch_full_ortools_scip_30nodes.csv",
        plot_dir / "rtt_epoch_query_solver_ortools_scip_30nodes.png",
        "SCIP",
    )

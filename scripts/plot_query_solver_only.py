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

    TITLE_FS = 17
    LABEL_FS = 15
    TICK_FS = 13
    LEGEND_FS = 13

    epochs   = [int(r["epoch"]) for r in rows]
    s_query  = [float(r["server_query_ms"]) for r in rows]
    s_solver = [float(r["server_solver_ms"]) for r in rows]
    e_query  = [float(r["es_query_ms"]) for r in rows]
    e_solver = [float(r["es_solver_ms"]) for r in rows]

    group_step = 0.90
    x = np.arange(len(epochs)) * group_step
    bar_w = 0.16

    fig_w = max(7.0, len(epochs) * 0.72)
    fig_h = 5.8
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    ax.bar(x - bar_w / 2, s_query, bar_w, label="Approximate Query", color="#2a9d8f")
    ax.bar(x - bar_w / 2, s_solver, bar_w, bottom=s_query,
           label="Solver (Approx Input)", color="#59a14f")

    ax.bar(x + bar_w / 2, e_query,  bar_w, label="Exact Query",  color="#f28e2b")
    ax.bar(x + bar_w / 2, e_solver, bar_w, bottom=e_query,
           label="Solver (Exact Input)", color="#b07aa1")

    ax.set_xticks(x)
    ax.set_xticklabels([str(e) for e in epochs])
    ax.set_xlabel("Epoch", fontsize=LABEL_FS)
    ax.set_ylabel("Time (ms)", fontsize=LABEL_FS)
    ax.set_title(
        "Query+Solver Time Comparison\n"
        "Approximate VS Exact",
        fontsize=TITLE_FS,
        pad=14,
    )
    ax.tick_params(axis="both", labelsize=TICK_FS)
    ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.20),
        ncol=2,
        fontsize=LEGEND_FS,
        frameon=False,
    )
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout(rect=[0, 0.08, 1, 0.92])
    plt.savefig(out_path, dpi=220)
    plt.close(fig)
    print(f"Saved: {out_path}")


def plot_query_solver_split(csv_path: Path, out_path: Path, backend: str) -> None:
    rows = load_csv(csv_path)

    TITLE_FS = 17
    LABEL_FS = 15
    TICK_FS = 13
    LEGEND_FS = 13

    epochs = [int(r["epoch"]) for r in rows]
    s_query = [float(r["server_query_ms"]) for r in rows]
    s_solver = [float(r["server_solver_ms"]) for r in rows]
    e_query = [float(r["es_query_ms"]) for r in rows]
    e_solver = [float(r["es_solver_ms"]) for r in rows]

    x = np.arange(len(epochs))
    bar_w = 0.35

    fig, (ax_q, ax_s) = plt.subplots(2, 1, figsize=(7.2, 6.6), sharex=True)

    ax_q.bar(x - bar_w / 2, s_query, bar_w, label="Approximate Query", color="#2a9d8f")
    ax_q.bar(x + bar_w / 2, e_query, bar_w, label="Exact Query", color="#f28e2b")
    ax_q.set_ylabel("Query Time (ms)", fontsize=LABEL_FS)
    ax_q.set_yscale("log")
    ax_q.grid(axis="y", alpha=0.3)
    ax_q.tick_params(axis="both", labelsize=TICK_FS)
    ax_q.set_title(
        "Query+Solver Time Comparison\n"
        "Approximate VS Exact",
        fontsize=TITLE_FS,
        pad=14,
    )

    ax_s.bar(x - bar_w / 2, s_solver, bar_w, label="Solver (Approx Input)", color="#59a14f")
    ax_s.bar(x + bar_w / 2, e_solver, bar_w, label="Solver (Exact Input)", color="#b07aa1")
    ax_s.set_xlabel("Epoch", fontsize=LABEL_FS)
    ax_s.set_ylabel("Solver Time (ms)", fontsize=LABEL_FS)
    ax_s.grid(axis="y", alpha=0.3)
    ax_s.tick_params(axis="both", labelsize=TICK_FS)

    ax_s.set_xticks(x)
    ax_s.set_xticklabels([str(e) for e in epochs])

    handles_q, labels_q = ax_q.get_legend_handles_labels()
    ax_q.legend(
        handles_q,
        labels_q,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.12),
        ncol=2,
        fontsize=LEGEND_FS,
        frameon=False,
    )
    handles_s, labels_s = ax_s.get_legend_handles_labels()
    fig.legend(
        handles_s,
        labels_s,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.02),
        ncol=2,
        fontsize=LEGEND_FS,
        frameon=False,
    )

    plt.tight_layout(rect=[0, 0.12, 1, 0.95])
    fig.subplots_adjust(hspace=0.45)
    plt.savefig(out_path, dpi=220)
    plt.close(fig)
    print(f"Saved: {out_path}")


def plot_query_solver_logy(csv_path: Path, out_path: Path, backend: str) -> None:
    rows = load_csv(csv_path)

    TITLE_FS = 17
    LABEL_FS = 15
    TICK_FS = 13
    LEGEND_FS = 13

    epochs = [int(r["epoch"]) for r in rows]
    s_query = [float(r["server_query_ms"]) for r in rows]
    s_solver = [float(r["server_solver_ms"]) for r in rows]
    e_query = [float(r["es_query_ms"]) for r in rows]
    e_solver = [float(r["es_solver_ms"]) for r in rows]

    group_step = 0.96
    x = np.arange(len(epochs)) * group_step
    bar_w = 0.13

    fig_w = max(8.2, len(epochs) * 0.82)
    fig_h = 5.0
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    ax.bar(x - 1.8 * bar_w, s_solver, bar_w, label="Solver (Approx input)", color="#59a14f")
    ax.bar(x - 0.6 * bar_w, s_query, bar_w, label="Approximate Query", color="#2a9d8f")
    ax.bar(x + 0.6 * bar_w, e_solver, bar_w, label="Solver (Exact input)", color="#b07aa1")
    ax.bar(x + 1.8 * bar_w, e_query, bar_w, label="Exact Query", color="#f28e2b")

    ax.set_xticks(x)
    ax.set_xticklabels([str(e) for e in epochs])
    ax.set_xlabel("Epoch", fontsize=LABEL_FS)
    ax.set_ylabel("Time (ms)", fontsize=LABEL_FS)
    ax.set_yscale("log")
    ax.set_title(
        "Query+Solver Time Comparison\n"
        "Approximate VS Exact",
        fontsize=TITLE_FS,
        pad=14,
    )
    ax.tick_params(axis="both", labelsize=TICK_FS)
    ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.20),
        ncol=2,
        fontsize=LEGEND_FS,
        frameon=False,
    )
    ax.grid(axis="y", alpha=0.3, which="both")
    plt.tight_layout(rect=[0, 0.01, 1, 0.95])
    plt.savefig(out_path, dpi=220)
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
    plot_query_solver_split(
        data_dir / "rtt_results_epoch_full_ortools_cbc_30nodes.csv",
        plot_dir / "rtt_epoch_query_solver_split_ortools_cbc_30nodes.png",
        "CBC",
    )
    plot_query_solver_logy(
        data_dir / "rtt_results_epoch_full_ortools_cbc_30nodes.csv",
        plot_dir / "rtt_epoch_query_solver_logy_ortools_cbc_30nodes.png",
        "CBC",
    )
    plot_query_solver(
        data_dir / "rtt_results_epoch_full_ortools_scip_30nodes.csv",
        plot_dir / "rtt_epoch_query_solver_ortools_scip_30nodes.png",
        "SCIP",
    )
    plot_query_solver_split(
        data_dir / "rtt_results_epoch_full_ortools_scip_30nodes.csv",
        plot_dir / "rtt_epoch_query_solver_split_ortools_scip_30nodes.png",
        "SCIP",
    )
    plot_query_solver_logy(
        data_dir / "rtt_results_epoch_full_ortools_scip_30nodes.csv",
        plot_dir / "rtt_epoch_query_solver_logy_ortools_scip_30nodes.png",
        "SCIP",
    )

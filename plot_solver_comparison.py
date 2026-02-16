#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt


FILES = [
    "rtt_solver_1.csv",
    "rtt_solver_2.csv",
    "rtt_solver_3.csv",
    "rtt_solver_4.csv",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--in-dir",
        type=str,
        default="data",
        help="Input directory containing rtt_solver_*.csv files",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default="plots",
        help="Output directory for generated plots",
    )
    return parser.parse_args()


def _resolve_input(in_dir: Path, file_name: str) -> Path:
    candidate = in_dir / file_name
    if candidate.exists():
        return candidate
    legacy = Path(file_name)
    if legacy.exists():
        return legacy
    return candidate


def _read_csv(path: Path) -> Tuple[Dict[str, str], Dict[str, List[float]]]:
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
    if not rows:
        raise RuntimeError(f"{path} is empty")

    meta = rows[0]
    series: Dict[str, List[float]] = {}
    for key in rows[0].keys():
        if key == "timestamp_utc":
            continue
        series[key] = [float(row[key]) for row in rows]
    return meta, series


def _label(meta: Dict[str, str]) -> str:
    tasks = meta["solver_task_count"]
    solver_nodes = meta["solver_node_count"]
    query_nodes = meta["query_node_count"]
    all_nodes = meta["all_nodes_count"]
    all_tasks = meta["all_tasks_count"]
    tasks_label = f"{tasks}" if tasks != "0" else f"all({all_tasks})"
    solver_nodes_label = f"{solver_nodes}" if solver_nodes != "0" else f"all({all_nodes})"
    query_nodes_label = f"{query_nodes}" if query_nodes != "0" else f"all({all_nodes})"
    return (
        f"tasks={tasks_label}, solver_nodes={solver_nodes_label}, "
        f"query_nodes={query_nodes_label}"
    )


def _plot_total(meta: Dict[str, str], series: Dict[str, List[float]], out_path: Path) -> None:
    epochs = series["epoch"]
    plt.figure(figsize=(10, 6))
    plt.plot(epochs, series["server_total_ms"], label="server total (ms)")
    plt.plot(epochs, series["es_total_ms"], label="es total (ms)")
    plt.xlabel("Epoch")
    plt.ylabel("RTT (ms)")
    plt.title(f"Total RTT vs Epoch | {_label(meta)}")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path)


def _plot_compare(
    title: str,
    y_server_key: str,
    y_es_key: str,
    runs: List[Tuple[str, Dict[str, str], Dict[str, List[float]]]],
    out_path: Path,
) -> None:
    plt.figure(figsize=(11, 7))
    for name, meta, series in runs:
        epochs = series["epoch"]
        label = f"{name} | {_label(meta)}"
        plt.plot(epochs, series[y_server_key], label=f"{label} | server")
        plt.plot(epochs, series[y_es_key], label=f"{label} | es", linestyle="--")
    plt.xlabel("Epoch")
    plt.ylabel("Time (ms)")
    plt.title(title)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path)


def main() -> None:
    args = parse_args()
    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    runs: Dict[str, Tuple[Dict[str, str], Dict[str, List[float]]]] = {}
    for file_name in FILES:
        path = _resolve_input(in_dir, file_name)
        meta, series = _read_csv(path)
        runs[file_name] = (meta, series)

    # 4 total-time graphs (one per CSV)
    for file_name, (meta, series) in runs.items():
        stem = Path(file_name).stem
        _plot_total(meta, series, out_dir / f"{stem}_total_vs_epoch.png")

    # 4 comparisons (for both query and solver times)
    # A) task=1, nodes=5 vs nodes=all
    compare_a = [
        ("rtt_solver_2", *runs["rtt_solver_2.csv"]),
        ("rtt_solver_1", *runs["rtt_solver_1.csv"]),
    ]
    _plot_compare(
        "Query Time vs Epoch | task=1, nodes: 5 vs all",
        "server_query_ms",
        "es_query_ms",
        compare_a,
        out_dir / "compare_query_task1_nodes_5_vs_all.png",
    )
    _plot_compare(
        "Solver Time vs Epoch | task=1, nodes: 5 vs all",
        "server_solver_ms",
        "es_solver_ms",
        compare_a,
        out_dir / "compare_solver_task1_nodes_5_vs_all.png",
    )

    # B) task=all, nodes=5 vs nodes=all
    compare_b = [
        ("rtt_solver_3", *runs["rtt_solver_3.csv"]),
        ("rtt_solver_4", *runs["rtt_solver_4.csv"]),
    ]
    _plot_compare(
        "Query Time vs Epoch | task=all, nodes: 5 vs all",
        "server_query_ms",
        "es_query_ms",
        compare_b,
        out_dir / "compare_query_taskall_nodes_5_vs_all.png",
    )
    _plot_compare(
        "Solver Time vs Epoch | task=all, nodes: 5 vs all",
        "server_solver_ms",
        "es_solver_ms",
        compare_b,
        out_dir / "compare_solver_taskall_nodes_5_vs_all.png",
    )

    # C) nodes=5, task=1 vs task=all
    compare_c = [
        ("rtt_solver_2", *runs["rtt_solver_2.csv"]),
        ("rtt_solver_3", *runs["rtt_solver_3.csv"]),
    ]
    _plot_compare(
        "Query Time vs Epoch | nodes=5, task: 1 vs all",
        "server_query_ms",
        "es_query_ms",
        compare_c,
        out_dir / "compare_query_nodes5_task_1_vs_all.png",
    )
    _plot_compare(
        "Solver Time vs Epoch | nodes=5, task: 1 vs all",
        "server_solver_ms",
        "es_solver_ms",
        compare_c,
        out_dir / "compare_solver_nodes5_task_1_vs_all.png",
    )

    # D) nodes=all, task=1 vs task=all
    compare_d = [
        ("rtt_solver_1", *runs["rtt_solver_1.csv"]),
        ("rtt_solver_4", *runs["rtt_solver_4.csv"]),
    ]
    _plot_compare(
        "Query Time vs Epoch | nodes=all, task: 1 vs all",
        "server_query_ms",
        "es_query_ms",
        compare_d,
        out_dir / "compare_query_nodesall_task_1_vs_all.png",
    )
    _plot_compare(
        "Solver Time vs Epoch | nodes=all, task: 1 vs all",
        "server_solver_ms",
        "es_solver_ms",
        compare_d,
        out_dir / "compare_solver_nodesall_task_1_vs_all.png",
    )

    print("Wrote plots:")
    for file_name in FILES:
        stem = Path(file_name).stem
        print(f"  {stem}_total_vs_epoch.png")
    print("  compare_query_task1_nodes_5_vs_all.png")
    print("  compare_query_taskall_nodes_5_vs_all.png")
    print("  compare_query_nodes5_task_1_vs_all.png")
    print("  compare_query_nodesall_task_1_vs_all.png")
    print("  compare_solver_task1_nodes_5_vs_all.png")
    print("  compare_solver_taskall_nodes_5_vs_all.png")
    print("  compare_solver_nodes5_task_1_vs_all.png")
    print("  compare_solver_nodesall_task_1_vs_all.png")


if __name__ == "__main__":
    main()

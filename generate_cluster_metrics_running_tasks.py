#!/usr/bin/env python3
"""Generate cluster metrics CSV using solver topology/task inputs."""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


DATA_ROOT = Path(__file__).resolve().parent / "solver_experimental" / "python_solver" / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=200, help="Total rows to generate")
    parser.add_argument("--chunk-size", type=int, default=100, help="Rows per write batch")
    parser.add_argument("--start", type=str, default="2025-01-01T00:00:00Z", help="Start timestamp (ISO-8601)")
    parser.add_argument("--out", type=str, default="~/cluster-metrics.csv", help="Output CSV filename")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--nodes",
        type=str,
        default=str(DATA_ROOT / "nodes.json"),
        help="Path to nodes.json",
    )
    parser.add_argument(
        "--edges",
        type=str,
        default=str(DATA_ROOT / "edges.json"),
        help="Path to edges.json",
    )
    parser.add_argument(
        "--tasks",
        type=str,
        default=str(DATA_ROOT / "tasks.json"),
        help="Path to tasks.json",
    )
    parser.add_argument(
        "--cluster-count",
        type=int,
        default=31,
        help="Total cluster count (clusters named N000..N030 by default)",
    )
    parser.add_argument(
        "--min-duration",
        type=float,
        default=300.0,
        help="Minimum estimated duration in seconds",
    )
    parser.add_argument(
        "--max-duration",
        type=float,
        default=14_400.0,
        help="Maximum estimated duration in seconds",
    )
    return parser.parse_args()


def parse_start(value: str) -> datetime:
    value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def load_json(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def build_capacity_maps(
    nodes: Iterable[Dict[str, Any]], edges: Iterable[Dict[str, Any]]
) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float]]:
    cpu_caps: Dict[str, float] = {}
    mem_caps: Dict[str, float] = {}
    net_caps: Dict[str, float] = {}
    for node in nodes:
        node_id = str(node["node_id"])
        cpu_caps[node_id] = float(node.get("cpu_capacity", 0.0))
        mem_caps[node_id] = float(node.get("memory_capacity", 0.0))
        net_caps[node_id] = 0.0

    for edge in edges:
        capacity = float(edge.get("capacity", 0.0))
        for endpoint in ("source", "target"):
            node_id = str(edge.get(endpoint, ""))
            if node_id in net_caps:
                net_caps[node_id] += capacity

    return cpu_caps, mem_caps, net_caps


def normalize_clusters(count: int) -> List[str]:
    return [f"N{idx:03d}" for idx in range(max(0, count))]


def normalize_tasks(
    tasks: List[Dict[str, Any]], count: int, start_index: int = 100
) -> List[Dict[str, Any]]:
    reserved = {str(task.get("task_id", "")) for task in tasks}
    normalized: List[Dict[str, Any]] = []
    idx = start_index
    while len(normalized) < count:
        task = dict(tasks[idx % len(tasks)])
        task_id = f"T{idx:03d}"
        while task_id in reserved:
            idx += 1
            task_id = f"T{idx:03d}"
        task["task_id"] = task_id
        normalized.append(task)
        idx += 1
    return normalized


def assign_running_tasks(
    clusters: List[str], tasks: List[Dict[str, Any]]
) -> List[Tuple[str, Dict[str, Any]]]:
    assignments: List[Tuple[str, Dict[str, Any]]] = []
    for cluster, task in zip(itertools.cycle(clusters), tasks):
        assignments.append((cluster, task))
    return assignments


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    start_dt = parse_start(args.start)

    nodes = load_json(args.nodes)
    edges = load_json(args.edges)
    tasks = load_json(args.tasks)

    cpu_caps, mem_caps, net_caps = build_capacity_maps(nodes, edges)
    clusters = normalize_clusters(args.cluster_count)
    if not clusters:
        raise SystemExit("Cluster count must be at least 1.")
    tasks = normalize_tasks(tasks, args.rows)
    assignments = assign_running_tasks(clusters, tasks)

    if not assignments:
        raise SystemExit("No running tasks assigned; adjust --tasks-per-cluster or tasks.json.")

    total = min(args.rows, len(assignments))
    chunk = max(1, args.chunk_size)
    min_duration = max(0.0, args.min_duration)
    max_duration = max(min_duration, args.max_duration)

    out_path = os.path.expanduser(args.out)
    with open(out_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "timestamp",
                "cluster",
                "task",
                "cpu_cores",
                "memory_gb",
                "network_mbps",
                "estimated_duration",
            ]
        )

        written = 0
        while written < total:
            n = min(chunk, total - written)
            for i in range(n):
                ts = (start_dt + timedelta(seconds=written + i)).strftime("%Y-%m-%dT%H:%M:%SZ")
                cluster, task = assignments[written + i]
                node_idx = int(cluster[1:]) if cluster[1:].isdigit() else 0
                source_node = nodes[node_idx % len(nodes)]
                source_id = str(source_node["node_id"])
                cpu_cap = cpu_caps.get(source_id, 0.0)
                mem_cap = mem_caps.get(source_id, 0.0)
                net_cap = net_caps.get(source_id, 0.0)

                task_cpu = float(task.get("cpu", 0.0))
                task_mem = float(task.get("memory", 0.0))
                task_net = float(task.get("bandwidth", 0.0))
                priority = float(task.get("priority", 1.0)) or 1.0

                cpu = clamp(task_cpu * rng.uniform(0.6, 1.1), 0.01, max(cpu_cap, 0.01))
                mem = clamp(task_mem * rng.uniform(0.6, 1.1), 0.01, max(mem_cap, 0.01))
                net_upper = net_cap if net_cap > 0.0 else max(task_net, 1.0)
                net = clamp(task_net * rng.uniform(0.6, 1.2), 0.01, net_upper)
                duration = rng.uniform(min_duration, max_duration) / max(priority, 0.1)

                writer.writerow(
                    [
                        ts,
                        cluster,
                        task.get("task_id", ""),
                        f"{cpu:.3f}",
                        f"{mem:.3f}",
                        f"{net:.2f}",
                        f"{duration:.1f}",
                    ]
                )
            written += n


if __name__ == "__main__":
    main()

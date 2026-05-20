#!/usr/bin/env python3
"""Run a greedy task-placement baseline on scheduler JSONL/CSV inputs."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from scheduler.entities import NetworkTopology
from scheduler.greedy_solver import GreedyTaskScheduler, NodeOrder, TaskOrder
from scheduler.load_info import load_edges, load_nodes, load_tasks


DEFAULT_DATA_DIR = Path(__file__).resolve().parent / "dummy_data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a first-fit greedy placement baseline using the same node, edge, "
            "and task inputs as the PuLP scheduler."
        )
    )
    parser.add_argument(
        "--node-path",
        type=Path,
        default=DEFAULT_DATA_DIR / "nodes.jsonl",
        help="Path to nodes JSONL/CSV input.",
    )
    parser.add_argument(
        "--edge-path",
        type=Path,
        default=DEFAULT_DATA_DIR / "edges.jsonl",
        help="Path to edges JSONL/CSV input.",
    )
    parser.add_argument(
        "--task-path",
        type=Path,
        default=DEFAULT_DATA_DIR / "tasks.jsonl",
        help="Path to tasks JSONL/CSV input.",
    )
    parser.add_argument(
        "--task-order",
        choices=["input", "arrival", "largest"],
        default="largest",
        help=(
            "Greedy task ordering. 'largest' is a resource-demand proxy because "
            "the current task schema has no accuracy/timeliness fields."
        ),
    )
    parser.add_argument(
        "--node-order",
        choices=["id", "available_cpu"],
        default="available_cpu",
        help="Candidate node order for first-fit placement.",
    )
    parser.add_argument(
        "--task-count",
        type=int,
        default=0,
        help="Limit to the first N tasks after loading (0 = all).",
    )
    parser.add_argument(
        "--node-count",
        type=int,
        default=0,
        help="Limit to the first N nodes sorted by node id (0 = all).",
    )
    parser.add_argument(
        "--out-json",
        type=Path,
        help="Optional path for writing assignment decisions and residual resources.",
    )
    return parser.parse_args()


def _limit_mapping(mapping: dict[str, Any], count: int) -> dict[str, Any]:
    if count <= 0 or count >= len(mapping):
        return dict(mapping)
    return {key: mapping[key] for key in sorted(mapping)[:count]}


def _json_payload(result: Any) -> dict[str, Any]:
    return {
        "assigned_count": result.assigned_count,
        "unassigned_count": len(result.unassigned_tasks),
        "decisions": {
            task_id: asdict(decision)
            for task_id, decision in sorted(result.decisions.items())
        },
        "unassigned_tasks": sorted(result.unassigned_tasks),
        "residual_cpu": dict(sorted(result.residual_cpu.items())),
        "residual_memory": dict(sorted(result.residual_memory.items())),
        "residual_bandwidth": {
            f"{edge_id[0]}--{edge_id[1]}": value
            for edge_id, value in sorted(result.residual_bandwidth.items())
        },
    }


def main() -> None:
    args = parse_args()
    nodes = _limit_mapping(load_nodes(args.node_path), args.node_count)
    node_ids = set(nodes)
    edges = {
        edge_id: edge
        for edge_id, edge in load_edges(args.edge_path).items()
        if edge_id[0] in node_ids and edge_id[1] in node_ids
    }
    tasks = _limit_mapping(load_tasks(args.task_path), args.task_count)

    network = NetworkTopology(nodes.values(), edges.values())
    scheduler = GreedyTaskScheduler(
        network,
        task_order=args.task_order,
        node_order=args.node_order,
    )
    result = scheduler.solve(tasks)

    print(
        "Greedy baseline complete: "
        f"assigned={result.assigned_count}, "
        f"unassigned={len(result.unassigned_tasks)}, "
        f"nodes={len(nodes)}, edges={len(edges)}, tasks={len(tasks)}, "
        f"task_order={args.task_order}, node_order={args.node_order}"
    )
    if result.unassigned_tasks:
        preview = ", ".join(sorted(result.unassigned_tasks)[:10])
        suffix = "..." if len(result.unassigned_tasks) > 10 else ""
        print(f"Unassigned preview: {preview}{suffix}")

    if args.out_json:
        args.out_json.parent.mkdir(parents=True, exist_ok=True)
        args.out_json.write_text(
            json.dumps(_json_payload(result), indent=2),
            encoding="utf-8",
        )
        print(f"Wrote {args.out_json}")


if __name__ == "__main__":
    main()

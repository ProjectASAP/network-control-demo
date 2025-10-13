"""
Helpers for loading network controller solver inputs from JSON files.

The expected schema for each data file is documented in the loader docstrings.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence, Tuple

from .solver import (
    Edge,
    EdgeKey,
    ExistingAssignment,
    NetworkControllerSolver,
    Node,
    Task,
    TaskCommunication,
    build_edges,
    build_nodes,
)

JsonMapping = MutableMapping[str, object]


def _coerce_path(edges: Iterable[Sequence[str]]) -> Tuple[EdgeKey, ...]:
    return tuple((edge[0], edge[1]) for edge in edges)


def _read_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_nodes(path: str | Path) -> Dict[str, Node]:
    """
    Load node capacities from a JSON file.

    Expected format:
        [
            {
                "node_id": "A",
                "cpu_capacity": 16,
                "memory_capacity": 64,
                "used_cpu": 4,              # optional
                "used_memory": 8            # optional
            },
            ...
        ]
    """
    payload = _read_json(Path(path))
    if not isinstance(payload, list):
        raise TypeError("Node payload must be a list")
    structured: Dict[str, Mapping[str, float]] = {}
    for node in payload:
        if not isinstance(node, Mapping):
            raise TypeError("Each node entry must be a mapping")
        node_id = str(node["node_id"])
        structured[node_id] = {
            "cpu": float(node["cpu_capacity"]),
            "memory": float(node["memory_capacity"]),
            "used_cpu": float(node.get("used_cpu", 0)),
            "used_memory": float(node.get("used_memory", 0)),
        }
    return build_nodes(structured)


def load_edges(path: str | Path) -> Dict[EdgeKey, Edge]:
    """
    Load edge capacities from a JSON file.

    Expected format:
        [
            {
                "source": "A",
                "target": "B",
                "capacity": 100,
                "used_bandwidth": 10    # optional
            },
            ...
        ]
    """
    payload = _read_json(Path(path))
    if not isinstance(payload, list):
        raise TypeError("Edge payload must be a list")
    structured: Dict[EdgeKey, Mapping[str, float]] = {}
    for edge in payload:
        if not isinstance(edge, Mapping):
            raise TypeError("Each edge entry must be a mapping")
        key: EdgeKey = (str(edge["source"]), str(edge["target"]))
        structured[key] = {
            "capacity": float(edge["capacity"]),
            "used": float(edge.get("used_bandwidth", 0)),
        }
    return build_edges(structured)


def load_tasks(path: str | Path) -> List[Task]:
    """
    Load task definitions from a JSON file.

    Expected format:
        [
            {
                "task_id": "video-analytics",
                "cpu": 6,
                "memory": 24,
                "bandwidth": 25,            # optional, defaults to sum of communications
                "priority": 3.0,
                "communications": [
                    {"target_task": "telemetry", "bandwidth": 20},
                    ...
                ],
                "allowed_nodes": ["A", "C"]   # optional constraint on placement nodes
            },
            ...
        ]
    """
    payload = _read_json(Path(path))
    if not isinstance(payload, list):
        raise TypeError("Task payload must be a list")
    tasks: List[Task] = []
    for item in payload:
        if not isinstance(item, Mapping):
            raise TypeError("Each task entry must be a mapping")
        communications_payload = item.get("communications", [])
        if not isinstance(communications_payload, list):
            raise TypeError("Task communications must be provided as a list")
        communications: List[TaskCommunication] = []
        for channel in communications_payload:
            if not isinstance(channel, Mapping):
                raise TypeError("Communication entry must be a mapping")
            communications.append(
                TaskCommunication(
                    target_task_id=str(channel["target_task"]),
                    bandwidth=float(channel["bandwidth"]),
                )
            )
        total_comm_bandwidth = sum(comm.bandwidth for comm in communications)
        if "bandwidth" in item:
            bandwidth_value = float(item["bandwidth"])
            if communications and total_comm_bandwidth - bandwidth_value > 1e-9:
                raise ValueError(
                    f"Task {item['task_id']} has communications requiring {total_comm_bandwidth} "
                    f"bandwidth but explicit bandwidth {bandwidth_value} is smaller"
                )
        else:
            bandwidth_value = total_comm_bandwidth

        allowed_nodes_payload = item.get("allowed_nodes")
        if allowed_nodes_payload is not None:
            if not isinstance(allowed_nodes_payload, list):
                raise TypeError("allowed_nodes must be provided as a list if specified")
            allowed_nodes = tuple(str(node) for node in allowed_nodes_payload)
        else:
            allowed_nodes = None

        tasks.append(
            Task(
                task_id=str(item["task_id"]),
                cpu=float(item["cpu"]),
                memory=float(item["memory"]),
                bandwidth=float(bandwidth_value),
                priority=float(item.get("priority", 1.0)),
                communications=tuple(communications),
                allowed_nodes=allowed_nodes,
            )
        )
    return tasks


def load_existing_assignments(path: str | Path) -> List[ExistingAssignment]:
    """
    Load existing assignments from a JSON file.

    Expected format:
        [
            {
                "task_id": "legacy-1",
                "node_id": "A",
                "cpu": 4,
                "memory": 16,
                "bandwidth": 20,
                "path": [["A", "B"]]
            },
            ...
        ]
    """
    payload = _read_json(Path(path))
    if not isinstance(payload, list):
        raise TypeError("Existing assignment payload must be a list")
    assignments: List[ExistingAssignment] = []
    for item in payload:
        if not isinstance(item, Mapping):
            raise TypeError("Each existing assignment must be a mapping")
        raw_path = item.get("path", [])
        if not isinstance(raw_path, list):
            raise TypeError("Assignment path must be a list")
        assignments.append(
            ExistingAssignment(
                task_id=str(item["task_id"]),
                node_id=str(item["node_id"]),
                cpu=float(item["cpu"]),
                memory=float(item["memory"]),
                bandwidth=float(item["bandwidth"]),
                path=_coerce_path(raw_path),
            )
        )
    return assignments


def load_previous_assignments(path: str | Path) -> Dict[str, str]:
    """
    Load previous epoch assignments from a JSON file.

    Expected format:
        {
            "task-id": "node-id",
            ...
        }
    """
    payload = _read_json(Path(path))
    if not isinstance(payload, Mapping):
        raise TypeError("Previous assignment payload must be an object mapping")
    return {str(task_id): str(node_id) for task_id, node_id in payload.items()}


def load_solver_from_directory(
    directory: str | Path,
    *,
    nodes_file: str = "nodes.json",
    edges_file: str = "edges.json",
) -> NetworkControllerSolver:
    """
    Instantiate a solver using data stored in ``directory``.
    """
    dir_path = Path(directory)
    nodes = load_nodes(dir_path / nodes_file)
    edges = load_edges(dir_path / edges_file)
    return NetworkControllerSolver(nodes, edges)


__all__ = [
    "load_edges",
    "load_existing_assignments",
    "load_nodes",
    "load_previous_assignments",
    "load_solver_from_directory",
    "load_tasks",
]

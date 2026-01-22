from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence, Tuple
import pandas as pd
import datetime as dt
import math
import networkx as nx
import jsonlines

from .entities import Edge, EdgeKey, Node, Task, TaskCommunication

_JSONL_SUFFIXES = {".jsonl", ".ndjson"}


def _path_is_jsonl(path: str | Path) -> bool:
    return Path(path).suffix.lower() in _JSONL_SUFFIXES


def _apply_column_mapping(
    row: Mapping[str, object], column_names: Mapping[str, str] | None
) -> dict[str, object]:
    if column_names is None:
        return dict(row)
    return {column_names.get(key, key): value for key, value in row.items()}


def _read_jsonl(path: str | Path) -> Iterable[dict[str, object]]:
    with jsonlines.open(path) as reader:
        for row in reader:
            if row is None:
                continue
            if isinstance(row, dict):
                yield row
            else:
                raise ValueError(f"Expected JSON object per line in {path}.")


def load_nodes(
    path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs
) -> dict[str, Node]:
    """
    Load node capacities from a CSV or JSONL file. Specify mapping between column names and expected fields if they differ.

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
    if _path_is_jsonl(path):
        return _load_nodes_jsonl(path, column_names=column_names)
    return _load_nodes_csv(path, column_names=column_names, **kwargs)


def _load_nodes_csv(
    path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs
) -> dict[str, Node]:
    df = pd.read_csv(path, **kwargs)
    if column_names is not None:
        df = df.rename(columns=column_names)
    payload = df.to_dict(orient="records")
    result = {}
    for row in payload:
        if row["node_id"] in result:
            continue
        node = Node(
            node_id=str(row["node_id"]),
            cpu_capacity=float(row["cpu_capacity"]),
            memory_capacity=float(row["memory_capacity"]),
            network_capacity=(
                float(row["network_capacity"])
                if row.get("network_capacity") not in (None, "")
                else None
            ),
            used_cpu=float(row.get("used_cpu", 0)),
            used_memory=float(row.get("used_memory", 0)),
            used_network=float(row.get("used_network", 0)),
        )
        result[node.node_id] = node
    return result


def _load_nodes_jsonl(
    path: str | Path, column_names: Mapping[str, str] | None = None
) -> dict[str, Node]:
    result: dict[str, Node] = {}
    for row in _read_jsonl(path):
        row = _apply_column_mapping(row, column_names)
        node_id = str(row["node_id"])
        if node_id in result:
            continue
        node = Node(
            node_id=node_id,
            cpu_capacity=float(row["cpu_capacity"]),
            memory_capacity=float(row["memory_capacity"]),
            network_capacity=(
                float(row["network_capacity"])
                if row.get("network_capacity") not in (None, "")
                else None
            ),
            used_cpu=float(row.get("used_cpu", 0) or 0),
            used_memory=float(row.get("used_memory", 0) or 0),
            used_network=float(row.get("used_network", 0) or 0),
        )
        result[node.node_id] = node
    return result


def load_edges(
    path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs
) -> dict[EdgeKey, Edge]:
    """
    Load edge capacities from a CSV or JSONL file. Specify mapping between column names and expected fields if they differ.

    Expected format:
        [
            {
                "source_node_id": "A",
                "target": "B",
                "capacity": 100,
                "used_bandwidth": 10    # optional
            },
            ...
        ]
    """
    if _path_is_jsonl(path):
        return _load_edges_jsonl(path, column_names=column_names)
    return _load_edges_csv(path, column_names=column_names, **kwargs)


def _load_edges_csv(
    path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs
) -> dict[EdgeKey, Edge]:
    df = pd.read_csv(path, **kwargs)
    if column_names is not None:
        df = df.rename(columns=column_names)
    payload = df.to_dict(orient="records")
    result = {}
    for row in payload:
        # Assume undirected graph topology.
        key: EdgeKey = (str(row["source"]), str(row["target"]))
        key = tuple(sorted(key))  # type: ignore
        if key in result:
            continue
        edge = Edge(
            edge_id=key,
            capacity=float(row["capacity"]),
            used_bandwidth=float(row.get("used_bandwidth", 0)),
        )
        result[key] = edge
    return result


def _load_edges_jsonl(
    path: str | Path, column_names: Mapping[str, str] | None = None
) -> dict[EdgeKey, Edge]:
    result: dict[EdgeKey, Edge] = {}
    for row in _read_jsonl(path):
        row = _apply_column_mapping(row, column_names)
        edge_id = row.get("edge_id")
        if edge_id is None:
            edge_id = (row["source"], row["target"])
        if not isinstance(edge_id, (list, tuple)) or len(edge_id) != 2:
            raise ValueError(f"Invalid edge_id {edge_id} in {path}.")
        key: EdgeKey = (str(edge_id[0]), str(edge_id[1]))
        key = tuple(sorted(key))  # type: ignore
        if key in result:
            continue
        edge = Edge(
            edge_id=key,
            capacity=float(row["capacity"]),
            used_bandwidth=float(row.get("used_bandwidth", 0) or 0),
        )
        result[key] = edge
    return result


def load_tasks(
    path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs
) -> dict[str, Task]:
    """
    Load task requests from a CSV or JSONL file. Specify mapping between column names and expected fields if they differ.

    Expected format:
        [
            {
                "task_id": "task1",
                "arrival_offset_s": 22.10
                "initial_cpu": 10,
                "initial_memory": 50,
                "duration_s": 3600.0
            }
        ]
    """
    if _path_is_jsonl(path):
        return _load_tasks_jsonl(path, column_names=column_names)
    return _load_tasks_csv(path, column_names=column_names, **kwargs)


def _load_tasks_csv(
    path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs
) -> dict[str, Task]:
    df = pd.read_csv(path, **kwargs).fillna(
        ""
    )  # Fill NaN in case certain tasks don't have peers.
    if column_names is not None:
        df = df.rename(columns=column_names)
    payload = df.to_dict(orient="records")
    result = {}
    for row in payload:
        task_id = str(row["task_id"])
        if task_id in result:
            continue
        peer_task_ids = row.get("peer_task_ids", "")
        peer_task_ids = peer_task_ids.split(";") if peer_task_ids else []
        peer_bandwidths = str(row.get("peer_bandwidths", ""))
        peer_bandwidths = (
            [float(bw) for bw in str(peer_bandwidths).split(";")]
            if peer_bandwidths
            else []
        )
        task = Task(
            task_id=task_id,
            arrival_offset_s=float(row["arrival_offset_s"]),
            duration_s=float(row["duration_s"]),
            initial_cpu=float(row["initial_cpu"]),
            initial_memory=float(row["initial_memory"]),
            peer_bandwidths={t: bw for t, bw in zip(peer_task_ids, peer_bandwidths)},
        )
        result[task.task_id] = task
    return result


def _parse_peer_task_ids(value: object | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if value == "":
        return []
    return str(value).split(";")


def _parse_peer_bandwidths(
    peer_task_ids: object | None, peer_bandwidths: object | None
) -> dict[str, float]:
    if isinstance(peer_bandwidths, dict):
        return {str(task_id): float(bw) for task_id, bw in peer_bandwidths.items()}
    if peer_bandwidths is None or peer_bandwidths == "":
        return {}
    peer_ids = _parse_peer_task_ids(peer_task_ids)
    if isinstance(peer_bandwidths, list):
        return {task_id: float(bw) for task_id, bw in zip(peer_ids, peer_bandwidths)}
    bandwidth_values = (
        [float(bw) for bw in str(peer_bandwidths).split(";")] if peer_bandwidths else []
    )
    return {task_id: bw for task_id, bw in zip(peer_ids, bandwidth_values)}


def _load_tasks_jsonl(
    path: str | Path, column_names: Mapping[str, str] | None = None
) -> dict[str, Task]:
    result: dict[str, Task] = {}
    for row in _read_jsonl(path):
        row = _apply_column_mapping(row, column_names)
        task_id = str(row["task_id"])
        if task_id in result:
            continue
        peer_bandwidths = _parse_peer_bandwidths(
            row.get("peer_task_ids"),
            row.get("peer_bandwidths"),
        )
        task = Task(
            task_id=task_id,
            arrival_offset_s=float(row["arrival_offset_s"]),
            duration_s=float(row["duration_s"]),
            initial_cpu=float(row["initial_cpu"]),
            initial_memory=float(row["initial_memory"]),
            peer_bandwidths=peer_bandwidths,
        )
        result[task.task_id] = task
    return result


def build_task_graph(tasks: dict[str, Task]) -> nx.DiGraph:
    task_graph = nx.DiGraph()
    for t_i, task in tasks.items():
        for t_j, bw in task.peer_bandwidths.items():
            task_graph.add_edge(t_i, t_j, bandwidth=bw)
    return task_graph


def load_task_communications(
    path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs
) -> dict[tuple[str, str], TaskCommunication]:
    """
    Load task communication demands from a CSV or JSONL file. Specify mapping between column names and expected fields if they differ.

    Expected format:
        [
            {
                "source_task_id": "task1",
                "target_task_id": "task2",
                "bandwidth": 20
            },
            ...
        ]
    """
    if _path_is_jsonl(path):
        return _load_task_communications_jsonl(path, column_names=column_names)
    return _load_task_communications_csv(path, column_names=column_names, **kwargs)


def _load_task_communications_csv(
    path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs
) -> dict[tuple[str, str], TaskCommunication]:
    df = pd.read_csv(path, **kwargs)
    if column_names is not None:
        df = df.rename(columns=column_names)
    payload = df.to_dict(orient="records")
    result = {}
    for row in payload:
        t_i, t_j = str(row["source_task_id"]), str(row["target_task_id"])
        if (t_i, t_j) in result:
            continue
        comm = TaskCommunication(
            source_task_id=t_i,
            target_task_id=t_j,
            bandwidth=float(row["bandwidth"]),
        )
        result[(comm.source_task_id, comm.target_task_id)] = comm
    return result


def _load_task_communications_jsonl(
    path: str | Path, column_names: Mapping[str, str] | None = None
) -> dict[tuple[str, str], TaskCommunication]:
    result: dict[tuple[str, str], TaskCommunication] = {}
    for row in _read_jsonl(path):
        row = _apply_column_mapping(row, column_names)
        t_i, t_j = str(row["source_task_id"]), str(row["target_task_id"])
        if (t_i, t_j) in result:
            continue
        comm = TaskCommunication(
            source_task_id=t_i,
            target_task_id=t_j,
            bandwidth=float(row["bandwidth"]),
        )
        result[(comm.source_task_id, comm.target_task_id)] = comm
    return result

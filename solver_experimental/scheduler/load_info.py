from __future__ import annotations

from cattrs import structure
import jsonlines
from pathlib import Path
from typing import Iterable, Mapping, TypeVar
import pandas as pd
import datetime as dt
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
            

def _load_entities_jsonl(path: str | Path, cls: type[T], mapping: Mapping[str, str] | None = None) -> list[T]:
    results = []
    for obj in _read_jsonl(path):
        data = _apply_column_mapping(obj, mapping)
        entity = structure(data, cls)
        results.append(entity)
    return results


T = TypeVar("T")


def load_nodes(path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs) -> dict[str, Node]:
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
        entities = _load_entities_jsonl(path, Node, mapping=column_names)
        result = {}
        for node in entities:
            if node.node_id in result:
                continue
            result[node.node_id] = node
        return result
    return _load_nodes_csv(path, column_names=column_names, **kwargs)


def _load_nodes_csv(
    path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs
) -> dict[str, Node]:
    # TODO: Old CSV loading logic. Remove/refactor later.
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
        node = structure(row, Node)
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
        entities = _load_entities_jsonl(path, Edge, mapping=column_names)
        result = {}
        for edge in entities:
            key = tuple(sorted(edge.edge_id))  # type: ignore
            if key in result:
                continue
            result[key] = edge
        return result
    return _load_edges_csv(path, column_names=column_names, **kwargs)


def _load_edges_csv(
    path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs
) -> dict[EdgeKey, Edge]:
    # TODO: Old CSV loading logic. Remove/refactor later.
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
        entities = _load_entities_jsonl(path, Task, mapping=column_names)
        result = {}
        for task in entities:
            if task.task_id in result:
                continue
            result[task.task_id] = task
        return result
    return _load_tasks_csv(path, column_names=column_names, **kwargs)


def _load_tasks_csv(
    path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs
) -> dict[str, Task]:
    df = pd.read_csv(path, **kwargs).fillna(
        ""
    )  # Fill NaN in case certain tasks don't have peers.
    # TODO: Old CSV loading logic. Remove/refactor later.
    df = pd.read_csv(path, **kwargs).fillna("") # Fill NaN in case certain tasks don't have peers.
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


def build_task_graph(tasks: dict[str, Task]) -> nx.DiGraph:
    task_graph = nx.DiGraph()
    for t_i, task in tasks.items():
        for t_j, bw in task.peer_bandwidths.items():
            task_graph.add_edge(t_i, t_j, bandwidth=bw)
    return task_graph

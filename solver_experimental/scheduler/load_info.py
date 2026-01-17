from __future__ import annotations

from cattrs import structure
import jsonlines
from pathlib import Path
from typing import Mapping, TypeVar
import pandas as pd
import datetime as dt
import networkx as nx

from .entities import Edge, EdgeKey, Node, Task, TaskCommunication


T = TypeVar("T")


def load_nodes(path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs) -> dict[str, Node]:
    """
    Load node capacities from a CSV file. Specify mapping between column names and expected fields if they differ.

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

    # NOTE: Support JSONL format for easier data representation.
    ext = Path(path).suffix.lower()
    if ext == ".jsonl":
        entities = load_entities_jsonl(path, Node)
        result = {}
        for node in entities:
            if node.node_id in result:
                continue
            result[node.node_id] = node
        return result

    # TODO: Old CSV loading logic. Remove/refactor later.
    df = pd.read_csv(path, **kwargs)
    if column_names is not None:
        df = df.rename(columns=column_names)
    payload = df.to_dict(orient="records")
    result = {}
    for row in payload:
        if row["node_id"] in result:
            continue
        node = structure(row, Node)
        result[node.node_id] = node
    return result


def load_edges(path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs) -> dict[EdgeKey, Edge]:
    """
    Load edge capacities from a CSV file. Specify mapping between column names and expected fields if they differ.

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
    # NOTE: Support JSONL format for easier data representation.
    ext = Path(path).suffix.lower()
    if ext == ".jsonl":
        entities = load_entities_jsonl(path, Edge)
        result = {}
        for edge in entities:
            key: EdgeKey = tuple(sorted(edge.edge_id)) # type: ignore
            if key in result:
                continue
            result[key] = edge
        return result

    # TODO: Old CSV loading logic. Remove/refactor later.
    df = pd.read_csv(path, **kwargs)
    if column_names is not None:
        df = df.rename(columns=column_names)
    payload = df.to_dict(orient="records")
    result = {}
    for row in payload:
        # Assume undirected graph topology.
        key: EdgeKey = (str(row["source"]), str(row["target"]))
        key = tuple(sorted(key)) # type: ignore
        if key in result:
            continue
        edge = Edge(
            edge_id=key,
            capacity=float(row["capacity"]),
            used_bandwidth=float(row.get("used_bandwidth", 0)),
        )
        result[key] = edge
    return result


def load_tasks(path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs) -> dict[str, Task]:
    """
    Load task requests from a CSV file. Specify mapping between column names and expected fields if they differ.

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
    # NOTE: Support JSONL format for easier data representation.
    ext = Path(path).suffix.lower()
    if ext == ".jsonl":
        entities = load_entities_jsonl(path, Task)
        result = {}
        for task in entities:
            if task.task_id in result:
                continue
            result[task.task_id] = task
        return result

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
        peer_bandwidths = [float(bw) for bw in str(peer_bandwidths).split(";")] if peer_bandwidths else []
        task = Task(
            task_id=task_id,
            arrival_offset_s=float(row["arrival_offset_s"]),
            duration_s=float(row["duration_s"]),
            initial_cpu=float(row["initial_cpu"]),
            initial_memory=float(row["initial_memory"]),
            peer_bandwidths={t: bw for t, bw in zip(peer_task_ids, peer_bandwidths)}
        )
        result[task.task_id] = task
    return result


def build_task_graph(tasks: dict[str, Task]) -> nx.DiGraph:
    task_graph = nx.DiGraph()
    for t_i, task in tasks.items():
        for t_j, bw in task.peer_bandwidths.items():
            task_graph.add_edge(t_i, t_j, bandwidth=bw)
    return task_graph


def load_entities_jsonl(path: str | Path, cls: type[T]) -> list[T]:
    results = []
    with jsonlines.open(path, mode="r") as reader:
        for obj in reader:
            entity = structure(obj, cls)
            results.append(entity)
    return results
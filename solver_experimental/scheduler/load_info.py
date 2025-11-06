from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence, Tuple
import pandas as pd
import datetime as dt

from entities import Edge, EdgeKey, Node, Task, TaskCommunication


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
    df = pd.read_csv(path, **kwargs)
    if column_names is not None:
        df = df.rename(columns=column_names)
    payload = df.to_dict(orient="records")
    result = {}
    for row in payload:
        node = Node(
            node_id=str(row["node_id"]),
            cpu_capacity=float(row["cpu_capacity"]),
            memory_capacity=float(row["memory_capacity"]),
            used_cpu=float(row.get("used_cpu", 0)),
            used_memory=float(row.get("used_memory", 0)),
        )
        result[node.node_id] = node
    return result


def load_edges(path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs) -> dict[EdgeKey, Edge]:
    """
    Load edge capacities from a CSV file. Specify mapping between column names and expected fields if they differ.

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
    df = pd.read_csv(path, **kwargs)
    if column_names is not None:
        df = df.rename(columns=column_names)
    payload = df.to_dict(orient="records")
    result = {}
    for row in payload:
        key: EdgeKey = (str(row["source"]), str(row["target"]))
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
                "cpu": 10,
                "memory": 50,
                "bandwidth": 20,
                "duration": 3600
            }
        ]
    """
    df = pd.read_csv(path, **kwargs)
    if column_names is not None:
        df = df.rename(columns=column_names)
    payload = df.to_dict(orient="records")
    result = {}
    for row in payload:
        task = Task(
            task_id=str(row["task_id"]),
            cpu=float(row["cpu"]),
            memory=float(row["memory"]),
            duration=row["duration"],
        )
        result[task.task_id] = task
    return result


def load_task_communications(path: str | Path, column_names: Mapping[str, str] | None = None, **kwargs) -> dict[tuple[str, str], TaskCommunication]:
    """
    Load task communication demands from a CSV file. Specify mapping between column names and expected fields if they differ.

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
    df = pd.read_csv(path, **kwargs)
    if column_names is not None:
        df = df.rename(columns=column_names)
    payload = df.to_dict(orient="records")
    result = {}
    for row in payload:
        comm = TaskCommunication(
            source_task_id=str(row["source_task_id"]),
            target_task_id=str(row["target_task_id"]),
            bandwidth=float(row["bandwidth"]),
        )
        result[(comm.source_task_id, comm.target_task_id)] = comm
    return result

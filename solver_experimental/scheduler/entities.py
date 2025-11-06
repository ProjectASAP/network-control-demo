from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
import networkx as nx
import datetime as dt


EdgeKey = Tuple[str, str]


def _normalise_edge(edge: EdgeKey, undirected: bool) -> EdgeKey:
    if undirected:
        return tuple(sorted(edge))  # type: ignore[return-value]
    return edge


class NetworkTopology:
    """Graph representation of the network."""

    def __init__(self, nodes: Iterable[Node], edges: Iterable[Edge], undirected: bool = True) -> None:
        self._graph = nx.Graph() if undirected else nx.DiGraph()
        self.undirected = undirected
        for node in nodes:
            self._graph.add_node(
                node.node_id,
                data=node
            )
        for edge in edges:
            u, v = _normalise_edge(edge.edge_id, undirected)
            if u not in self._graph or v not in self._graph:
                raise ValueError(f"Edge {edge.edge_id} refers to unknown nodes.")
            self._graph.add_edge(u, v, data=edge)

    @property
    def nodes(self) -> dict[str, Node]:
        return {node_id: node["data"] for node_id, node in self._graph.nodes(data=True)}
    
    @property
    def edges(self) -> dict[EdgeKey, Edge]:
        return {_normalise_edge((u, v), self.undirected): edge["data"] for u, v, edge in self._graph.edges(data=True)}

    def get_node(self, node_id: str) -> Node:
        return self._graph.nodes[node_id]["data"]
    
    def get_edge(self, edge_id: EdgeKey) -> Edge:
        return self._graph.edges[edge_id]["data"]

    def has_path(self, source: str, target: str) -> bool:
        return nx.has_path(self._graph, source, target)

    def find_shortest_path(self, source: str, target: str) -> List[str]:
        node_path = nx.shortest_path(self._graph, source, target)
        return node_path


@dataclass
class Node:
    """Capacity descriptor for a compute node."""

    node_id: str
    cpu_capacity: float
    memory_capacity: float
    used_cpu: float = 0.0
    used_memory: float = 0.0


@dataclass
class Edge:
    """Bandwidth descriptor for a network link."""

    edge_id: EdgeKey
    capacity: float
    used_bandwidth: float = 0.0


@dataclass(frozen=True)
class TaskCommunication:
    """Bandwidth demand between two tasks."""
    source_task_id: str
    target_task_id: str
    bandwidth: float


@dataclass
class Task:
    """Specification for a task that needs placement."""

    task_id: str
    arrival_offset_s: float
    duration_s: float
    initial_cpu: float
    initial_memory: float
    peer_bandwidths: dict[str, float] = field(default_factory=dict)


@dataclass
class CommunicationAllocation:
    """Concrete routing choice for a communication requirement."""

    source_task_id: str
    target_task_id: str
    path: Tuple[EdgeKey, ...]
    bandwidth: float


@dataclass
class RunningTask:
    """Final assignment decision for a task."""

    node_id: str
    start_time_s: float
    task: Task
    

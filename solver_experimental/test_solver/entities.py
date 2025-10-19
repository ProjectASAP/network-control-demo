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
        self._graph = nx.Graph()
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
    def nodes(self) -> list[tuple[str, Node]]:
        return list((node_id, node["data"]) for node_id, node in self._graph.nodes(data=True))
    
    @property
    def edges(self) -> list[tuple[EdgeKey, Mapping[str, float]]]:
        return list(((u, v), edge["data"]) for u, v, edge in self._graph.edges(data=True))

    def get_node(self, node_id: str) -> Node:
        return self._graph.nodes[node_id]["data"]
    
    def get_edge(self, edge_id: EdgeKey) -> Edge:
        norm_edge = _normalise_edge(edge_id, isinstance(self._graph, nx.Graph))
        return self._graph.edges[norm_edge]["data"]

    def has_path(self, source: str, target: str) -> bool:
        return nx.has_path(self._graph, source, target)

    def find_shortest_path(self, source: str, target: str) -> List[EdgeKey]:
        node_path = nx.shortest_path(self._graph, source, target)
        edge_path: List[EdgeKey] = []
        for i in range(len(node_path) - 1):
            edge = (node_path[i], node_path[i + 1])
            edge_path.append(_normalise_edge(edge, isinstance(self._graph, nx.Graph)))
        return edge_path


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


@dataclass
class ExistingAssignment:
    """A workload that is already running in the network."""

    task_id: str
    node_id: str
    cpu: float
    memory: float
    bandwidth: float
    path: Sequence[EdgeKey] = field(default_factory=tuple)


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
    cpu: float
    memory: float
    bandwidth: float
    duration: float


@dataclass
class CommunicationAllocation:
    """Concrete routing choice for a communication requirement."""

    source_task_id: str
    target_task_id: str
    path: Tuple[EdgeKey, ...]
    bandwidth: float


@dataclass
class AssignmentDecision:
    """Final decision for a task."""

    task_id: str
    task: Task
    node_id: str


@dataclass
class AssignmentResult:
    """Solver result with the selected assignments."""

    objective_value: float
    decisions: Dict[str, AssignmentDecision]
    unassigned_tasks: List[str]
    moves_used: int
    moved_tasks: List[str]

    def assigned_tasks(self) -> List[str]:
        return list(self.decisions.keys())
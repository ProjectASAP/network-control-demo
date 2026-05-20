from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from .entities import EdgeKey, NetworkTopology, RunningTask, Task

TaskOrder = Literal["input", "arrival", "largest"]
NodeOrder = Literal["id", "available_cpu"]
PathAllocation = tuple[tuple[EdgeKey, ...], float]


@dataclass(frozen=True)
class GreedyAssignmentDecision:
    task_id: str
    node_id: str
    communication_paths: tuple[tuple[EdgeKey, ...], ...] = field(default_factory=tuple)


@dataclass
class GreedyAssignmentResult:
    decisions: dict[str, GreedyAssignmentDecision]
    unassigned_tasks: list[str]
    residual_cpu: dict[str, float]
    residual_memory: dict[str, float]
    residual_bandwidth: dict[EdgeKey, float]

    @property
    def assigned_count(self) -> int:
        return len(self.decisions)


class GreedyTaskScheduler:
    """First-fit task placement baseline using the scheduler JSONL data model."""

    def __init__(
        self,
        network: NetworkTopology,
        *,
        task_order: TaskOrder = "largest",
        node_order: NodeOrder = "available_cpu",
    ) -> None:
        self.network = network
        self.task_order = task_order
        self.node_order = node_order

    def solve(
        self,
        tasks: dict[str, Task],
        running_tasks: dict[str, RunningTask] | None = None,
        *,
        current_time_s: float = 0.0,
    ) -> GreedyAssignmentResult:
        running_tasks = running_tasks or {}

        residual_cpu = {
            node_id: max(node.cpu_capacity - node.used_cpu, 0.0)
            for node_id, node in self.network.nodes.items()
        }
        residual_memory = {
            node_id: max(node.memory_capacity - node.used_memory, 0.0)
            for node_id, node in self.network.nodes.items()
        }
        residual_bandwidth = {
            edge_id: max(edge.capacity - edge.used_bandwidth, 0.0)
            for edge_id, edge in self.network.edges.items()
        }

        decisions: dict[str, GreedyAssignmentDecision] = {}
        assigned_tasks: dict[str, RunningTask] = {}
        for task_id, running_task in running_tasks.items():
            decisions[task_id] = GreedyAssignmentDecision(
                task_id=task_id,
                node_id=running_task.node_id,
            )
            assigned_tasks[task_id] = running_task

        unassigned: list[str] = []
        for task in self._ordered_tasks(tasks):
            if task.task_id in assigned_tasks:
                continue
            placement = self._place_task(
                task,
                tasks,
                assigned_tasks,
                residual_cpu,
                residual_memory,
                residual_bandwidth,
            )
            if placement is None:
                unassigned.append(task.task_id)
                continue

            node_id, path_allocations = placement
            residual_cpu[node_id] -= task.initial_cpu
            residual_memory[node_id] -= task.initial_memory
            for path, bandwidth in path_allocations:
                for edge_id in path:
                    residual_bandwidth[edge_id] -= bandwidth
            assigned_tasks[task.task_id] = RunningTask(
                node_id=node_id,
                start_time_s=current_time_s,
                task=task,
            )
            decisions[task.task_id] = GreedyAssignmentDecision(
                task_id=task.task_id,
                node_id=node_id,
                communication_paths=tuple(path for path, _ in path_allocations),
            )

        return GreedyAssignmentResult(
            decisions=decisions,
            unassigned_tasks=unassigned,
            residual_cpu=residual_cpu,
            residual_memory=residual_memory,
            residual_bandwidth=residual_bandwidth,
        )

    def _ordered_tasks(self, tasks: dict[str, Task]) -> list[Task]:
        ordered = list(tasks.values())
        if self.task_order == "input":
            return ordered
        if self.task_order == "arrival":
            return sorted(ordered, key=lambda task: (task.arrival_offset_s, task.task_id))
        return sorted(
            ordered,
            key=lambda task: (
                -(task.initial_cpu + task.initial_memory + sum(task.peer_bandwidths.values())),
                task.arrival_offset_s,
                task.task_id,
            ),
        )

    def _ordered_nodes(self, residual_cpu: dict[str, float]) -> list[str]:
        node_ids = list(self.network.nodes.keys())
        if self.node_order == "id":
            return sorted(node_ids)
        return sorted(node_ids, key=lambda node_id: (-residual_cpu[node_id], node_id))

    def _place_task(
        self,
        task: Task,
        all_tasks: dict[str, Task],
        assigned_tasks: dict[str, RunningTask],
        residual_cpu: dict[str, float],
        residual_memory: dict[str, float],
        residual_bandwidth: dict[EdgeKey, float],
    ) -> tuple[str, list[PathAllocation]] | None:
        for node_id in self._ordered_nodes(residual_cpu):
            if residual_cpu[node_id] < task.initial_cpu:
                continue
            if residual_memory[node_id] < task.initial_memory:
                continue

            path_allocations: list[PathAllocation] = []
            path_usage: dict[EdgeKey, float] = {}
            feasible = True
            for peer_id, peer_bandwidth in self._assigned_peer_bandwidths(
                task, all_tasks, assigned_tasks
            ):
                peer_node = assigned_tasks[peer_id].node_id
                if peer_node == node_id:
                    continue
                if not self.network.has_path(node_id, peer_node):
                    feasible = False
                    break
                edge_path = self._edge_path(
                    self.network.find_shortest_path(node_id, peer_node)
                )
                for edge_id in edge_path:
                    path_usage[edge_id] = path_usage.get(edge_id, 0.0) + peer_bandwidth
                    if path_usage[edge_id] > residual_bandwidth.get(edge_id, 0.0):
                        feasible = False
                        break
                if not feasible:
                    break
                path_allocations.append((tuple(edge_path), peer_bandwidth))

            if feasible:
                return node_id, path_allocations
        return None

    def _assigned_peer_bandwidths(
        self,
        task: Task,
        all_tasks: dict[str, Task],
        assigned_tasks: dict[str, RunningTask],
    ) -> list[tuple[str, float]]:
        peer_bandwidths: dict[str, float] = {}
        for peer_id, bandwidth in task.peer_bandwidths.items():
            if peer_id in assigned_tasks:
                peer_bandwidths[peer_id] = peer_bandwidths.get(peer_id, 0.0) + bandwidth
        for peer_id in assigned_tasks:
            peer_task = all_tasks.get(peer_id) or assigned_tasks[peer_id].task
            bandwidth = peer_task.peer_bandwidths.get(task.task_id)
            if bandwidth is not None:
                peer_bandwidths[peer_id] = peer_bandwidths.get(peer_id, 0.0) + bandwidth
        return list(peer_bandwidths.items())

    def _edge_path(self, node_path: list[str]) -> list[EdgeKey]:
        edge_path: list[EdgeKey] = []
        for index in range(len(node_path) - 1):
            edge_path.append(tuple(sorted((node_path[index], node_path[index + 1]))))  # type: ignore[arg-type]
        return edge_path


"""
Network controller task assignment solver built on top of OR-Tools.

This implementation formulates the placement problem as a mixed-integer program
with the following elements:

Inputs
------
* Node capacities (CPU, memory) and existing utilisation.
* Edge capacities (bandwidth) and existing utilisation.
* Tasks with resource requirements, communication demands, optional placement
  domains (`allowed_nodes`), priorities, and historical placements.
* Existing task assignments whose resource consumption must remain honoured.
* Optional cap on the number of task migrations between scheduling epochs.

Decision Variables
------------------
* `x[t, n] ∈ {0,1}` — whether task `t` runs on node `n`.
* `s_t ∈ {0,1}` — whether task `t` is skipped (i.e., not assigned).
* `move_t ∈ {0,1}` — whether task `t` moves away from its previous node.
* `z_c ∈ {0,1}` — whether communication demand `c` is active (both tasks placed).
* `f[c, e] ≥ 0` — bandwidth routed for communication `c` over directed edge `e`.

Constraints
-----------
1. Task placement domain: each task is either assigned to exactly one eligible
   node or skipped (`Σ_n x[t, n] + s_t = 1`).
2. Node capacity: the total CPU/memory consumed by placed tasks never exceeds
   the available capacity after accounting for existing workloads.
3. Link capacity: the sum of flow routed over each physical link respects the
   remaining bandwidth budget.
4. Flow conservation: for every communication demand, the routed flow forms a
   feasible path between the selected source and destination nodes.
5. Migration budget: the number of tasks that move away from their previous
   placements is bounded by `max_task_movements` (when provided).

Objective
---------
Maximise the total priority of assigned tasks.
"""

from __future__ import annotations

from collections import deque
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from ortools.linear_solver import pywraplp

EdgeKey = Tuple[str, str]


def _normalise_edge(edge: EdgeKey, undirected: bool) -> EdgeKey:
    if undirected:
        return tuple(sorted(edge))  # type: ignore[return-value]
    return edge


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

    target_task_id: str
    bandwidth: float


@dataclass
class Task:
    """Specification for a task that needs placement."""

    task_id: str
    cpu: float
    memory: float
    bandwidth: float
    priority: float = 1.0
    communications: Sequence[TaskCommunication] = field(default_factory=tuple)
    allowed_nodes: Optional[Sequence[str]] = None

    def __post_init__(self) -> None:
        if not isinstance(self.communications, tuple):
            object.__setattr__(self, "communications", tuple(self.communications))
        if self.allowed_nodes is not None and not isinstance(self.allowed_nodes, tuple):
            object.__setattr__(self, "allowed_nodes", tuple(self.allowed_nodes))

    def has_feasible_domain(self) -> bool:
        return self.allowed_nodes is None or bool(self.allowed_nodes)


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
    node_id: str
    communication_paths: Tuple[CommunicationAllocation, ...]


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


class NetworkControllerSolver:
    """OR-Tools based solver for network task placement."""

    SUPPORTED_BACKENDS = ("CBC", "SCIP", "GLPK")

    def __init__(
        self,
        nodes: Mapping[str, Node],
        edges: Mapping[EdgeKey, Edge],
        *,
        undirected: bool = True,
        solver_backend: str = "CBC",
    ) -> None:
        if not nodes:
            raise ValueError("At least one node must be provided")
        if not edges:
            raise ValueError("At least one network edge must be provided")
        solver_backend = solver_backend.upper()
        if solver_backend not in self.SUPPORTED_BACKENDS:
            raise ValueError(
                f"Unsupported solver backend {solver_backend!r}. "
                f"Supported: {self.SUPPORTED_BACKENDS}"
            )
        self._solver_backend = solver_backend

        self._undirected = undirected
        self._nodes: Dict[str, Node] = {
            node_id: Node(
                node_id=node.node_id,
                cpu_capacity=node.cpu_capacity,
                memory_capacity=node.memory_capacity,
                used_cpu=node.used_cpu,
                used_memory=node.used_memory,
            )
            for node_id, node in nodes.items()
        }
        self._edges: Dict[EdgeKey, Edge] = {}
        for raw_key, edge in edges.items():
            key = _normalise_edge(raw_key, undirected)
            if key in self._edges:
                raise ValueError(f"Duplicate edge detected for key {key}")
            self._edges[key] = Edge(
                edge_id=key,
                capacity=edge.capacity,
                used_bandwidth=edge.used_bandwidth,
            )

        self._node_ids: Tuple[str, ...] = tuple(self._nodes.keys())
        self._directed_arcs: List[EdgeKey] = []
        self._out_arcs: Dict[str, List[EdgeKey]] = {
            node_id: [] for node_id in self._node_ids
        }
        self._in_arcs: Dict[str, List[EdgeKey]] = {
            node_id: [] for node_id in self._node_ids
        }
        for (u, v), edge in self._edges.items():
            if u not in self._nodes or v not in self._nodes:
                raise ValueError(f"Edge {edge.edge_id} references unknown nodes")
            self._directed_arcs.append((u, v))
            self._out_arcs[u].append((u, v))
            self._in_arcs[v].append((u, v))
            if undirected:
                self._directed_arcs.append((v, u))
                self._out_arcs[v].append((v, u))
                self._in_arcs[u].append((v, u))

    # --------------------------------------------------------------------- #
    # Public API
    # --------------------------------------------------------------------- #
    def solve(
        self,
        tasks: Sequence[Task],
        existing_assignments: Optional[Sequence[ExistingAssignment]] = None,
        *,
        previous_assignments: Optional[Mapping[str, str]] = None,
        max_task_movements: Optional[int] = None,
        ilp_output_path: Optional[str | Path] = None,
    ) -> AssignmentResult:
        """
        Optimise task placement subject to resource and migration constraints.

        Parameters
        ----------
        tasks:
            Workload specifications (resource requirements, communications,
            optional node domains, priorities).
        existing_assignments:
            Placements that are already active and must be preserved when
            evaluating resource availability.
        previous_assignments:
            Mapping from task_id to node_id describing last epoch's placements.
            Used to measure task movements.
        max_task_movements:
            Optional upper bound on the number of tasks allowed to move.
        ilp_output_path:
            Optional filesystem path where the generated ILP formulation should
            be saved in LP format before solving.

        Returns
        -------
        AssignmentResult containing the objective value, chosen placements,
        migration statistics, and list of unassigned tasks.
        """
        if max_task_movements is not None and max_task_movements < 0:
            raise ValueError("max_task_movements must be greater than or equal to zero")

        residual_cpu: Dict[str, float] = {
            node_id: node.cpu_capacity - node.used_cpu
            for node_id, node in self._nodes.items()
        }
        residual_memory: Dict[str, float] = {
            node_id: node.memory_capacity - node.used_memory
            for node_id, node in self._nodes.items()
        }
        residual_bandwidth: Dict[EdgeKey, float] = {
            edge_id: edge.capacity - edge.used_bandwidth
            for edge_id, edge in self._edges.items()
        }

        if existing_assignments:
            for assignment in existing_assignments:
                if assignment.node_id not in self._nodes:
                    raise KeyError(
                        f"Existing assignment references unknown node {assignment.node_id}"
                    )
                residual_cpu[assignment.node_id] -= assignment.cpu
                residual_memory[assignment.node_id] -= assignment.memory
                for raw_edge in assignment.path:
                    edge_key = _normalise_edge(raw_edge, self._undirected)
                    if edge_key not in residual_bandwidth:
                        raise KeyError(
                            f"Existing assignment references unknown edge {raw_edge}"
                        )
                    residual_bandwidth[edge_key] -= assignment.bandwidth

        for node_id, remaining in residual_cpu.items():
            if remaining < -1e-9:
                raise ValueError(
                    f"Node {node_id} is over-subscribed before optimisation (CPU)."
                )
        for node_id, remaining in residual_memory.items():
            if remaining < -1e-9:
                raise ValueError(
                    f"Node {node_id} is over-subscribed before optimisation (memory)."
                )
        for edge_id, remaining in residual_bandwidth.items():
            if remaining < -1e-9:
                raise ValueError(
                    f"Edge {edge_id} is over-subscribed before optimisation (bandwidth)."
                )

        solver = pywraplp.Solver.CreateSolver(self._solver_backend)
        if solver is None:
            raise RuntimeError(
                f"Failed to initialise OR-Tools {self._solver_backend} solver. "
                f"Ensure the {self._solver_backend} backend is installed."
            )

        x_vars: Dict[str, Dict[str, pywraplp.Variable]] = {}
        skip_vars: Dict[str, pywraplp.Variable] = {}
        assign_sums: Dict[str, pywraplp.LinearExpr] = {}

        for task in tasks:
            if not task.has_feasible_domain():
                continue
            x_vars[task.task_id] = {}
            allowed = set(task.allowed_nodes or self._node_ids)
            for node_id in self._node_ids:
                var = solver.BoolVar(f"x_{task.task_id}_{node_id}")
                x_vars[task.task_id][node_id] = var
                if node_id not in allowed:
                    solver.Add(var == 0)
            skip_var = solver.BoolVar(f"skip_{task.task_id}")
            skip_vars[task.task_id] = skip_var
            assign_sum = solver.Sum(x_vars[task.task_id].values())
            assign_sums[task.task_id] = assign_sum
            solver.Add(assign_sum + skip_var == 1)

        active_tasks = [task for task in tasks if task.task_id in assign_sums]

        for node_id in self._node_ids:
            cpu_expr = solver.Sum(
                task.cpu * x_vars[task.task_id][node_id] for task in active_tasks
            )
            mem_expr = solver.Sum(
                task.memory * x_vars[task.task_id][node_id] for task in active_tasks
            )
            solver.Add(cpu_expr <= residual_cpu[node_id])
            solver.Add(mem_expr <= residual_memory[node_id])

        communications: List[Tuple[int, Task, TaskCommunication]] = []
        for task in active_tasks:
            for idx, comm in enumerate(task.communications):
                if comm.bandwidth <= 0:
                    continue
                communications.append((len(communications), task, comm))

        flow_vars: Dict[int, Dict[EdgeKey, pywraplp.Variable]] = {}
        z_vars: Dict[int, pywraplp.Variable] = {}
        src_indicator: Dict[int, Dict[str, pywraplp.Variable]] = {}
        dst_indicator: Dict[int, Dict[str, pywraplp.Variable]] = {}

        for comm_id, source_task, comm in communications:
            if comm.target_task_id not in x_vars:
                # Target task cannot be assigned => communication inactive.
                continue
            target_task = next(
                task for task in active_tasks if task.task_id == comm.target_task_id
            )
            z_var = solver.BoolVar(f"z_{source_task.task_id}_{comm.target_task_id}")
            z_vars[comm_id] = z_var

            flow_vars[comm_id] = {}
            for arc in self._directed_arcs:
                flow = solver.NumVar(
                    0.0, solver.infinity(), f"f_{comm_id}_{arc[0]}_{arc[1]}"
                )
                solver.Add(flow <= comm.bandwidth * z_var)
                flow_vars[comm_id][arc] = flow

            source_assign = assign_sums[source_task.task_id]
            target_assign = assign_sums[target_task.task_id]
            solver.Add(z_var <= source_assign)
            solver.Add(z_var <= target_assign)
            solver.Add(z_var >= source_assign + target_assign - 1)

            src_indicator[comm_id] = {}
            dst_indicator[comm_id] = {}

            for node_id in self._node_ids:
                src_var = solver.BoolVar(f"src_{comm_id}_{node_id}")
                dst_var = solver.BoolVar(f"dst_{comm_id}_{node_id}")
                src_indicator[comm_id][node_id] = src_var
                dst_indicator[comm_id][node_id] = dst_var

                solver.Add(src_var <= z_var)
                solver.Add(src_var <= x_vars[source_task.task_id][node_id])
                solver.Add(src_var >= x_vars[source_task.task_id][node_id] + z_var - 1)

                solver.Add(dst_var <= z_var)
                solver.Add(dst_var <= x_vars[target_task.task_id][node_id])
                solver.Add(dst_var >= x_vars[target_task.task_id][node_id] + z_var - 1)

                out_sum = solver.Sum(
                    flow_vars[comm_id][arc] for arc in self._out_arcs[node_id]
                )
                in_sum = solver.Sum(
                    flow_vars[comm_id][arc] for arc in self._in_arcs[node_id]
                )
                solver.Add(out_sum - in_sum == comm.bandwidth * (src_var - dst_var))

        for edge_key, residual in residual_bandwidth.items():
            if residual < 0:
                residual = 0.0
            u, v = edge_key
            arc_uv = (u, v)
            arc_vu = (v, u)
            flow_sum = []
            for comm_id in flow_vars:
                if arc_uv in flow_vars[comm_id]:
                    flow_sum.append(flow_vars[comm_id][arc_uv])
                if arc_vu in flow_vars[comm_id]:
                    flow_sum.append(flow_vars[comm_id][arc_vu])
            if flow_sum:
                solver.Add(solver.Sum(flow_sum) <= residual)

        previous_assignments = previous_assignments or {}
        move_vars: Dict[str, pywraplp.Variable] = {}
        total_moves: List[pywraplp.Variable] = []

        for task in active_tasks:
            move_var = solver.BoolVar(f"move_{task.task_id}")
            move_vars[task.task_id] = move_var
            total_moves.append(move_var)
            prev_node = previous_assignments.get(task.task_id)
            if prev_node is None:
                solver.Add(move_var == 0)
                continue
            if prev_node not in self._node_ids:
                raise KeyError(
                    f"Previous assignment references unknown node {prev_node}"
                )
            stay_var = solver.BoolVar(f"stay_{task.task_id}")
            solver.Add(stay_var <= x_vars[task.task_id][prev_node])
            solver.Add(stay_var <= assign_sums[task.task_id])
            solver.Add(
                stay_var
                >= x_vars[task.task_id][prev_node] + assign_sums[task.task_id] - 1
            )
            solver.Add(move_var + stay_var == 1)

        if max_task_movements is not None:
            solver.Add(solver.Sum(total_moves) <= max_task_movements)

        objective = solver.Objective()
        for task in active_tasks:
            for var in x_vars[task.task_id].values():
                objective.SetCoefficient(var, task.priority)
        objective.SetMaximization()

        if ilp_output_path is not None:
            lp_content = solver.ExportModelAsLpFormat(False)
            lp_path = Path(ilp_output_path)
            lp_path.parent.mkdir(parents=True, exist_ok=True)
            lp_path.write_text(lp_content)

        status = solver.Solve()
        if status not in (pywraplp.Solver.OPTIMAL, pywraplp.Solver.FEASIBLE):
            raise RuntimeError("No feasible solution found for the given inputs.")

        assigned_nodes: Dict[str, str] = {}
        unassigned_tasks: List[str] = []
        for task in active_tasks:
            assigned_node = None
            for node_id, var in x_vars[task.task_id].items():
                if var.solution_value() > 0.5:
                    assigned_node = node_id
                    break
            if assigned_node:
                assigned_nodes[task.task_id] = assigned_node
            else:
                unassigned_tasks.append(task.task_id)

        communication_allocations: Dict[str, List[CommunicationAllocation]] = {
            task.task_id: [] for task in active_tasks if task.task_id in assigned_nodes
        }

        for comm_id, source_task, comm in communications:
            if comm_id not in z_vars:
                continue
            if z_vars[comm_id].solution_value() < 0.5:
                continue
            if source_task.task_id not in assigned_nodes:
                continue
            if comm.target_task_id not in assigned_nodes:
                continue

            source_node = assigned_nodes[source_task.task_id]
            target_node = assigned_nodes[comm.target_task_id]

            flows = {
                arc: var.solution_value()
                for arc, var in flow_vars[comm_id].items()
                if var.solution_value() > 1e-8
            }
            paths = self._extract_paths(flows, source_node, target_node, comm.bandwidth)
            for path_edges, bandwidth in paths:
                allocation = CommunicationAllocation(
                    source_task_id=source_task.task_id,
                    target_task_id=comm.target_task_id,
                    path=tuple(path_edges),
                    bandwidth=bandwidth,
                )
                if source_task.task_id in communication_allocations:
                    communication_allocations[source_task.task_id].append(allocation)
                if comm.target_task_id in communication_allocations:
                    communication_allocations[comm.target_task_id].append(allocation)

        decisions: Dict[str, AssignmentDecision] = {}
        for task_id, node_id in assigned_nodes.items():
            decisions[task_id] = AssignmentDecision(
                task_id=task_id,
                node_id=node_id,
                communication_paths=tuple(communication_allocations.get(task_id, [])),
            )

        moves_used = int(
            round(sum(move_vars[task_id].solution_value() for task_id in move_vars))
        )
        moved_tasks = [
            task_id for task_id, var in move_vars.items() if var.solution_value() > 0.5
        ]

        objective_value = solver.Objective().Value()

        return AssignmentResult(
            objective_value=objective_value,
            decisions=decisions,
            unassigned_tasks=unassigned_tasks,
            moves_used=moves_used,
            moved_tasks=moved_tasks,
        )

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _extract_paths(
        self,
        flows: Mapping[EdgeKey, float],
        source_node: str,
        target_node: str,
        bandwidth: float,
        *,
        tolerance: float = 1e-8,
    ) -> List[Tuple[List[EdgeKey], float]]:
        """Decompose edge flows into simple source-to-target paths."""
        residual = dict(flows)
        paths: List[Tuple[List[EdgeKey], float]] = []

        while True:
            path = self._find_positive_path(
                residual, source_node, target_node, tolerance
            )
            if not path:
                break
            path_flow = min(residual[arc] for arc in path)
            for arc in path:
                residual[arc] -= path_flow
            paths.append((path, path_flow))

        total_flow = sum(b for _, b in paths)
        if total_flow < bandwidth - 1e-6:
            # If numerical issues prevented recovering the full flow,
            # attribute the remainder to a direct (source, target) hop when possible.
            remaining = bandwidth - total_flow
            direct_arc = (source_node, target_node)
            if direct_arc in flows and flows[direct_arc] > tolerance:
                paths.append(([direct_arc], remaining))
        return paths

    def _find_positive_path(
        self,
        residual: Mapping[EdgeKey, float],
        source: str,
        target: str,
        tolerance: float,
    ) -> Optional[List[EdgeKey]]:
        """Find any path from source to target following arcs with positive residual flow."""
        queue: deque[str] = deque([source])
        parents: Dict[str, Optional[str]] = {source: None}
        parent_arc: Dict[str, EdgeKey] = {}

        while queue:
            node = queue.popleft()
            if node == target:
                break
            for arc in self._out_arcs[node]:
                if residual.get(arc, 0.0) <= tolerance:
                    continue
                neighbour = arc[1]
                if neighbour in parents:
                    continue
                parents[neighbour] = node
                parent_arc[neighbour] = arc
                queue.append(neighbour)

        if target not in parents:
            return None

        path: List[EdgeKey] = []
        node = target
        while parents[node] is not None:
            arc = parent_arc[node]
            path.append(arc)
            node = parents[node]
        path.reverse()
        return path


def build_nodes(
    capacities: Mapping[str, Mapping[str, float]],
) -> Dict[str, Node]:
    """Helper to build node descriptors from a nested mapping."""
    result: Dict[str, Node] = {}
    for node_id, spec in capacities.items():
        result[node_id] = Node(
            node_id=node_id,
            cpu_capacity=spec["cpu"],
            memory_capacity=spec["memory"],
            used_cpu=spec.get("used_cpu", 0.0),
            used_memory=spec.get("used_memory", 0.0),
        )
    return result


def build_edges(
    capacities: Mapping[EdgeKey, Mapping[str, float]],
) -> Dict[EdgeKey, Edge]:
    """Helper to build edge descriptors from a nested mapping."""
    result: Dict[EdgeKey, Edge] = {}
    for edge, spec in capacities.items():
        result[edge] = Edge(
            edge_id=edge,
            capacity=spec["capacity"],
            used_bandwidth=spec.get("used", 0.0),
        )
    return result


__all__ = [
    "AssignmentDecision",
    "AssignmentResult",
    "CommunicationAllocation",
    "Edge",
    "EdgeKey",
    "ExistingAssignment",
    "NetworkControllerSolver",
    "Node",
    "Task",
    "TaskCommunication",
    "build_edges",
    "build_nodes",
]

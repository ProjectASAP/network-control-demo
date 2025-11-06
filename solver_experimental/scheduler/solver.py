import pulp as plp
from typing import Dict, List, Tuple, Set
import time
from collections import defaultdict
from .entities import *
from .load_info import build_task_graph
import networkx as nx


def get_valid_task_graph(tasks: dict[str, Task], task_graph: nx.DiGraph):
        """
        Given some current tasks, filters out tasks that are invalid (cannot be scheduled), and returns a dictionary mapping between
        valid task pairs and their bandwidth requirements. A task cannot be scheduled unless all its peer tasks are present.

        Returns:
            task_communication: {(t_i, t_j): bandwidth (float)} - t_i, t_j represents task ids
            valid_tasks: {task_id: Task} - Tasks that can be scheduled.
        """
        weak_comps = nx.weakly_connected_components(task_graph)
        valid_tasks = tasks.copy()
        for comp in weak_comps:
            for t in comp:
                if t not in tasks:
                    for t in comp:
                        valid_tasks.pop(t)
                    break
        task_communication = {(t_i, t_j): bw for t_i, t_j, bw in task_graph.edges.data('bandwidth') if t_i in valid_tasks and t_j in valid_tasks}
        return task_communication, valid_tasks


class TaskScheduler:

    def __init__(self, network: NetworkTopology, reassignment_penalty: float = 1.0, max_reassignments: int = 5,):
        """
        Initialize the task scheduler.
        
        Args:
            network: Graph representation of network.
        """
        self.network = network
        self.nodes = network.nodes
        self.edges = network.edges

        self.reassignment_penalty = reassignment_penalty
        self.max_reassignments = max_reassignments
    
    def solve(self, 
              tasks: dict[str, Task],
              running_tasks: dict[str, RunningTask],
              paths: dict[tuple[str, str], List[str]],
              task_graph: nx.DiGraph,
              time_limit: int = 300) -> Tuple[Dict[str, RunningTask], dict[str, Task], float | None, int]:
        """
        Solve the task scheduling optimization problem.
        
        Args:
            tasks: {task_id: Task} - Mapping between task id and associated Task spec.
            task_communication: {(task_i, task_j): TaskCommunication} - Mapping between (src, dest) task pair and bandwidth requirements.
            running_tasks: {task_id: RunningTask} - Map between task id and running task information.
            paths: {(node1, node2): [[path_nodes_1], [path_nodes_2], ...]} - Routing paths between node pairs, with each path represented as a sequence of node ids.
            time_limit: Solver time limit in seconds
            
        Returns:
            (assignment, leftover_tasks, objective_value, status_code) - New assignment dict, objective value, and solver status code.
        """
        prob = plp.LpProblem("Task_Scheduling", plp.LpMinimize)

        # Include currently running tasks in optimization.
        tasks = tasks | {t: rt.task for t, rt in running_tasks.items()}

        # Get task communication requirements and filter out tasks whose peers are not here as well.
        task_communication, tasks = get_valid_task_graph(tasks, task_graph=task_graph)
        leftover_tasks = {task_id: tasks[task_id] for task_id in tasks.keys() - tasks.keys()}

        # Decision variables
        # d[t][n] = 1 if task t assigned to node n, 0 otherwise
        d = {t: {n: plp.LpVariable(f"d_{t}_{n}", cat='Binary') 
                 for n in self.nodes} for t in tasks}
        
        # Auxiliary variable for task allocation tracking. Is 1 if task t has an assignment to any node (0 otherwise).
        allocated = {t: plp.LpVariable(f"allocated_{t}", cat='Binary') 
                     for t in tasks}
        
        # For a given task t (from a previous epoch), we sum over all d[t][n] for all n besides the node t was assigned originally.
        # Since only one d[t][n] can equal 1 (see constraint 1) for a given t, this sum is 1 if t is reassigned. Sum over all such tasks, and
        # we get the total reassignments.
        reassignments = plp.lpSum(
            d[t][n] for t in tasks for n in self.nodes.keys()
            if t in running_tasks and running_tasks[t].node_id != n
        )
        total_allocated = plp.lpSum(allocated[t] for t in tasks)
        
        # Objective: Minimize reassignments, Maximize allocations.
        prob += -total_allocated + self.reassignment_penalty * reassignments
        
        prob += reassignments <= self.max_reassignments
        
        # Constraints
        # 1. Each task assigned to exactly one node if allocated
        for t in tasks:
            prob += plp.lpSum(d[t][n] for n in self.nodes) == allocated[t]
        
        # 2. Node resource capacity constraints
        for n, node in self.nodes.items():
            # CPU.
            prob += plp.lpSum(
                tasks[t].initial_cpu * d[t][n] 
                for t in tasks.keys()
            ) <= node.cpu_capacity
            # Memory.
            prob += plp.lpSum(
                tasks[t].initial_memory * d[t][n] 
                for t in tasks.keys()
            ) <= node.memory_capacity

        # 3. Construct path bandwidth usage variables. For each node pair, compute the bandwidth used on each path between that node pair.
        used_path_bandwidths = {}
        choose_path_constraints = {(t_i, t_j): 0 for t_i, t_j in task_communication.keys()}
        for n_i, n_j in paths.keys():
            used_path_bandwidths[(n_i, n_j)] = {}
            pair_bandwidth = used_path_bandwidths[(n_i, n_j)]

            paths_btwn = paths.get((n_i, n_j), paths.get((n_j, n_i), []))
            if not paths_btwn:
                continue

            # TODO: Right now optimization formulation doesn't support multiple paths, so there is only 1 path in paths_btwn.
            for k in range(len(paths_btwn)):
                pair_bandwidth[k] = 0
                for (t_i, t_j), bandwidth in task_communication.items():

                    # The following constraints are for mimicking the behavior of logical AND (i.e. z_ij = d[t_i][n_i] * d[t_j][n_j]).
                    # The result is a binary variable that represents whether we assigned t_i -> n_i and t_j -> n_j.
                    z_ij = plp.LpVariable(f"z_{t_i}_{t_j}_{n_i}_{n_j}", cat='Binary')
                    prob += z_ij <= d[t_i][n_i]
                    prob += z_ij <= d[t_j][n_j]
                    prob += z_ij >= (d[t_i][n_i] + d[t_j][n_j] - 1)

                    # Binary variable representing whether we assigned t_i -> n_j and t_j -> n_i (other way around from above).
                    z_ji = plp.LpVariable(f"z_{t_i}_{t_j}_{n_j}_{n_i}", cat='Binary')
                    prob += z_ji <= d[t_i][n_j]
                    prob += z_ji <= d[t_j][n_i]
                    prob += z_ji >= (d[t_i][n_j] + d[t_j][n_i] - 1)

                    # Adding z_ij and z_ji together is a binary variable that indicates whether (t_i, t_j) were assigned to (n_i, n_j) in any order.
                    # Alternatively, it indicates whether (t_i, t_j) is using the path between (n_i, n_j).
                    pair_bandwidth[k] += bandwidth * (z_ij + z_ji)
                    choose_path_constraints[(t_i, t_j)] += z_ij + z_ji

        # Handle task pairs assigned to the same node. Assume no bandwidth cost.
        for n in self.nodes:
            for t_i, t_j in task_communication.keys():
                # Whether task pair is assigned on the same node. z = 1 iff both d_i and d_j equal 1.
                z_ii = plp.LpVariable(f"z_{t_i}_{t_j}_{n}_{n}", cat='Binary')
                prob += z_ii <= d[t_i][n]
                prob += z_ii <= d[t_j][n]
                prob += z_ii >= (d[t_i][n] + d[t_j][n] - 1)
                # Treat assignment to same node as a "path" option as well.
                choose_path_constraints[(t_i, t_j)] += z_ii
                    
        # Force communicating task pairs to use exactly one path (or assign both to the same node if not possible). 
        # Note that we do not have to choose a path if neither task is assigned.
        for t_i, t_j in task_communication.keys():
            # These constraints mimick logical OR. The binary variable indicates whether either t_i or t_j is allocated anywhere.
            allocated_ij = plp.LpVariable(f"allocated_{t_i}_{t_j}", cat='Binary')
            prob += allocated_ij >= allocated[t_i]
            prob += allocated_ij >= allocated[t_j]
            prob += allocated_ij <= (allocated[t_i] + allocated[t_j])

            # If any of one of the tasks in the pair are allocated (allocated_ij = 1), we must choose a path to communicate over, which will force the other task to be allocated
            # somewhere as well (at least one of the z_ij's or z_ji's for a given t_i, t_j must equal 1).
            prob += choose_path_constraints[(t_i, t_j)] == allocated_ij

        # 4. Edge bandwidth constraints. Each edge's total bandwidth used by all task pairs <= edge capacity.
        edge_constraints = {}
        for n_i, n_j in used_path_bandwidths:
            paths_btwn = paths.get((n_i, n_j), paths.get((n_j, n_i), []))
            for k, path in enumerate(paths_btwn):
                # Extract edges on this path
                path_edges = []
                for idx in range(len(path) - 1):
                    edge_key: EdgeKey = (path[idx], path[idx+1]) if path[idx] < path[idx+1] else (path[idx+1], path[idx])
                    path_edges.append(edge_key)
                # For each edge, add the bandwidth used by all paths that cross this edge.
                for edge_key in path_edges:
                    if edge_key not in edge_constraints:
                        edge_constraints[edge_key] = 0
                    edge_constraints[edge_key] += used_path_bandwidths[(n_i, n_j)][k]
        # Total used bandwidth should not exceed edge capacity.
        for edge_key, total_bandwidth in edge_constraints.items():
            prob += total_bandwidth <= self.edges[edge_key].capacity
        
        # Solve
        solver = plp.PULP_CBC_CMD(timeLimit=time_limit, msg=0)
        prob.solve(solver)
        
        # Extract solution
        assignments = {}
        status_code = prob.status
        objective_value = None
        if plp.LpStatus[status_code] == 'Optimal':
            objective_value = plp.value(prob.objective)
            for t, task in tasks.items():
                for n in self.nodes:
                    if plp.value(d[t][n]) == 1:
                        assigned_task = RunningTask(node_id=n, start_time_s=time.time(), task=task)
                        assignments[t] = assigned_task
                        break
                else:
                    leftover_tasks[t] = task
        return assignments, leftover_tasks, objective_value, status_code

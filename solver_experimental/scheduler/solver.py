import pulp as plp
from typing import Dict, List, Tuple, Set
from collections import defaultdict
from itertools import combinations
from entities import *

class TaskScheduler:

    def __init__(self, network: NetworkTopology, reassignment_penalty: float = 1.0):
        """
        Initialize the task scheduler.
        
        Args:
            network: Graph representation of network.
        """
        self.network = network
        self.nodes = network.nodes
        self.edges = network.edges

        self.reassignment_penalty = reassignment_penalty
    
    def solve(self, 
              tasks: dict[str, Task],
              task_communication: Dict[Tuple[str, str], TaskCommunication],
              running_tasks: dict[str, RunningTask],
              paths: Dict[Tuple[str, str], List[str]],
              time_limit: int = 300) -> Tuple[Dict, float | None, int]:
        """
        Solve the task scheduling optimization problem.
        
        Args:
            tasks: {task_id: Task} - Mapping between task id and associated Task spec.
            task_communication: {(task_i, task_j): TaskCommunication} - Mapping between (src, dest) task pair and bandwidth requirements.
            running_tasks: {task_id: RunningTask} - Map between task id and running task information.
            paths: {(node1, node2): [[path_nodes_1], [path_nodes_2], ...]} - Routing paths between node pairs, with each path represented as a sequence of node ids.
            time_limit: Solver time limit in seconds
            
        Returns:
            (assignment, objective_value, status_code) - New assignment dict, objective value, and solver status code.
        """
        prob = plp.LpProblem("Task_Scheduling", plp.LpMinimize)

        # Include currently running tasks in optimization.
        tasks = tasks | {t: rt.task for t, rt in running_tasks.items()}

        # Decision variables
        # d[t][n] = 1 if task t assigned to node n, 0 otherwise
        d = {t: {n: plp.LpVariable(f"d_{t}_{n}", cat='Binary') 
                 for n in self.nodes} for t in tasks}
        
        # Auxiliary variable for task allocation tracking
        allocated = {t: plp.LpVariable(f"allocated_{t}", cat='Binary') 
                     for t in tasks}
        
        # Objective: Minimize reassignments, Maximize allocations
        reassignments = plp.lpSum(
            d[t][n] for t in tasks for n in self.nodes.keys()
            if t in running_tasks and running_tasks[t].node_id != n
        )
        total_allocated = plp.lpSum(allocated[t] for t in tasks)
        
        prob += -total_allocated + self.reassignment_penalty * reassignments
        
        # Constraints
        # 1. Each task assigned to exactly one node if allocated
        for t in tasks:
            prob += plp.lpSum(d[t][n] for n in self.nodes) == allocated[t]
        
        # 2. Node resource capacity constraints
        for n, node in self.nodes.items():
            # CPU.
            prob += plp.lpSum(
                tasks[t].cpu * d[t][n] 
                for t in tasks.keys()
            ) <= node.cpu_capacity
            # Memory.
            prob += plp.lpSum(
                tasks[t].memory * d[t][n] 
                for t in tasks.keys()
            ) <= node.memory_capacity

        # 3. Construct path bandwidth usage variables. For each node pair, compute the bandwidth used on each path between that node pair.
        path_bandwidths = {}
        choose_path_constraints = {(t_i, t_j): 0 for t_i, t_j in task_communication.keys()}
        for n_i, n_j in combinations(self.nodes, 2):
            path_bandwidths[(n_i, n_j)] = {}
            pair_bandwidth = path_bandwidths[(n_i, n_j)]

            paths_btwn = paths.get((n_i, n_j), paths.get((n_j, n_i), []))
            if not paths_btwn:
                continue

            for k, path in enumerate(paths_btwn):
                pair_bandwidth[k] = 0
                for (t_i, t_j), task_comm in task_communication.items():

                    # Whether task pair is assigned on node pair. z = 1 iff both d_i and d_j work.
                    z_ij = plp.LpVariable(f"z_{t_i}_{t_j}_{n_i}_{n_j}", cat='Binary')
                    prob += z_ij <= d[t_i][n_i]
                    prob += z_ij <= d[t_j][n_j]
                    prob += z_ij >= (d[t_i][n_i] + d[t_j][n_j] - 1)

                    # Do both ways (t_i, t_j assigned to either n_i and n_j or n_j and n_i) -> only one of z_ij or z_ji can be 1.0 at a time.
                    z_ji = plp.LpVariable(f"z_{t_i}_{t_j}_{n_j}_{n_i}", cat='Binary')
                    prob += z_ji <= d[t_i][n_j]
                    prob += z_ji <= d[t_j][n_i]
                    prob += z_ji >= (d[t_i][n_j] + d[t_j][n_i] - 1)

                    pair_bandwidth[k] += task_comm.bandwidth * (z_ij + z_ji)
                    choose_path_constraints[(t_i, t_j)] += z_ij + z_ji

        # Handle task pairs assigned to the same node. Assume no bandwidth cost.
        for n in self.nodes:
            for t_i, t_j in task_communication.keys():
                # Whether task pair is assigned on the same node. z = 1 iff both d_i and d_j work.
                z_ii = plp.LpVariable(f"z_{t_i}_{t_j}_{n}_{n}", cat='Binary')
                prob += z_ii <= d[t_i][n]
                prob += z_ii <= d[t_j][n]
                prob += z_ii >= (d[t_i][n] + d[t_j][n] - 1)

                choose_path_constraints[(t_i, t_j)] += z_ii
                    
        # Enforce that each task pair uses exactly one path if they are assigned to different nodes
        for t_i, t_j in task_communication.keys():
            prob += choose_path_constraints[(t_i, t_j)] == 1

        # 4. Edge bandwidth constraints. Each edge's total bandwidth used by all task pairs <= edge capacity.
        edge_constraints = {}
        for n_i, n_j in path_bandwidths:
            for k, path in enumerate(paths.get((n_i, n_j), paths.get((n_j, n_i), []))):
                # Extract edges on this path
                path_edges = []
                for idx in range(len(path) - 1):
                    edge_key: EdgeKey = (path[idx], path[idx+1]) if path[idx] < path[idx+1] else (path[idx+1], path[idx])
                    path_edges.append(edge_key)

                for edge_key in path_edges:
                    if edge_key not in edge_constraints:
                        edge_constraints[edge_key] = 0
                    edge_constraints[edge_key] += path_bandwidths[(n_i, n_j)][k]
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
                        assigned_task = RunningTask(node_id=n, task=task)
                        assignments[t] = assigned_task
                        break
        return assignments, objective_value, status_code

import pulp as plp
from typing import Dict, List, Tuple, Set
from collections import defaultdict
from itertools import combinations

class TaskScheduler:
    def __init__(self, tasks: List[str], resources: List[str], 
                 nodes: List[str], edges: List[Tuple[str, str]]):
        """
        Initialize the task scheduler.
        
        Args:
            tasks: List of task identifiers
            resources: List of resource types (e.g., ['cpu', 'gpu', 'memory'])
            nodes: List of node identifiers
            edges: List of (node1, node2) tuples representing network edges
        """
        self.tasks = tasks
        self.resources = resources
        self.nodes = nodes
        self.edges = edges
        self.graph = self._build_graph()
        
    def _build_graph(self) -> Dict:
        """Build adjacency structure for the network graph."""
        graph = defaultdict(list)
        for u, v in self.edges:
            graph[u].append(v)
            graph[v].append(u)
        return dict(graph)
    
    def solve(self, task_resources: Dict[str, Dict[str, float]],
              node_capacity: Dict[str, Dict[str, float]],
              task_communication: Dict[Tuple[str, str], float],
              edge_bandwidth: Dict[Tuple[str, str], float],
              prev_assignment: Dict[str, str],
              paths: Dict[Tuple[str, str], List[str]],
              reassignment_penalty: float = 1.0,
              time_limit: int = 300) -> Tuple[Dict, float | None, int]:
        """
        Solve the task scheduling optimization problem.
        
        Args:
            task_resources: {task: {resource: amount}} - Resource requirements
            node_capacity: {node: {resource: capacity}} - Node capacities
            task_communication: {(task_i, task_j): bandwidth} - Communication requirements
            edge_bandwidth: {(node1, node2): capacity} - Edge bandwidth capacities
            prev_assignment: {task: node} - Previous epoch assignments
            paths: {(node1, node2): [path_nodes]} - Routing paths between node pairs
            reassignment_penalty: Weight for minimizing reassignments
            time_limit: Solver time limit in seconds
            
        Returns:
            (assignment, objective_value, status_code) - New assignment dict, objective value, and solver status code.
        """
        prob = plp.LpProblem("Task_Scheduling", plp.LpMinimize)
        
        # Decision variables
        # d[t][n] = 1 if task t assigned to node n, 0 otherwise
        d = {t: {n: plp.LpVariable(f"d_{t}_{n}", cat='Binary') 
                 for n in self.nodes} for t in self.tasks}
        
        # Auxiliary variable for task allocation tracking
        allocated = {t: plp.LpVariable(f"allocated_{t}", cat='Binary') 
                     for t in self.tasks}
        
        # Objective: Minimize reassignments, Maximize allocations
        reassignments = plp.lpSum(
            d[t][n] for t in self.tasks for n in self.nodes
            if t in prev_assignment and prev_assignment[t] != n
        )
        total_allocated = plp.lpSum(allocated[t] for t in self.tasks)
        
        prob += -total_allocated + reassignment_penalty * reassignments
        
        # Constraints
        # 1. Each task assigned to exactly one node if allocated
        for t in self.tasks:
            prob += plp.lpSum(d[t][n] for n in self.nodes) == allocated[t]
        
        # 2. Node resource capacity constraints
        for n in self.nodes:
            for r in self.resources:
                if r in node_capacity.get(n, {}):
                    prob += plp.lpSum(
                        task_resources.get(t, {}).get(r, 0) * d[t][n] 
                        for t in self.tasks
                    ) <= node_capacity[n][r]

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
                for t_i, t_j in task_communication.keys():

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

                    pair_bandwidth[k] += task_communication[(t_i, t_j)] * (z_ij + z_ji)
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
                    edge_key = (path[idx], path[idx+1]) if path[idx] < path[idx+1] else (path[idx+1], path[idx])
                    path_edges.append(edge_key)

                for edge_key in path_edges:
                    if edge_key not in edge_constraints:
                        edge_constraints[edge_key] = 0
                    edge_constraints[edge_key] += path_bandwidths[(n_i, n_j)][k]
        for edge_key, total_bandwidth in edge_constraints.items():
            prob += total_bandwidth <= edge_bandwidth[edge_key]
        
        # Solve
        solver = plp.PULP_CBC_CMD(timeLimit=time_limit, msg=0)
        prob.solve(solver)
        prob.writeLP("task_scheduling.lp")

        print("Status:", plp.LpStatus[prob.status])
        
        # Extract solution
        assignment = {}
        status_code = prob.status
        objective_value = None
        if plp.LpStatus[status_code] == 'Optimal':
            objective_value = plp.value(prob.objective)
            for t in self.tasks:
                for n in self.nodes:
                    if plp.value(d[t][n]) == 1:
                        assignment[t] = n
                        break
        return assignment, objective_value, status_code


# Example usage
if __name__ == "__main__":
    # Define problem instance
    tasks = ['task_0', 'task_1', 'task_2', 'task_3']
    resources = ['cpu', 'memory']
    nodes = ['node_0', 'node_1', 'node_2']
    edges = [('node_0', 'node_1'), ('node_1', 'node_2'), ('node_0', 'node_2')]
    
    # Task resource requirements
    task_resources = {
        'task_0': {'cpu': 4, 'memory': 8},
        'task_1': {'cpu': 2, 'memory': 4},
        'task_2': {'cpu': 3, 'memory': 6},
        'task_3': {'cpu': 2, 'memory': 3},
    }
    
    # Node capacity
    node_capacity = {
        'node_0': {'cpu': 10, 'memory': 16},
        'node_1': {'cpu': 8, 'memory': 12},
        'node_2': {'cpu': 12, 'memory': 20},
    }
    
    # Communication requirements between tasks (bandwidth in Mbps)
    task_communication = {
        ('task_0', 'task_1'): 50,
        ('task_0', 'task_2'): 100,
        ('task_1', 'task_3'): 25,
    }
    
    # Edge bandwidth capacities
    edge_bandwidth = {
        ('node_0', 'node_1'): 200,
        ('node_1', 'node_2'): 150,
        ('node_0', 'node_2'): 300,
    }
    
    # Previous assignment from prior epoch
    prev_assignment = {
        'task_0': 'node_0',
        'task_1': 'node_1',
        'task_2': 'node_2',
    }
    
    # Paths between nodes (complete paths)
    paths = {
        ('node_0', 'node_1'): [['node_0', 'node_1']],
        ('node_0', 'node_2'): [['node_0', 'node_2']],
        ('node_1', 'node_2'): [['node_1', 'node_2']],
        ('node_1', 'node_0'): [['node_1', 'node_0']],
        ('node_2', 'node_0'): [['node_2', 'node_0']],
        ('node_2', 'node_1'): [['node_2', 'node_1']],
    }
    
    # Solve
    scheduler = TaskScheduler(tasks, resources, nodes, edges)
    assignment, obj_value, status_code = scheduler.solve(
        task_resources, node_capacity, task_communication,
        edge_bandwidth, prev_assignment, paths,
        reassignment_penalty=10.0, time_limit=300
    )
    if plp.LpStatus[status_code] == 'Optimal':
        print("Optimal Assignment:")
        for task, node in sorted(assignment.items()):
            print(f"  {task} -> {node}")
        display_obj_value = f"{obj_value:.2f}" if obj_value is not None else "N/A"
        print(f"\nObjective Value: {display_obj_value}")
    else:
        print("No optimal assignment found.")
    
    # Show reassignments
    reassignments = [t for t in assignment if t in prev_assignment 
                     and assignment[t] != prev_assignment[t]]
    print(f"Reassignments: {reassignments if reassignments else 'None'}")
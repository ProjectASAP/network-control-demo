import cvxpy

class Optimizer:
    def __init__(self, tasks, resources, bandwidth, topology, allocation, paths):
        self.tasks = tasks
        self.resources = resources
        self.bandwidth = bandwidth
        self.topology = topology
        self.allocation = allocation
        #self.logical_bandwidth = logical_bandwidth  # dict: (task1_id, task2_id) -> bandwidth
        self.paths = paths  # dict: (node1_id, node2_id) -> [edge_id, ...]

        self.problem = None
        self.decision_vars = {}

    def define_decision_variables(self):
        # decision_vars[task.task_id][node_id] = 1 if task assigned to node
        self.decision_vars = {
            task.task_id: cvxpy.Variable(len(self.resources), boolean=True) 
            for task in self.tasks
        }

    def set_constraints(self):
        constraints = []

        # Each task assigned to at most one node
        for task in self.tasks:
            constraints.append(cvxpy.sum(self.decision_vars[task.task_id]) <= 1)

        # Node resource capacity constraints (per resource type)
        for node_idx, resource in enumerate(self.resources):
            for res_type in resource.capacity.keys():
                constraints.append(
                    cvxpy.sum([
                        task.resource_requirements.get(res_type, 0) * self.decision_vars[task.task_id][node_idx]
                        for task in self.tasks
                    ]) <= resource.capacity[res_type]
                )

        # Edge bandwidth capacity constraints (including logical bandwidth)
        for edge in self.topology.edges:
            edge_load = []
            for (t1, t2), bw in self.bandwidth.bandwidth_needs.items():
                for node1_idx in range(len(self.resources)):
                    for node2_idx in range(len(self.resources)):
                        # If path between node1 and node2 uses this edge
                        if edge.id in self.paths.get((node1_idx, node2_idx), []):
                            edge_load.append(
                                bw * self.decision_vars[t1][node1_idx] * self.decision_vars[t2][node2_idx]
                            )
            constraints.append(cvxpy.sum(edge_load) <= self.bandwidth[edge.id])

        return constraints

    def solve(self):
        self.define_decision_variables()
        constraints = self.set_constraints()

        objective = cvxpy.Maximize(
            cvxpy.sum([cvxpy.sum(self.decision_vars[task.id]) for task in self.tasks])
        )

        self.problem = cvxpy.Problem(objective, constraints)
        self.problem.solve()

        return {task.id: self.decision_vars[task.id].value for task in self.tasks}
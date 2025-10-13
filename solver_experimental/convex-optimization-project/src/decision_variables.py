from cvxpy import Variable

class DecisionVariables:
    def __init__(self, num_tasks, num_nodes):
        self.num_tasks = num_tasks
        self.num_nodes = num_nodes
        self.allocation = Variable((num_tasks, num_nodes), boolean=True)

    def get_allocation(self):
        return self.allocation

    def set_allocation(self, allocation_matrix):
        for i in range(self.num_tasks):
            for j in range(self.num_nodes):
                self.allocation[i, j] = allocation_matrix[i][j]
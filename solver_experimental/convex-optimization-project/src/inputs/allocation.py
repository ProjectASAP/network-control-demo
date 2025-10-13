class Allocation:
    def __init__(self):
        self.current_allocation = {}

    def load_allocation_data(self, allocation_data):
        self.current_allocation = allocation_data

    def get_current_allocation(self):
        return self.current_allocation

    def update_allocation(self, task, node):
        self.current_allocation[task] = node

    def remove_allocation(self, task):
        if task in self.current_allocation:
            del self.current_allocation[task]
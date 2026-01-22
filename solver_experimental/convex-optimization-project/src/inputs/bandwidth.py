import csv


class Bandwidth:
    def __init__(self):
        self.bandwidth_needs = {}  # {(task1, task2): value}
        self.bandwidth_capacity = {}  # {edge_id: value}

    def load_bandwidth(self, needs_file, capacity_file):
        # Load bandwidth needs between tasks
        with open(needs_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Assumes columns: task1,task2,bandwidth
                self.bandwidth_needs[(row["task1"], row["task2"])] = float(
                    row["bandwidth"]
                )

        # Load bandwidth capacity for edges
        with open(capacity_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Assumes columns: edge_id,capacity
                self.bandwidth_capacity[row["edge_id"]] = float(row["capacity"])

    def get_bandwidth_needs(self):
        return self.bandwidth_needs

    def get_bandwidth_capacity(self):
        return self.bandwidth_capacity

    def check_capacity(self, edge_id, required_bw):
        return self.bandwidth_capacity.get(edge_id, 0) >= required_bw

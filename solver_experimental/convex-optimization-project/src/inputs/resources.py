import csv


class Resource:
    def __init__(self, node_id, capacity):
        self.node_id = node_id
        self.capacity = capacity  # dict: {resource_type: value}

    @classmethod
    def load_resources(cls, file_path):
        resources = []
        with open(file_path, "r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                node_id = row["node_id"]
                # Remove node_id and convert resource values to float
                capacity = {k: float(v) for k, v in row.items() if k != "node_id"}
                resources.append(cls(node_id, capacity))
        return resources

    def __repr__(self):
        return f"Resource(node_id={self.node_id}, capacity={self.capacity})"

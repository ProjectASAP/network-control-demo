import csv


class Topology:
    def __init__(self):
        self.nodes = []
        self.edges = []

    @staticmethod
    def load_topology(nodes_file, edges_file):
        topology = Topology()
        # Load nodes
        with open(nodes_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                topology.nodes.append(row["node_id"])
        # Load edges
        with open(edges_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Each edge is a dict: {'edge_id': ..., 'src': ..., 'dst': ...}
                topology.edges.append(
                    {"edge_id": row["edge_id"], "src": row["src"], "dst": row["dst"]}
                )
        return topology

    def get_nodes(self):
        return self.nodes

    def get_edges(self):
        return self.edges

    def __str__(self):
        return f"Topology(nodes={self.nodes}, edges={self.edges})"

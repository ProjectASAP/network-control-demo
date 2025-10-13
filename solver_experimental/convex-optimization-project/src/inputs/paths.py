import csv

def load_paths(filepath):
    paths = {}
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            node1 = row['node1_id']
            node2 = row['node2_id']
            edge_str = row['edge_ids']
            edge_ids = edge_str.split('|')
            paths[(node1, node2)] = edge_ids
    return paths
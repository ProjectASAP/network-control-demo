import sys
import os

parent_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.append(parent_dir)

from entities import *
from load_info import *
from solver import *
import networkx as nx
from pathlib import Path
from itertools import combinations
import pprint
import os


def load_network_topology(node_path: str | Path, edge_path: str | Path) -> NetworkTopology:
    nodes = load_nodes(node_path)
    edges = load_edges(edge_path)
    network = NetworkTopology(nodes.values(), edges.values())
    return network


def solve_scheduling_problem(node_path, edge_path, task_path, task_comms_path):
    network = load_network_topology(node_path, edge_path)

    tasks = load_tasks(task_path)
    task_comms = load_task_communications(task_comms_path)
    paths = {}
    for n_i, n_j in combinations(network.nodes, 2):
        if network.has_path(n_i, n_j):
            paths[(n_i, n_j)] = [network.find_shortest_path(n_i, n_j)]

    scheduler = TaskScheduler(network, reassignment_penalty=10.0)

    assignment, obj_value, status_code = scheduler.solve(
        tasks,
        task_comms,
        running_tasks={"t1": RunningTask("n4", time.time(), tasks["t1"])},
        paths=paths
    )
    return assignment, obj_value, status_code


if __name__ == "__main__":
    test_cases_dir = "data/"
    test_cases = ["low_link_capacity", "high_task_cpu", "cpu_bandwidth_strained"]

    for test_case in test_cases:
        print(f"Test Case: {test_case}")
        case_dir = os.path.join(test_cases_dir, test_case)

        node_path = os.path.join(case_dir, "nodes.csv")
        edge_path = os.path.join(case_dir, "edges.csv")
        tasks_path = os.path.join(case_dir, "tasks.csv")
        task_comms_path = os.path.join(case_dir, "task_comms.csv")

        assignment, obj_value, status_code = solve_scheduling_problem(
            node_path,
            edge_path,
            tasks_path,
            task_comms_path
        )
        if plp.LpStatus[status_code] == 'Optimal':
            print("Optimal Assignment:")
            for task, node in sorted(assignment.items()):
                print(f"  {task} -> {node}")
            
            display_obj_value = f"{obj_value:.2f}" if obj_value is not None else "N/A"
            print(f"\nObjective Value: {display_obj_value}")
        else:
            print("No optimal assignment found.")
    
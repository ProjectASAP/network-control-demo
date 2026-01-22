# main.py

import cvxpy as cp
from optimizer import Optimizer
from inputs.tasks import Task
from inputs.resources import Resource
from inputs.bandwidth import Bandwidth
from inputs.topology import Topology
from inputs.allocation import Allocation
from inputs.logical_bandwidth import load_logical_bandwidth
from inputs.paths import load_paths


def main():
    # Load input data
    tasks = Task.load_tasks("data/tasks.csv")
    resources = Resource.load_resources("data/resources.csv")
    bandwidth = Bandwidth()
    bandwidth.load_bandwidth("data/logical_bandwidth.csv", "data/edgecap.csv")
    topology = Topology()
    topology = topology.load_topology("data/nodes.csv", "data/edges.csv")
    allocation = Allocation().load_allocation_data("data/allocation.csv")
    # logical_bandwidth = load_logical_bandwidth('data/logical_bandwidth.csv')
    paths = load_paths("data/paths.csv")

    print("Loaded Data:")
    print(f"Tasks: {tasks}")
    print(f"Resources: {resources}")
    print(f"Bandwidth: {bandwidth}")
    print(f"Topology: {topology}")
    print(f"Allocation: {allocation}")
    # print(f"Logical Bandwidth: {logical_bandwidth}")
    print(f"Paths: {paths}")

    # Initialize the optimizer
    optimizer = Optimizer(tasks, resources, bandwidth, topology, allocation, paths)

    # Execute the optimization process
    optimal_allocation = optimizer.solve()

    # Output the results
    print("Optimal Task Allocation:")
    print(optimal_allocation)


if __name__ == "__main__":
    main()

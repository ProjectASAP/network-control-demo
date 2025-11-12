import os
import sys
import yaml
import time
import requests
import argparse
import datetime
import numpy as np
import logging
from itertools import combinations
from collections import deque

# import urllib3
from loguru import logger
import pulp
from typing import Dict
import threading
import subprocess
import concurrent.futures
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# import similarity_scores
# from prometheus_api_client import PrometheusConnect
# from classes.config import Config
# from classes.QueryLatencyExporter import QueryLatencyExporter
# from promql_utilities.query_results.classes import QueryResult, QueryResultAcrossTime
# from promql_utilities.query_results.serializers import SerializerFactory

from scheduler.entities import RunningTask, Task, NetworkTopology
from scheduler.load_info import load_nodes, load_edges, load_tasks, build_task_graph
from scheduler.solver import TaskScheduler


QUERIES = [
    'quantile_over_time(0.5, cpu_usage[150s])'
]


def main(args: argparse.Namespace):
    logger.info("Loading network information and initializing solver...")

    logger.debug(f"Node path: {args.node_path}")
    nodes = load_nodes(args.node_path)
    logger.debug(f"Edge path: {args.edge_path}")
    edges = load_edges(args.edge_path)
    network = NetworkTopology(nodes.values(), edges.values())

    # Find shortest path between nodes.
    paths = {}
    for n_i, n_j in combinations(network.nodes, 2):
        if network.has_path(n_i, n_j):
            paths[(n_i, n_j)] = [network.find_shortest_path(n_i, n_j)]

    logger.info("Loading task request information...")
    tasks = load_tasks(args.task_path)
    task_graph = build_task_graph(tasks)
    task_queue = deque(sorted(tasks.values(), key=lambda task: task.arrival_offset_s))

    solver = TaskScheduler(network=network)
    start_time = time.time()

    # Mapping between task id and running task.
    running_tasks: dict[str, RunningTask] = {}
    unassigned_tasks: dict[str, Task] = {}
    while task_queue:
        time.sleep(args.interval)
        curr_offset = (time.time() - start_time) * 100 
        logger.debug(f"Current time offset: {curr_offset:.2f} ms")

        for query in QUERIES:
            try:
                params = {
                    'query': query
                }
                response = requests.get(args.server_url, params=params)
                response.raise_for_status()
                data = response.json()
            except requests.RequestException as e:
                logger.debug(f"Could not fetch telemetry information: {e}")
                continue

        # Filter out finished tasks. For now, don't account for solver time and variable finish times.
        running_tasks = {task_id: rt for task_id, rt in running_tasks.items() if curr_offset - rt.start_time_s >= rt.task.duration_s}
        logger.debug(f"Currently running tasks: {list(running_tasks.keys())}")

        arrived_tasks: dict[str, Task] = {}
        # Schedule newly arrived tasks.
        while task_queue:
            task = task_queue[0]
            if task.arrival_offset_s < curr_offset:
                arrived_tasks[task.task_id] = task
                task_queue.popleft()
            else:
                break
        logger.debug(f"Arrived tasks: {list(arrived_tasks.keys())}")
        logger.debug(f"Unassigned tasks from previous rounds: {list(unassigned_tasks.keys())}")
        
        tasks_to_schedule = arrived_tasks | unassigned_tasks

        logger.info(f"Scheduling {len(tasks_to_schedule)} tasks...")
        assignments, leftover_tasks, objective_value, status_code = solver.solve(
            tasks=tasks_to_schedule,
            task_graph=task_graph,
            running_tasks=running_tasks,
            paths=paths
        )
        logger.debug(f"Solver status: {pulp.LpStatus[status_code]}")

        unassigned_tasks = leftover_tasks
        running_tasks.update(assignments)

        logger.info(f"Number of unassigned tasks after scheduling: {len(unassigned_tasks)}")
        if pulp.LpStatus[status_code] == 'Optimal' and assignments:
            assignment_repr = "Assignment: "
            for task, rt in sorted(assignments.items()):
                assignment_repr += f"{task} -> {rt.node_id}, "
            logger.info(assignment_repr.rstrip(", "))
            display_obj_value = f"{objective_value:.2f}" if objective_value is not None else "N/A"
            logger.debug(f"Objective Value: {display_obj_value}")
        else:
            logger.info("Could not assign tasks.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Network demo controller.")
    # parser.add_argument("--config-file", type=str, required=True)
    parser.add_argument("--node-path", type=str, required=True)
    parser.add_argument("--edge-path", type=str, required=True)
    parser.add_argument("--task-path", type=str, required=True)
    # parser.add_argument("--task-communication-path", type=str, required=True)
    parser.add_argument("--interval", type=float, default=10.0)
    parser.add_argument("--server-url", type=str, default="http://localhost:8088/api/v1")
    parser.add_argument("--log-level", type=str, default="INFO")

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level=args.log_level)
    main(args)
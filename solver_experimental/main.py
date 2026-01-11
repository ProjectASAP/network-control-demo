import os
import sys
import yaml
import time
import requests
import argparse
import datetime
from itertools import combinations
from collections import deque
from loguru import logger
import pulp
from typing import Dict
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
<<<<<<< HEAD
from elasticsearch import Elasticsearch
=======
from dataclasses import dataclass
from cattrs import structure, unstructure
import jsonlines
>>>>>>> emit_assignments

from scheduler.entities import RunningTask, Task, NetworkTopology
from scheduler.load_info import load_nodes, load_edges, load_tasks, build_task_graph
from scheduler.solver import TaskScheduler
from query_engine_utils.config import QueryManagerConfig, QueryGroupConfig, load_query_config
from query_engine_utils.server_querying import QueryManager


@dataclass
class AppConfig:
    node_path: str
    edge_path: str
    task_path: str
    query_manager_config: str
    interval: float = 10.0
    log_level: str = "INFO"


def assign_tasks(args: AppConfig):
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

    query_config = load_query_config(args.query_manager_config)

    # Elasticsearch vs Sketch lib rust.
    client = Elasticsearch(
        hosts=[{"host": "localhost", "port": 9200, "scheme": "http"}],
        api_key='TWg0S01wc0JhR1AxOFVUcUY5N2w6bGR0TjIySHRZTHVwdmZLTmtqcGtGQQ=='
    )
    index_name = 'cluster-metrics'
    query = {
        'bool': {
            'must': [
                {'term': {'cluster.keyword': 'cluster-c'}},
                {'term': {'task.keyword': 'worker'}},
                # {'range': {'@timestamp': {'gte': 'now-30s', 'lt': 'now'}}}
            ]
        }
    }

    quantiles = [10 * i for i in range(1, 10)]
    aggs = {
        "average_cpu": {"avg": {"field": "cpu_cores"}},
        "cpu_quantiles": {
            "percentiles": {
                "field": "cpu_cores", 
                "percents": quantiles
            }
        }
    }
    with QueryManager(query_config=query_config) as query_manager:
        # Mapping between task id and running task.
        running_tasks: dict[str, RunningTask] = {}
        unassigned_tasks: dict[str, Task] = {}

        while task_queue:
            time.sleep(args.interval)
            curr_offset = (time.time() - start_time) * 100
            logger.debug(f"Current time offset: {curr_offset:.2f} s")

            # TODO: Execute PromQL queries and do something with results (e.g. update task spec estimates).
            # query_manager.update_task_metrics(running_tasks=running_tasks)

            # Query Elasticsearch instead.
            start_t = time.time()
            data = client.search(index=index_name, aggs=aggs)
            end_time = time.time()
            print(f"Query took {end_time - start_t} seconds (Elastic)")
            # print(f'Aggregations: {data["aggregations"]}')

            sketch_query_url = "http://localhost:10101/metrics/cpu_cores"
            payload = {
                'quantiles': [f'p{q}' for q in quantiles]
            }
            start_t = time.time()
            sketch_response = requests.post(sketch_query_url, json=payload)
            print(f'Sketch query took {time.time() - start_t} seconds (Sketch)')
            # print(f'Aggregations: {sketch_response.json()}')

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
            if not tasks_to_schedule:
                logger.info(f"Waiting for tasks to arrive...")
                continue

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

            yield assignments


def main(args: argparse.Namespace):
    config = structure(vars(args), AppConfig)
    with jsonlines.open('assignments_log.jsonl', mode='w') as writer:
        for assignments in assign_tasks(config):
            running_tasks = unstructure(assignments.values(), list[RunningTask])
            writer.write(running_tasks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Network demo controller.")
    parser.add_argument("--node-path", type=str, required=True)
    parser.add_argument("--edge-path", type=str, required=True)
    parser.add_argument("--task-path", type=str, required=True)
    parser.add_argument("--interval", type=float, default=10.0)
    parser.add_argument("--query-manager-config", type=str, required=True)
    parser.add_argument("--log-level", type=str, default="INFO")

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level=args.log_level)
    main(args)
import os
import sys
import yaml
import time
import argparse
import datetime
import csv
import threading
import json
import copy
from itertools import combinations
from collections import deque
from loguru import logger
import pulp
from typing import Dict
import httpx
from urllib3.util.retry import Retry
from dataclasses import dataclass
from cattrs import structure, unstructure
import jsonlines

from scheduler.entities import RunningTask, Task, NetworkTopology
from scheduler.load_info import load_nodes, load_edges, load_tasks, build_task_graph
from scheduler.solver import TaskScheduler
from query_engine_utils.config import QueryManagerConfig, QueryGroupConfig, load_query_config
from query_engine_utils.server_querying import QueryManager

from es_query import update_tasks_with_metrics


@dataclass
class AppConfig:
    node_path: str
    edge_path: str
    task_path: str
    query_manager_config: str
    interval: float = 10.0
    log_level: str = "INFO"


E2E_LOG_PATH = os.getenv("E2E_LOG_CSV", "e2e.csv")
_E2E_LOG_LOCK = threading.Lock()


def log_e2e(
    duration_ms: float,
    curr_offset: float,
    tasks_to_schedule: int,
    ran_solver: bool,
    metrics_source: str,
    assignment: dict[str, str] | None,
) -> None:
    assignment_text = ""
    if assignment is not None:
        assignment_text = json.dumps(assignment, separators=(",", ":"), sort_keys=True)
    timestamp = datetime.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
    with _E2E_LOG_LOCK:
        try:
            needs_header = os.path.getsize(E2E_LOG_PATH) == 0
        except OSError:
            needs_header = True
        with open(E2E_LOG_PATH, "a", newline="") as handle:
            writer = csv.writer(handle)
            if needs_header:
                writer.writerow([
                    "timestamp",
                    "offset_s",
                    "tasks_to_schedule",
                    "ran_solver",
                    "metrics_source",
                    "duration_ms",
                    "assignment",
                ])
            writer.writerow([
                timestamp,
                f"{curr_offset:.3f}",
                str(tasks_to_schedule),
                "1" if ran_solver else "0",
                metrics_source,
                f"{duration_ms:.3f}",
                assignment_text,
            ])


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
    with QueryManager(query_config=query_config) as query_manager:
        # Mapping between task id and running task.
        running_tasks: dict[str, RunningTask] = {}
        unassigned_tasks: dict[str, Task] = {}
        synthetic_node_id = os.getenv("SYNTHETIC_NODE_ID", "synthetic-node")

        while task_queue:
            time.sleep(args.interval)
            curr_offset = (time.time() - start_time)
            logger.debug(f"Current time offset: {curr_offset:.2f} s")

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
                log_e2e(
                    duration_ms=-1.0,
                    curr_offset=curr_offset,
                    tasks_to_schedule=0,
                    ran_solver=False,
                    metrics_source="none",
                    assignment=None,
                )
                logger.info(f"Waiting for tasks to arrive...")
                continue

            query_tasks = dict(running_tasks)
            for task_id, task in tasks_to_schedule.items():
                if task_id in query_tasks:
                    continue
                query_tasks[task_id] = RunningTask(
                    node_id=synthetic_node_id,
                    start_time_s=curr_offset,
                    task=task,
                )

            tasks_to_schedule_es = copy.deepcopy(tasks_to_schedule)
            running_tasks_es = copy.deepcopy(running_tasks)
            query_tasks_es = dict(running_tasks_es)
            for task_id, task in tasks_to_schedule_es.items():
                if task_id in query_tasks_es:
                    continue
                query_tasks_es[task_id] = RunningTask(
                    node_id=synthetic_node_id,
                    start_time_s=curr_offset,
                    task=task,
                )

            # TODO: Execute PromQL queries and do something with results (e.g. update task spec estimates).
            # query_manager.update_task_metrics(running_tasks=query_tasks)

            # Query Elasticsearch (sketch server) instead.
            e2e_start = time.perf_counter()
            assignments: dict[str, RunningTask] = {}
            leftover_tasks: dict[str, Task] = {}
            objective_value = None
            status_code = None
            try:
                task_metrics = update_tasks_with_metrics(running_tasks=query_tasks)
                if task_metrics:
                    logger.debug(f"Collected metrics for {len(task_metrics)} tasks")

                logger.info(f"Scheduling {len(tasks_to_schedule)} tasks...")
                assignments, leftover_tasks, objective_value, status_code = solver.solve(
                    tasks=tasks_to_schedule,
                    task_graph=task_graph,
                    running_tasks=running_tasks,
                    paths=paths
                )
                logger.debug(f"Solver status (sketch): {pulp.LpStatus[status_code]}")
            finally:
                duration_ms = (time.perf_counter() - e2e_start) * 1000.0
                assignment_map = {task_id: rt.node_id for task_id, rt in assignments.items()}
                log_e2e(
                    duration_ms=duration_ms,
                    curr_offset=curr_offset,
                    tasks_to_schedule=len(tasks_to_schedule),
                    ran_solver=True,
                    metrics_source="sketch",
                    assignment=assignment_map,
                )

            es_error = None
            es_start = time.perf_counter()
            assignments_es: dict[str, RunningTask] = {}
            try:
                task_metrics_es = update_tasks_with_metrics(
                    running_tasks=query_tasks_es,
                    use_backend=True,
                )
                if task_metrics_es:
                    logger.debug(f"Collected ES metrics for {len(task_metrics_es)} tasks")

                assignments_es, _, _, status_code_es = solver.solve(
                    tasks=tasks_to_schedule_es,
                    task_graph=task_graph,
                    running_tasks=running_tasks_es,
                    paths=paths
                )
                logger.debug(f"Solver status (es): {pulp.LpStatus[status_code_es]}")
            except Exception as exc:
                es_error = exc
                logger.warning(f"ES assignment failed: {exc}")
            finally:
                duration_ms = (time.perf_counter() - es_start) * 1000.0
                assignment_map_es = {task_id: rt.node_id for task_id, rt in assignments_es.items()}
                log_e2e(
                    duration_ms=duration_ms,
                    curr_offset=curr_offset,
                    tasks_to_schedule=len(tasks_to_schedule_es),
                    ran_solver=es_error is None,
                    metrics_source="elasticsearch",
                    assignment=assignment_map_es,
                )

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
    with httpx.Client(timeout=5) as client:
        for assignments in assign_tasks(config):
            running_tasks = unstructure(assignments.values(), list[RunningTask])
            if running_tasks:
                client.post("http://localhost:8000/ingest", json=running_tasks)


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

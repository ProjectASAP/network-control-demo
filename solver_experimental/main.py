import os
import sys
import os
import yaml
import time
import argparse
import copy
import uuid
import csv
import datetime as dt
from itertools import combinations
from collections import deque
from loguru import logger
import pulp
import httpx
from urllib3.util.retry import Retry
from dataclasses import dataclass
from cattrs import structure, unstructure

from scheduler.entities import RunningTask, Task, NetworkTopology
from scheduler.load_info import load_nodes, load_edges, load_tasks, build_task_graph
from scheduler.solver import TaskScheduler
from query_engine_utils.config import (
    QueryManagerConfig,
    QueryGroupConfig,
    load_query_config,
)
from query_engine_utils.server_querying import QueryManager
from config import (
    CONSISTENCY_CHECK_TOLERANCE,
    PARALLEL_BENCHMARK_ENABLED,
    SCHEDULER_BATCH_SIZE,
    CLUSTER_METRICS_CSV,
    ES_TIME_FIELD,
    TIME_RANGE_MS,
)
from logging_utils import log_e2e, log_node_metric_comparisons

from es_query import (
    fetch_node_usage,
    compare_node_metrics,
    check_es_available,
    NodeMetricsSnapshot,
)

INGEST_POST_TIMEOUT = float(os.getenv("INGEST_POST_TIMEOUT_SECONDS", "30"))


@dataclass
class AppConfig:
    # Runtime configuration for data sources and loop cadence.
    node_path: str
    edge_path: str
    task_path: str
    query_manager_config: str
    emulator_url: str = "http://localhost:8000"
    epoch_length_s: float = 300.0
    interval: float = 10.0
    log_level: str = "INFO"


def filter_completed_tasks(client: httpx.Client, running_tasks: dict[str, RunningTask], current_time: float | None = None) -> dict[str, RunningTask]:
    """Filter out tasks whose duration has elapsed. Assumes client base URL is set to the emulator URL."""
    active_tasks = {}
    try:
        response = client.get("/active_tasks")
        response.raise_for_status()
        data = response.json()
        active_tasks = structure(data["running_tasks"], dict[str, RunningTask])
        return active_tasks
    except Exception as exc:
        logger.warning(f"Failed to fetch active tasks from emulator: {exc}")

    # Fallback: local filtering based on time estimates.
    logger.warning("Falling back to local task completion filtering.")
    if current_time is None:
        current_time = time.time()
    for task_id, rt in running_tasks.items():
        elapsed = current_time - rt.start_time_s
        if elapsed < rt.task.duration_s:
            active_tasks[task_id] = rt
    return active_tasks


def _post_with_retry(
    client: httpx.Client,
    url: str,
    payload: list[dict],
    retries: int = 20,
    delay_s: float = 0.2,
) -> None:
    for attempt in range(1, retries + 1):
        try:
            response = client.post(url, json=payload, timeout=INGEST_POST_TIMEOUT)
            response.raise_for_status()
            return
        except httpx.HTTPError as exc:
            if attempt >= retries:
                raise
            logger.warning(
                "POST failed (attempt {}/{}): {}",
                attempt,
                retries,
                exc,
            )
            time.sleep(delay_s)


def assign_tasks(args: AppConfig):
    logger.info("Loading network information and initializing solver...")
    # Load network topology from disk.
    logger.debug(f"Node path: {args.node_path}")
    nodes = load_nodes(args.node_path)
    logger.debug(f"Edge path: {args.edge_path}")
    edges = load_edges(args.edge_path)
    network = NetworkTopology(nodes.values(), edges.values())
    node_ids = sorted(nodes.keys())
    node_limit_raw = os.getenv("NODE_QUERY_LIMIT")
    if node_limit_raw:
        try:
            node_limit = int(node_limit_raw)
        except ValueError:
            logger.warning(
                "Invalid NODE_QUERY_LIMIT '{}'; ignoring.", node_limit_raw
            )
            node_limit = None
        if node_limit is not None and node_limit > 0:
            node_ids = node_ids[:node_limit]
            logger.info("Limiting node queries to first {} nodes.", node_limit)

    # Precompute shortest paths between all node pairs.
    paths = {}
    for n_i, n_j in combinations(network.nodes, 2):
        if network.has_path(n_i, n_j):
            paths[(n_i, n_j)] = [network.find_shortest_path(n_i, n_j)]

    # Load task stream template; replay tasks each epoch with unique ids.
    logger.info("Loading task request information...")
    template_tasks = load_tasks(args.task_path)
    template_tasks_sorted = sorted(
        template_tasks.values(), key=lambda task: task.arrival_offset_s
    )

    # Initialize the solver and benchmarking timers.
    solver = TaskScheduler(network=network)

    # Load query config and toggle benchmark modes.
    parallel_enabled = PARALLEL_BENCHMARK_ENABLED
    consistency_tolerance = CONSISTENCY_CHECK_TOLERANCE

    # Decide whether to run the ES comparison path.
    es_available = check_es_available()
    if not es_available:
        logger.warning("Direct ES backend unavailable; running sketch-only benchmark.")
    if not parallel_enabled:
        logger.info("Parallel benchmark disabled; running sketch-only benchmark.")

    epoch_length_s = args.epoch_length_s
    query_time_range_ms = epoch_length_s * 1000

    with httpx.Client(timeout=5, base_url=args.emulator_url) as client:
        # Track running and unassigned tasks across iterations.
        running_tasks: dict[str, RunningTask] = {}
        unassigned_tasks: dict[str, Task] = {}
        retry_counts: dict[str, int] = {}
        failed_tasks: dict[str, Task] = {}
        if not template_tasks_sorted:
            logger.info("No template tasks found; ingestion-only mode.")

        task_queue: deque[Task] = deque(template_tasks_sorted)
        epoch_index = -1

        base_time_s = time.time()
        while True:
            # Add delay between iterations to give time for task assignment and metric feedback loop to take effect.
            # This delay is independent of the internal clock used for task arrival offsets and metric querying, which is based on the base_time_s + offset.
            time.sleep(args.interval)
            epoch_index += 1

            # Advance simulated time to the next arrival.
            curr_offset_s = epoch_index * epoch_length_s
            current_time_s = base_time_s + curr_offset_s
            logger.debug(f"Current time offset: {curr_offset_s:.2f} s")
            # Prune tasks whose duration has elapsed.
            running_tasks = filter_completed_tasks(client, running_tasks, current_time_s)
            logger.debug(f"Currently running tasks ({len(running_tasks)}): {list(running_tasks.keys())}")

            arrived_tasks: dict[str, Task] = {}
            # Pull all tasks that have arrived at this simulated time.
            while task_queue and task_queue[0].arrival_offset_s <= curr_offset_s:
                task = task_queue.popleft()
                arrived_tasks[task.task_id] = task
            logger.debug(f"Arrived tasks ({len(arrived_tasks)}): {list(arrived_tasks.keys())}")
            logger.debug(
                f"Unassigned tasks from previous rounds ({len(unassigned_tasks)}): {list(unassigned_tasks.keys())}"
            )

            # Combine leftover and new tasks, keeping older unassigned tasks first.
            tasks_to_schedule_pool = dict(
                list(unassigned_tasks.items()) + list(arrived_tasks.items())
            )
            if failed_tasks:
                tasks_to_schedule_pool = {
                    task_id: task
                    for task_id, task in tasks_to_schedule_pool.items()
                    if task_id not in failed_tasks
                }
            if not tasks_to_schedule_pool:
                log_e2e(
                    duration_ms=-1.0,
                    curr_offset=curr_offset_s,
                    tasks_to_schedule=0,
                    ran_solver=False,
                    metrics_source="none",
                    assignment=None,
                    correlation_id=None,
                )
                logger.info(
                    "No pending tasks to schedule at this arrival time."
                )
                continue
            task_graph = build_task_graph(tasks_to_schedule_pool)
            tasks_to_schedule = tasks_to_schedule_pool
            overflow_tasks: dict[str, Task] = {}
            if len(tasks_to_schedule) > SCHEDULER_BATCH_SIZE:
                ordered_items = list(tasks_to_schedule.items())
                tasks_to_schedule = dict(ordered_items[:SCHEDULER_BATCH_SIZE])
                overflow_tasks = dict(ordered_items[SCHEDULER_BATCH_SIZE:])
            logger.info(
                "Backlog size: {}; scheduling: {}",
                len(tasks_to_schedule_pool),
                len(tasks_to_schedule),
            )

            current_time_ms = int((base_time_s + curr_offset_s) * 1000.0)
            window_start_ms = max(0, current_time_ms - query_time_range_ms)
            logger.debug(
                "Time window epoch={} current={} start={} field={}",
                epoch_index + 1,
                current_time_ms,
                window_start_ms,
                ES_TIME_FIELD,
            )
            # Fetch metrics from sketch and optionally from ES.
            correlation_id = uuid.uuid4().hex[:8]
            sketch_node_metrics: dict[str, NodeMetricsSnapshot] = {}
            metrics_needed = ["cpu", "mem"]
            if any(task.peer_bandwidths for task in tasks_to_schedule.values()):
                metrics_needed.append("net")

            sketch_start = time.perf_counter()
            sketch_node_metrics, sketch_top_entities = fetch_node_usage(
                node_ids=node_ids,
                correlation_id=correlation_id,
                metrics=metrics_needed,
                current_time_ms=current_time_ms,
                time_range_ms=query_time_range_ms,
            )
            sketch_query_ms = (time.perf_counter() - sketch_start) * 1000.0
            if sketch_node_metrics:
                logger.debug(
                    "Collected metrics for {} nodes in {:.2f} ms",
                    len(sketch_node_metrics),
                    sketch_query_ms,
                )

            # Run solver on sketch metrics.
            sk_solver_start = time.perf_counter()
            assignments: dict[str, RunningTask] = {}
            leftover_tasks: dict[str, Task] = {}
            objective_value = None

            logger.info(f"Scheduling {len(tasks_to_schedule)} tasks...")
            assignments, leftover_tasks, objective_value, status_code = (
                solver.solve(
                    tasks=tasks_to_schedule,
                    task_graph=task_graph,
                    running_tasks=running_tasks,
                    paths=paths,
                    time_limit=30,
                    current_time_s=current_time_s,
                )
            )

            solver_status = pulp.LpStatus.get(status_code, "unknown")
            logger.info(
                "Solver status: {} (code={})",
                solver_status,
                status_code,
            )

            # Log total time for sketch query + solver, along with the assignments.
            sk_solver_ms = (time.perf_counter() - sk_solver_start) * 1000.0
            sk_duration_ms = sketch_query_ms + sk_solver_ms
            assignment_map = {
                task_id: rt.node_id for task_id, rt in assignments.items()
            }
            log_e2e(
                duration_ms=sk_duration_ms,
                curr_offset=curr_offset_s,
                tasks_to_schedule=len(tasks_to_schedule),
                ran_solver=True,
                metrics_source="sketch",
                assignment=assignment_map,
                correlation_id=correlation_id,
            )

            # Carry over leftovers and any unscheduled overflow, keeping oldest tasks first.
            if assignments:
                for task_id in assignments:
                    retry_counts.pop(task_id, None)
            for task_id in list(leftover_tasks.keys()):
                retry_counts[task_id] = retry_counts.get(task_id, 0) + 1
                if retry_counts[task_id] >= 5:
                    failed_tasks[task_id] = leftover_tasks.pop(task_id)
                    logger.warning(
                        "Task {} exceeded retry limit; moving to failed list.",
                        task_id,
                    )
            unassigned_tasks = dict(
                list(leftover_tasks.items()) + list(overflow_tasks.items())
            )
            running_tasks.update(assignments)

            # Display assignments + objective value, and push assignments to emulator immediately so next query sees the metrics.
            logger.info(f"Number of running tasks ({len(assignments)} new assignments): {len(running_tasks)}")
            if solver_status == "Optimal" and assignments:
                assignment_repr = "Assignment: "
                for task, rt in sorted(assignments.items()):
                    assignment_repr += f"{task} -> {rt.node_id}, "
                logger.info(assignment_repr.rstrip(", "))
                display_obj_value = (
                    f"{objective_value:.2f}"
                    if objective_value is not None
                    else "N/A"
                )
                logger.debug(f"Objective Value: {display_obj_value}")
            else:
                logger.info("Could not assign tasks.")

            # Push assignments to emulator immediately so next query sees the metrics.
            if assignments:
                running_tasks_payload = unstructure(
                    list(assignments.values()), list[RunningTask]
                )
                _post_with_retry(
                    client,
                    "/ingest",
                    running_tasks_payload,
                )
                logger.debug(
                    "Pushed {} assignments to emulator before next task arrival",
                    len(assignments),
                )

            yield assignments


def main(args: argparse.Namespace):
    # Convert CLI args to config and run the scheduling loop.
    # Assignments are pushed to the emulator inside assign_tasks() immediately
    # after each scheduling decision, ensuring the next query sees the metrics.
    config = structure(vars(args), AppConfig)
    for assignments in assign_tasks(config):
        # Assignments already pushed inside assign_tasks(); just consume the generator.
        pass


if __name__ == "__main__":
    # Parse CLI options and configure logging.
    parser = argparse.ArgumentParser(description="Network demo controller.")
    parser.add_argument("--node-path", type=str, default="dummy_data/nodes.jsonl")
    parser.add_argument("--edge-path", type=str, default="dummy_data/edges.jsonl")
    parser.add_argument("--task-path", type=str, default="dummy_data/tasks.jsonl")
    parser.add_argument("--emulator-url", type=str, default="http://localhost:8000")
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--epoch-length-s", type=float, default=300.0)
    parser.add_argument("--query-manager-config", type=str, required=True)
    parser.add_argument("--log-level", type=str, default="INFO")
    args = parser.parse_args()
    logger.remove()
    logger.add(sys.stderr, level=args.log_level)
    main(args)

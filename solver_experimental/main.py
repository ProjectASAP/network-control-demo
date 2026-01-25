import os
import sys
import yaml
import time
import argparse
import copy
import uuid
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
)
from logging_utils import log_e2e, log_node_metric_comparisons

from es_query import (
    fetch_node_usage,
    compare_node_metrics,
    check_es_available,
    NodeMetricsSnapshot,
)


@dataclass
class AppConfig:
    # Runtime configuration for data sources and loop cadence.
    node_path: str
    edge_path: str
    task_path: str
    query_manager_config: str
    emulator_url: str = "http://localhost:8000"
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


def assign_tasks(args: AppConfig, client: httpx.Client):
    logger.info("Loading network information and initializing solver...")
    # Load network topology from disk.
    logger.debug(f"Node path: {args.node_path}")
    nodes = load_nodes(args.node_path)
    logger.debug(f"Edge path: {args.edge_path}")
    edges = load_edges(args.edge_path)
    network = NetworkTopology(nodes.values(), edges.values())
    node_ids = list(nodes.keys())

    # Precompute shortest paths between all node pairs.
    paths = {}
    for n_i, n_j in combinations(network.nodes, 2):
        if network.has_path(n_i, n_j):
            paths[(n_i, n_j)] = [network.find_shortest_path(n_i, n_j)]

    # Load task stream and build dependency graph.
    logger.info("Loading task request information...")
    tasks = load_tasks(args.task_path)
    task_graph = build_task_graph(tasks)
    task_queue = deque(sorted(tasks.values(), key=lambda task: task.arrival_offset_s))

    # Initialize the solver and benchmarking timers.
    solver = TaskScheduler(network=network)
    start_time = time.time()

    # Load query config and toggle benchmark modes.
    query_config = load_query_config(args.query_manager_config)
    parallel_enabled = PARALLEL_BENCHMARK_ENABLED
    consistency_tolerance = CONSISTENCY_CHECK_TOLERANCE

    # Decide whether to run the ES comparison path.
    es_available = check_es_available()
    if not es_available:
        logger.warning("Direct ES backend unavailable; running sketch-only benchmark.")
    if not parallel_enabled:
        logger.info("Parallel benchmark disabled; running sketch-only benchmark.")

    with QueryManager(query_config=query_config) as query_manager:
        # Track running and unassigned tasks across iterations.
        running_tasks: dict[str, RunningTask] = {}
        unassigned_tasks: dict[str, Task] = {}
        retry_counts: dict[str, int] = {}
        failed_tasks: dict[str, Task] = {}

        while task_queue or unassigned_tasks or running_tasks:
            time.sleep(args.interval)
            curr_offset = time.time() - start_time
            logger.debug(f"Current time offset: {curr_offset:.2f} s")

            # Prune tasks whose duration has elapsed.
            curr_time = start_time + curr_offset
            running_tasks = filter_completed_tasks(client, running_tasks, curr_time)
            logger.debug(f"Currently running tasks ({len(running_tasks)}): {list(running_tasks.keys())}")

            arrived_tasks: dict[str, Task] = {}
            # NOTE: (Temp) Pull one newly arrived task into the scheduling window at a time. 
            while task_queue:
                if len(arrived_tasks) >= 1:
                    break
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
                    curr_offset=curr_offset,
                    tasks_to_schedule=0,
                    ran_solver=False,
                    metrics_source="none",
                    assignment=None,
                    correlation_id=None,
                )
                logger.info(f"Waiting for tasks to arrive...")
                continue

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

            # Clone tasks for the ES path to avoid mutating sketch results.
            tasks_to_schedule_es = copy.deepcopy(tasks_to_schedule)
            running_tasks_es = copy.deepcopy(running_tasks)

            # TODO: Execute PromQL queries and do something with results (e.g. update task spec estimates).
            # query_manager.update_task_metrics(running_tasks=query_tasks)

            # Fetch metrics from sketch and optionally from ES.
            correlation_id = uuid.uuid4().hex[:8]
            sketch_node_metrics: dict[str, NodeMetricsSnapshot] = {}
            es_node_metrics: dict[str, NodeMetricsSnapshot] = {}
            sketch_top_entities = []
            es_top_entities = []
            metrics_needed = ["cpu", "mem"]
            if any(task.peer_bandwidths for task in tasks_to_schedule.values()):
                metrics_needed.append("net")
            percentiles = [25, 50, 75, 90]
            if parallel_enabled and es_available:
                sketch_start = time.perf_counter()
                sketch_node_metrics, sketch_top_entities = fetch_node_usage(
                    node_ids=node_ids,
                    correlation_id=correlation_id,
                    metrics=metrics_needed,
                    percentiles=percentiles,
                )
                sketch_query_ms = (time.perf_counter() - sketch_start) * 1000.0

                es_start = time.perf_counter()
                es_node_metrics, es_top_entities = fetch_node_usage(
                    node_ids=node_ids,
                    use_es=True,
                    correlation_id=correlation_id,
                    metrics=metrics_needed,
                    percentiles=percentiles,
                )
                es_query_ms = (time.perf_counter() - es_start) * 1000.0
                logger.debug(
                    "Fetched metrics (sketch=%d, es=%d) in %.2f/%.2f ms",
                    len(sketch_node_metrics),
                    len(es_node_metrics),
                    sketch_query_ms,
                    es_query_ms,
                )
            else:
                sketch_start = time.perf_counter()
                sketch_node_metrics, sketch_top_entities = fetch_node_usage(
                    node_ids=node_ids,
                    correlation_id=correlation_id,
                    metrics=metrics_needed,
                    percentiles=percentiles,
                )
                sketch_query_ms = (time.perf_counter() - sketch_start) * 1000.0
                if sketch_node_metrics:
                    logger.debug(
                        "Collected metrics for %d nodes in %.2f ms",
                        len(sketch_node_metrics),
                        sketch_query_ms,
                    )

            # Apply node usage from cumulative metrics before solving.
            for node_id, snapshot in sketch_node_metrics.items():
                node = network.get_node(node_id)
                if snapshot.cumulative is not None:
                    node.used_cpu = min(
                        snapshot.cumulative.cpu_cores, node.cpu_capacity
                    )
                    node.used_memory = min(
                        snapshot.cumulative.memory_gb, node.memory_capacity
                    )
                    if node.network_capacity is not None:
                        node.used_network = min(
                            snapshot.cumulative.network_mbps,
                            node.network_capacity,
                        )

            # Solver Pass #1: Sketch
            sk_solver_start = time.perf_counter()
            assignments: dict[str, RunningTask] = {}
            leftover_tasks: dict[str, Task] = {}
            objective_value = None
            status_code = None
            try:
                logger.info(f"Scheduling {len(tasks_to_schedule)} tasks...")
                assignments, leftover_tasks, objective_value, status_code = (
                    solver.solve(
                        tasks=tasks_to_schedule,
                        task_graph=task_graph,
                        running_tasks=running_tasks,
                        paths=paths,
                        time_limit=30,
                    )
                )
                logger.debug(f"Solver status (sketch): {pulp.LpStatus[status_code]}")
            finally:
                if status_code is not None:
                    logger.info(
                        "Solver status: {} (code={})",
                        pulp.LpStatus.get(status_code, "unknown"),
                        status_code,
                    )
                sk_solver_ms = (time.perf_counter() - sk_solver_start) * 1000.0
                sk_duration_ms = sketch_query_ms + sk_solver_ms
                assignment_map = {
                    task_id: rt.node_id for task_id, rt in assignments.items()
                }
                log_e2e(
                    duration_ms=sk_duration_ms,
                    curr_offset=curr_offset,
                    tasks_to_schedule=len(tasks_to_schedule),
                    ran_solver=True,
                    metrics_source="sketch",
                    assignment=assignment_map,
                    correlation_id=correlation_id,
                )

            assignments_es: dict[str, RunningTask] = {}
            status_code_es = None
            if parallel_enabled and es_available:
                # Solver Pass #2: ES-backed metrics for comparison.
                es_solver_start = time.perf_counter()
                es_error = None
                try:
                    assignments_es, _, _, status_code_es = solver.solve(
                        tasks=tasks_to_schedule_es,
                        task_graph=task_graph,
                        running_tasks=running_tasks_es,
                        paths=paths,
                        time_limit=30,
                    )
                    logger.debug(f"Solver status (es): {pulp.LpStatus[status_code_es]}")
                except Exception as exc:
                    es_error = exc
                    logger.warning(f"ES assignment failed: {exc}")
                finally:
                    es_solver_ms = (time.perf_counter() - es_solver_start) * 1000.0
                    es_duration_ms = es_query_ms + es_solver_ms
                    assignment_map_es = {
                        task_id: rt.node_id for task_id, rt in assignments_es.items()
                    }
                    log_e2e(
                        duration_ms=es_duration_ms,
                        curr_offset=curr_offset,
                        tasks_to_schedule=len(tasks_to_schedule_es),
                        ran_solver=es_error is None,
                        metrics_source="elasticsearch",
                        assignment=assignment_map_es,
                        correlation_id=correlation_id,
                    )

                # Compare sketch vs ES metrics and log any discrepancies.
                discrepancies = compare_node_metrics(
                    sketch_metrics=sketch_node_metrics,
                    es_metrics=es_node_metrics,
                    tolerance=consistency_tolerance,
                )
                if discrepancies:
                    logger.warning(
                        f"Consistency check failed: {len(discrepancies)} discrepancies"
                    )
                    for item in discrepancies[:5]:
                        logger.warning(f"  - {item}")
                log_node_metric_comparisons(
                    correlation_id=correlation_id,
                    sketch_metrics=sketch_node_metrics,
                    es_metrics=es_node_metrics,
                    sketch_top_entities=sketch_top_entities,
                    es_top_entities=es_top_entities,
                )
                # Detect solver assignment differences between sketch and ES inputs.
                if assignments and assignments_es:
                    sketch_assignment_map = {
                        tid: rt.node_id for tid, rt in assignments.items()
                    }
                    es_assignment_map = {
                        tid: rt.node_id for tid, rt in assignments_es.items()
                    }
                    if sketch_assignment_map != es_assignment_map:
                        logger.info(
                            "Solver assignments differ between Sketch and ES paths"
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
                        "Task {} exceeded retry limit; moving to failed list.", task_id
                    )
            unassigned_tasks = dict(
                list(leftover_tasks.items()) + list(overflow_tasks.items())
            )
            running_tasks.update(assignments)

            logger.info(f"Number of running tasks ({len(assignments)} new assignments): {len(running_tasks)}")
            if pulp.LpStatus[status_code] == 'Optimal' and assignments:
                assignment_repr = "Assignment: "
                for task, rt in sorted(assignments.items()):
                    assignment_repr += f"{task} -> {rt.node_id}, "
                logger.info(assignment_repr.rstrip(", "))
                display_obj_value = (
                    f"{objective_value:.2f}" if objective_value is not None else "N/A"
                )
                logger.debug(f"Objective Value: {display_obj_value}")
            else:
                logger.info("Could not assign tasks.")

            yield assignments


def main(args: argparse.Namespace):
    # Convert CLI args to config and post assignments to the emulator.
    config = structure(vars(args), AppConfig)
    with httpx.Client(timeout=5, base_url=config.emulator_url) as client:
        for assignments in assign_tasks(config, client):
            running_tasks = unstructure(assignments.values(), list[RunningTask])
            if not running_tasks:
                continue
            try:
                response = client.post("/ingest", json=running_tasks)
                response.raise_for_status()
                logger.info(f"Posted {len(running_tasks)} assignments to emulator.")
            except Exception as exc:
                logger.error(f"Failed to post assignments to emulator: {exc}")


if __name__ == "__main__":
    # Parse CLI options and configure logging.
    parser = argparse.ArgumentParser(description="Network demo controller.")
    parser.add_argument("--node-path", type=str, default="dummy_data/nodes.jsonl")
    parser.add_argument("--edge-path", type=str, default="dummy_data/edges.jsonl")
    parser.add_argument("--task-path", type=str, default="dummy_data/tasks.jsonl")
    parser.add_argument("--emulator-url", type=str, default="http://localhost:8000")
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--query-manager-config", type=str, required=True)
    parser.add_argument("--log-level", type=str, default="INFO")
    args = parser.parse_args()
    logger.remove()
    logger.add(sys.stderr, level=args.log_level)
    main(args)

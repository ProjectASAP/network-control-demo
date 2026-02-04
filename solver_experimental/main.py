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
from typing import Dict
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
    interval: float = 10.0
    log_level: str = "INFO"


def _csv_time_bounds_ms(path: str) -> tuple[int, int]:
    earliest: dt.datetime | None = None
    latest: dt.datetime | None = None
    with open(path, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ts = row.get("timestamp")
            if not ts:
                continue
            try:
                parsed = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                continue
            if earliest is None or parsed < earliest:
                earliest = parsed
            if latest is None or parsed > latest:
                latest = parsed
    if earliest is None or latest is None:
        raise ValueError(f"No valid timestamp rows found in {path}")
    return int(earliest.timestamp() * 1000), int(latest.timestamp() * 1000)


def _instantiate_epoch_tasks(
    template_tasks: dict[str, Task], epoch_index: int, epoch_length_s: int
) -> dict[str, Task]:
    # Clone tasks with epoch-scoped ids and absolute arrival offsets.
    id_map = {task_id: f"{task_id}_e{epoch_index}" for task_id in template_tasks}
    epoch_start_s = epoch_index * epoch_length_s
    epoch_tasks: dict[str, Task] = {}
    for task_id, task in template_tasks.items():
        peer_bandwidths = {
            id_map[peer_id]: bw
            for peer_id, bw in task.peer_bandwidths.items()
            if peer_id in id_map
        }
        epoch_tasks[id_map[task_id]] = Task(
            task_id=id_map[task_id],
            arrival_offset_s=epoch_start_s + task.arrival_offset_s,
            duration_s=task.duration_s,
            initial_cpu=task.initial_cpu,
            initial_memory=task.initial_memory,
            peer_bandwidths=peer_bandwidths,
        )
    return epoch_tasks


def _iter_csv_rows(path: str):
    with open(path, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ts = row.get("timestamp")
            if not ts:
                continue
            try:
                parsed = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                continue
            ts_ms = int(parsed.timestamp() * 1000)
            ts_iso = parsed.astimezone(dt.timezone.utc).isoformat(timespec="milliseconds")
            if ts_iso.endswith("+00:00"):
                ts_iso = ts_iso.replace("+00:00", "Z")
            yield {
                "timestamp": ts_iso,
                "timestamp_ms": ts_ms,
                "cluster": row.get("cluster"),
                "task": row.get("task"),
                "cpu_cores": float(row.get("cpu_cores", 0.0) or 0.0),
                "memory_gb": float(row.get("memory_gb", 0.0) or 0.0),
                "network_mbps": float(row.get("network_mbps", 0.0) or 0.0),
            }


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


def _chunk_rows(rows: list[dict], chunk_size: int) -> list[list[dict]]:
    if chunk_size <= 0:
        return [rows]
    return [rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)]


def assign_tasks(args: AppConfig, ingest_client: httpx.Client | None = None):
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
    query_config = load_query_config(args.query_manager_config)
    parallel_enabled = PARALLEL_BENCHMARK_ENABLED
    consistency_tolerance = CONSISTENCY_CHECK_TOLERANCE

    # Decide whether to run the ES comparison path.
    es_available = check_es_available()
    if not es_available:
        logger.warning("Direct ES backend unavailable; running sketch-only benchmark.")
    if not parallel_enabled:
        logger.info("Parallel benchmark disabled; running sketch-only benchmark.")

    epoch_length_s = 3000
    query_time_range_ms = TIME_RANGE_MS
    base_epoch_ms, latest_epoch_ms = _csv_time_bounds_ms(CLUSTER_METRICS_CSV)
    dataset_duration_s = max(0.0, (latest_epoch_ms - base_epoch_ms) / 1000.0)
    max_epochs = max(1, int(dataset_duration_s // epoch_length_s) + 1)
    logger.info(
        "Using base epoch {} ms from {} (duration {:.1f}s, epochs={}, epoch_len={}s)",
        base_epoch_ms,
        CLUSTER_METRICS_CSV,
        dataset_duration_s,
        max_epochs,
        epoch_length_s,
    )

    with QueryManager(query_config=query_config) as query_manager:
        # Track running and unassigned tasks across iterations.
        running_tasks: dict[str, RunningTask] = {}
        unassigned_tasks: dict[str, Task] = {}
        retry_counts: dict[str, int] = {}
        failed_tasks: dict[str, Task] = {}
        if not template_tasks_sorted:
            logger.info("No template tasks found; ingestion-only mode.")
        epoch_index = 0
        task_queue: deque[Task] = deque()
        next_row_iter = _iter_csv_rows(CLUSTER_METRICS_CSV)
        next_row = next(next_row_iter, None)
        current_time_s = 0.0

        def _load_epoch_tasks(index: int) -> deque[Task]:
            epoch_tasks = _instantiate_epoch_tasks(
                template_tasks, index, epoch_length_s
            )
            return deque(
                sorted(epoch_tasks.values(), key=lambda task: task.arrival_offset_s)
            )

        if template_tasks_sorted and max_epochs > 0:
            task_queue = _load_epoch_tasks(epoch_index)

        while True:
            if not task_queue and template_tasks_sorted and epoch_index + 1 < max_epochs:
                epoch_index += 1
                task_queue = _load_epoch_tasks(epoch_index)
                continue

            next_task_time = (
                task_queue[0].arrival_offset_s if task_queue else float("inf")
            )
            next_row_time = (
                (next_row["timestamp_ms"] - base_epoch_ms) / 1000.0
                if next_row
                else float("inf")
            )

            if next_task_time == float("inf") and next_row_time == float("inf"):
                break

            if next_row_time <= next_task_time:
                current_epoch_end_s = (epoch_index + 1) * epoch_length_s
                cutoff_s = min(next_task_time, current_epoch_end_s)
                batch_rows: list[dict] = []
                while next_row is not None:
                    next_row_time = (next_row["timestamp_ms"] - base_epoch_ms) / 1000.0
                    if next_row_time > cutoff_s:
                        break
                    batch_rows.append(next_row)
                    next_row = next(next_row_iter, None)
                if batch_rows:
                    latest_batch_s = (
                        batch_rows[-1]["timestamp_ms"] - base_epoch_ms
                    ) / 1000.0
                    current_time_s = max(current_time_s, latest_batch_s)
                    if ingest_client is not None:
                        for chunk in _chunk_rows(batch_rows, 1000):
                            _post_with_retry(
                                ingest_client,
                                "http://localhost:8000/ingest_rows",
                                chunk,
                            )
                    continue
                if (
                    next_task_time == float("inf")
                    and next_row is not None
                    and next_row_time > current_epoch_end_s
                    and epoch_index + 1 < max_epochs
                ):
                    epoch_index += 1
                    if template_tasks_sorted:
                        task_queue = _load_epoch_tasks(epoch_index)
                    continue

            # Advance simulated time to the next arrival.
            curr_offset = max(current_time_s, next_task_time)
            current_time_s = curr_offset
            logger.debug(f"Current time offset: {curr_offset:.2f} s")
            # Prune tasks whose duration has elapsed.
            running_tasks = {
                task_id: rt
                for task_id, rt in running_tasks.items()
                if curr_offset - rt.start_time_s < rt.task.duration_s
            }
            logger.debug(f"Currently running tasks: {list(running_tasks.keys())}")

            arrived_tasks: dict[str, Task] = {}
            # Pull all tasks that have arrived at this simulated time.
            while task_queue and task_queue[0].arrival_offset_s <= curr_offset:
                task = task_queue.popleft()
                arrived_tasks[task.task_id] = task
            logger.debug(f"Arrived tasks: {list(arrived_tasks.keys())}")
            logger.debug(
                "Unassigned tasks from previous rounds: {}",
                list(unassigned_tasks.keys()),
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

            # Clone tasks for the ES path to avoid mutating sketch results.
            tasks_to_schedule_es = copy.deepcopy(tasks_to_schedule)
            running_tasks_es = copy.deepcopy(running_tasks)

            # TODO: Execute PromQL queries and do something with results (e.g. update task spec estimates).
            # query_manager.update_task_metrics(running_tasks=query_tasks)

            current_time_ms = base_epoch_ms + int(curr_offset * 1000.0)
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
            es_node_metrics: dict[str, NodeMetricsSnapshot] = {}
            sketch_top_entities = []
            es_top_entities = []
            metrics_needed = ["cpu", "mem"]
            if any(task.peer_bandwidths for task in tasks_to_schedule.values()):
                metrics_needed.append("net")
            # Simplified: only query cumulative usage, no percentiles needed.
            if parallel_enabled and es_available:
                sketch_start = time.perf_counter()
                sketch_node_metrics, sketch_top_entities = fetch_node_usage(
                    node_ids=node_ids,
                    correlation_id=correlation_id,
                    metrics=metrics_needed,
                    current_time_ms=current_time_ms,
                    time_range_ms=query_time_range_ms,
                )
                sketch_query_ms = (time.perf_counter() - sketch_start) * 1000.0

                es_start = time.perf_counter()
                es_node_metrics, es_top_entities = fetch_node_usage(
                    node_ids=node_ids,
                    use_es=True,
                    correlation_id=correlation_id,
                    metrics=metrics_needed,
                    current_time_ms=current_time_ms,
                    time_range_ms=query_time_range_ms,
                    time_field=ES_TIME_FIELD,
                )
                es_query_ms = (time.perf_counter() - es_start) * 1000.0
                logger.debug(
                    "Fetched metrics (sketch={}, es={}) in {:.2f}/{:.2f} ms",
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

            # Apply node usage from cumulative metrics before solving.
            # This runs for both parallel and sketch-only modes.
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

            # Log node usage to show feedback loop effect.
            usage_summary = []
            for node_id in sorted(sketch_node_metrics.keys()):
                node = network.get_node(node_id)
                cpu_pct = (node.used_cpu / node.cpu_capacity * 100) if node.cpu_capacity else 0
                mem_pct = (node.used_memory / node.memory_capacity * 100) if node.memory_capacity else 0
                usage_summary.append(f"{node_id}:CPU={cpu_pct:.0f}%,MEM={mem_pct:.0f}%")
            if usage_summary:
                logger.info(
                    "Node usage at offset {:.1f}s: {}",
                    curr_offset,
                    " | ".join(usage_summary),
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
                        current_time_s=curr_offset,
                    )
                )
                logger.debug(
                    f"Solver status (sketch): {pulp.LpStatus[status_code]}"
                )
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
                        current_time_s=curr_offset,
                    )
                    logger.debug(
                        f"Solver status (es): {pulp.LpStatus[status_code_es]}"
                    )
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
                        "Consistency check failed: {} discrepancies",
                        len(discrepancies),
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
                        "Task {} exceeded retry limit; moving to failed list.",
                        task_id,
                    )
            unassigned_tasks = dict(
                list(leftover_tasks.items()) + list(overflow_tasks.items())
            )
            running_tasks.update(assignments)

            # Emit summary logging for each scheduling round.
            logger.info(
                "Number of unassigned tasks after scheduling: {}",
                len(unassigned_tasks),
            )
            if pulp.LpStatus[status_code] == "Optimal" and assignments:
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
            if assignments and ingest_client is not None:
                running_tasks_payload = unstructure(
                    list(assignments.values()), list[RunningTask]
                )
                _post_with_retry(
                    ingest_client,
                    "http://localhost:8000/ingest",
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
    with httpx.Client(timeout=5) as client:
        for assignments in assign_tasks(config, ingest_client=client):
            # Assignments already pushed inside assign_tasks(); just consume the generator.
            pass


if __name__ == "__main__":
    # Parse CLI options and configure logging.
    parser = argparse.ArgumentParser(description="Network demo controller.")
    parser.add_argument("--node-path", type=str, default="dummy_data/nodes.jsonl")
    parser.add_argument("--edge-path", type=str, default="dummy_data/edges.jsonl")
    parser.add_argument("--task-path", type=str, default="dummy_data/tasks.jsonl")
    parser.add_argument("--interval", type=float, default=10.0)
    parser.add_argument("--query-manager-config", type=str, required=True)
    parser.add_argument("--log-level", type=str, default="INFO")
    args = parser.parse_args()
    logger.remove()
    logger.add(sys.stderr, level=args.log_level)
    main(args)

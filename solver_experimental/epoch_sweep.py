#!/usr/bin/env python3
"""Epoch-based sweep: ingest synthetic data, query both backends, run solver."""

from __future__ import annotations

import argparse
import copy
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from itertools import combinations
from typing import Dict, List

import requests

from scheduler.entities import NetworkTopology, Task
from scheduler.load_info import load_nodes, load_edges, load_tasks, build_task_graph
from scheduler.solver import TaskScheduler

from config import (
    ES_API_KEY,
    ES_INDEX_NAME,
    ES_TIME_FIELD,
    ES_URL,
    SKETCH_URL,
    PARALLEL_BENCHMARK_ENABLED,
    CONSISTENCY_CHECK_TOLERANCE,
    TIME_RANGE_MS,
)
from es_query import fetch_node_usage, compare_node_metrics, check_es_available
from logging_utils import log_e2e, log_node_metric_comparisons

DEFAULT_ROWS_PER_EPOCH = 100_000
DEFAULT_START_EPOCH = 1
DEFAULT_END_EPOCH = 5
DEFAULT_BATCH_SIZE = 1000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Epoch-based sweep with solver")
    parser.add_argument("--node-path", type=str, default="dummy_data/nodes.jsonl")
    parser.add_argument("--edge-path", type=str, default="dummy_data/edges.jsonl")
    parser.add_argument("--task-path", type=str, default="dummy_data/tasks.jsonl")
    parser.add_argument("--start-epoch", type=int, default=DEFAULT_START_EPOCH)
    parser.add_argument("--end-epoch", type=int, default=DEFAULT_END_EPOCH)
    parser.add_argument("--rows-per-epoch", type=int, default=DEFAULT_ROWS_PER_EPOCH)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--solver-task-count",
        type=int,
        default=0,
        help="Number of solver tasks (0 = all)",
    )
    parser.add_argument(
        "--solver-node-count",
        type=int,
        default=0,
        help="Number of solver nodes (0 = all)",
    )
    parser.add_argument(
        "--query-node-count",
        type=int,
        default=0,
        help="Number of nodes to query (0 = all)",
    )
    parser.add_argument(
        "--connect-timeout", type=float, default=5.0, help="Connection timeout (s)"
    )
    parser.add_argument(
        "--read-timeout", type=float, default=60.0, help="Read timeout (s)"
    )
    parser.add_argument("--log-level", type=str, default="INFO")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

TASK_IDS = [f"T{i:03d}" for i in range(1, 201)]


def iter_batches(
    total_rows: int,
    node_ids: List[str],
    rng: random.Random,
    batch_size: int,
    epoch: int,
    base_time_ms: int,
) -> List[List[Dict[str, object]]]:
    """Generate synthetic metric rows in batches, matching solver_experimental field names."""
    batches: List[List[Dict[str, object]]] = []
    for start in range(0, total_rows, batch_size):
        end = min(total_rows, start + batch_size)
        batch: List[Dict[str, object]] = []
        for i in range(start, end):
            node = node_ids[i % len(node_ids)]
            task = TASK_IDS[i % len(TASK_IDS)]
            ts_ms = base_time_ms + i
            ts_iso = (
                datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )
            batch.append(
                {
                    "cluster": node,
                    "task": task,
                    "cpu_cores": rng.uniform(0.1, 64.0),
                    "memory_gb": rng.uniform(0.1, 256.0),
                    "network_mbps": rng.uniform(0.1, 10_000.0),
                    "estimated_duration": 3600.0,
                    "@timestamp": ts_iso,
                    "timestamp_ms": ts_ms,
                }
            )
        batches.append(batch)
    return batches


# ---------------------------------------------------------------------------
# Direct ingestion helpers
# ---------------------------------------------------------------------------


def ingest_to_server(
    server_url: str,
    batch: List[Dict[str, object]],
    epoch: int,
    connect_timeout: float,
    read_timeout: float,
) -> None:
    """Ingest a batch directly to the Rust sketch server (columnar format)."""
    payload = {
        "epoch": epoch,
        "task": [row["task"] for row in batch],
        "cluster": [row["cluster"] for row in batch],
        "cpu_cores": [row["cpu_cores"] for row in batch],
        "memory_gb": [row["memory_gb"] for row in batch],
        "network_mbps": [row["network_mbps"] for row in batch],
    }
    resp = requests.post(
        f"{server_url}/",
        json=payload,
        timeout=(connect_timeout, read_timeout),
    )
    if not resp.ok:
        from loguru import logger
        logger.warning("Sketch ingest failed ({}): {}", resp.status_code, resp.text[:200])
        return
    data = resp.json()
    inserted = data.get("inserted")
    if inserted != len(batch):
        from loguru import logger
        logger.warning("Server ingest mismatch: {} != {}", inserted, len(batch))


def reset_es_index(
    es_url: str,
    es_index: str,
    api_key: str | None,
    connect_timeout: float,
    read_timeout: float,
) -> None:
    """Delete and recreate the ES index with field mappings matching solver_experimental."""
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"ApiKey {api_key}"
    timeout = (connect_timeout, read_timeout)
    requests.delete(f"{es_url}/{es_index}", headers=headers, timeout=timeout)
    mapping = {
        "mappings": {
            "properties": {
                "cluster": {"type": "keyword"},
                "task": {"type": "keyword"},
                "cpu_cores": {"type": "float"},
                "memory_gb": {"type": "float"},
                "network_mbps": {"type": "float"},
                "estimated_duration": {"type": "float"},
                "@timestamp": {"type": "date"},
                "timestamp": {"type": "date"},
            }
        }
    }
    resp = requests.put(
        f"{es_url}/{es_index}",
        headers=headers,
        json=mapping,
        timeout=timeout,
    )
    resp.raise_for_status()


def ingest_to_es(
    es_url: str,
    es_index: str,
    api_key: str | None,
    batch: List[Dict[str, object]],
    connect_timeout: float,
    read_timeout: float,
    refresh: str | None = None,
) -> None:
    """Bulk ingest a batch directly to Elasticsearch."""
    headers = {"Content-Type": "application/x-ndjson"}
    if api_key:
        headers["Authorization"] = f"ApiKey {api_key}"
    lines = []
    for row in batch:
        lines.append(json.dumps({"index": {}}))
        doc = {
            "cluster": row["cluster"],
            "task": row["task"],
            "cpu_cores": row["cpu_cores"],
            "memory_gb": row["memory_gb"],
            "network_mbps": row["network_mbps"],
            "estimated_duration": row.get("estimated_duration", 0.0),
            "@timestamp": row["@timestamp"],
        }
        lines.append(json.dumps(doc))
    payload = "\n".join(lines) + "\n"
    params = {"refresh": refresh} if refresh else None
    resp = requests.post(
        f"{es_url}/{es_index}/_bulk",
        headers=headers,
        data=payload,
        params=params,
        timeout=(connect_timeout, read_timeout),
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("errors"):
        raise RuntimeError("ES bulk ingestion reported errors")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _select_first_n(items: List[str], count: int) -> List[str]:
    if count <= 0 or count >= len(items):
        return list(items)
    return list(items[:count])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    from loguru import logger

    logger.remove()
    logger.add(sys.stderr, level=args.log_level)

    # Load topology and solver assets.
    logger.info("Loading topology and solver assets...")
    nodes = load_nodes(args.node_path)
    edges = load_edges(args.edge_path)
    all_tasks = load_tasks(args.task_path)

    # Select subset of nodes/tasks for solver.
    solver_node_ids = _select_first_n(sorted(nodes.keys()), args.solver_node_count)
    solver_task_ids = _select_first_n(sorted(all_tasks.keys()), args.solver_task_count)
    solver_nodes = {nid: nodes[nid] for nid in solver_node_ids}
    solver_tasks = {tid: all_tasks[tid] for tid in solver_task_ids}
    solver_node_set = set(solver_nodes.keys())
    solver_edges = {
        eid: edge
        for eid, edge in edges.items()
        if eid[0] in solver_node_set and eid[1] in solver_node_set
    }

    network = NetworkTopology(solver_nodes.values(), solver_edges.values())
    task_graph = build_task_graph(solver_tasks)
    paths = {}
    for n_i, n_j in combinations(network.nodes, 2):
        if network.has_path(n_i, n_j):
            paths[(n_i, n_j)] = [network.find_shortest_path(n_i, n_j)]

    solver = TaskScheduler(network=network)
    logger.info(
        "Solver setup: tasks={}/{} nodes={}/{}",
        len(solver_tasks),
        len(all_tasks),
        len(solver_nodes),
        len(nodes),
    )

    # Determine query node set.
    query_node_ids = _select_first_n(solver_node_ids, args.query_node_count)
    if args.query_node_count > 0:
        logger.info(
            "Query setup: nodes={}/{}",
            len(query_node_ids),
            len(solver_node_ids),
        )

    rng = random.Random(args.seed)
    es_available = check_es_available()
    parallel_enabled = PARALLEL_BENCHMARK_ENABLED and es_available
    if not es_available:
        logger.warning("ES unavailable; running sketch-only.")

    # Reset ES index with solver_experimental field mappings.
    if es_available:
        logger.info("Resetting ES index...")
        reset_es_index(
            ES_URL, ES_INDEX_NAME, ES_API_KEY,
            args.connect_timeout, args.read_timeout,
        )

    # Base timestamp for synthetic data (arbitrary, recent past).
    base_time_ms = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)

    for epoch in range(args.start_epoch, args.end_epoch + 1):
        logger.info("=== Epoch {} ===", epoch)

        # 1. Generate synthetic data for this epoch.
        epoch_base_ms = base_time_ms + (epoch - 1) * args.rows_per_epoch
        batches = iter_batches(
            args.rows_per_epoch,
            query_node_ids,
            rng,
            args.batch_size,
            epoch,
            epoch_base_ms,
        )
        total_batches = len(batches)
        log_every = max(1, total_batches // 10)

        # 2. Ingest to both server + ES.
        for batch_idx, batch in enumerate(batches, start=1):
            ingest_to_server(
                SKETCH_URL, batch, epoch,
                args.connect_timeout, args.read_timeout,
            )
            is_last = batch_idx == total_batches
            if es_available:
                ingest_to_es(
                    ES_URL, ES_INDEX_NAME, ES_API_KEY,
                    batch,
                    args.connect_timeout, args.read_timeout,
                    refresh="wait_for" if is_last else None,
                )
            if batch_idx % log_every == 0 or is_last:
                logger.info(
                    "  ingest: {}/{} batches ({}%)",
                    batch_idx,
                    total_batches,
                    batch_idx * 100 // total_batches,
                )

        # Current time for queries = end of this epoch's data.
        current_time_ms = epoch_base_ms + args.rows_per_epoch
        correlation_id = uuid.uuid4().hex[:8]
        metrics_needed = ["cpu", "mem", "net"]

        # 3. Query both backends.
        sketch_start = time.perf_counter()
        sketch_metrics, sketch_top = fetch_node_usage(
            node_ids=query_node_ids,
            correlation_id=correlation_id,
            metrics=metrics_needed,
            current_time_ms=current_time_ms,
            time_range_ms=TIME_RANGE_MS,
        )
        sketch_query_ms = (time.perf_counter() - sketch_start) * 1000.0

        es_metrics = {}
        es_top = []
        es_query_ms = 0.0
        if parallel_enabled:
            es_start = time.perf_counter()
            es_metrics, es_top = fetch_node_usage(
                node_ids=query_node_ids,
                use_es=True,
                correlation_id=correlation_id,
                metrics=metrics_needed,
                current_time_ms=current_time_ms,
                time_range_ms=TIME_RANGE_MS,
                time_field=ES_TIME_FIELD,
            )
            es_query_ms = (time.perf_counter() - es_start) * 1000.0

        logger.info(
            "Query RTT: sketch={:.2f}ms es={:.2f}ms",
            sketch_query_ms,
            es_query_ms,
        )

        # 4. Update node usage from sketch metrics and run solver.
        for node_id, snapshot in sketch_metrics.items():
            node = network.get_node(node_id)
            if node is None or snapshot.cumulative is None:
                continue
            node.used_cpu = min(snapshot.cumulative.cpu_cores, node.cpu_capacity)
            node.used_memory = min(
                snapshot.cumulative.memory_gb, node.memory_capacity
            )
            if node.network_capacity is not None:
                node.used_network = min(
                    snapshot.cumulative.network_mbps, node.network_capacity
                )

        # 5. Solver pass: sketch-backed.
        sk_start = time.perf_counter()
        assignments, leftover, obj_val, status = solver.solve(
            tasks=solver_tasks,
            task_graph=task_graph,
            running_tasks={},
            paths=paths,
            time_limit=30,
        )
        sk_solver_ms = (time.perf_counter() - sk_start) * 1000.0
        sk_total_ms = sketch_query_ms + sk_solver_ms
        assignment_map = {tid: rt.node_id for tid, rt in assignments.items()}
        log_e2e(
            duration_ms=sk_total_ms,
            curr_offset=float(epoch),
            tasks_to_schedule=len(solver_tasks),
            ran_solver=True,
            metrics_source="sketch",
            assignment=assignment_map,
            correlation_id=correlation_id,
        )
        logger.info(
            "Sketch solver: obj={} assigned={} leftover={} solver={:.2f}ms total={:.2f}ms",
            obj_val,
            len(assignments),
            len(leftover),
            sk_solver_ms,
            sk_total_ms,
        )

        # 6. Solver pass: ES-backed (if available).
        es_solver_ms = 0.0
        es_total_ms = 0.0
        if parallel_enabled:
            # Apply ES usage to a copy of the network.
            for node_id, snapshot in es_metrics.items():
                node = network.get_node(node_id)
                if node is None or snapshot.cumulative is None:
                    continue
                node.used_cpu = min(
                    snapshot.cumulative.cpu_cores, node.cpu_capacity
                )
                node.used_memory = min(
                    snapshot.cumulative.memory_gb, node.memory_capacity
                )
                if node.network_capacity is not None:
                    node.used_network = min(
                        snapshot.cumulative.network_mbps, node.network_capacity
                    )

            es_solver_start = time.perf_counter()
            es_tasks = copy.deepcopy(solver_tasks)
            assignments_es, leftover_es, obj_val_es, status_es = solver.solve(
                tasks=es_tasks,
                task_graph=task_graph,
                running_tasks={},
                paths=paths,
                time_limit=30,
            )
            es_solver_ms = (time.perf_counter() - es_solver_start) * 1000.0
            es_total_ms = es_query_ms + es_solver_ms
            assignment_map_es = {
                tid: rt.node_id for tid, rt in assignments_es.items()
            }
            log_e2e(
                duration_ms=es_total_ms,
                curr_offset=float(epoch),
                tasks_to_schedule=len(es_tasks),
                ran_solver=True,
                metrics_source="elasticsearch",
                assignment=assignment_map_es,
                correlation_id=correlation_id,
            )
            logger.info(
                "ES solver: obj={} assigned={} leftover={} solver={:.2f}ms total={:.2f}ms",
                obj_val_es,
                len(assignments_es),
                len(leftover_es),
                es_solver_ms,
                es_total_ms,
            )

        # 7. Compare backends.
        if parallel_enabled:
            discrepancies = compare_node_metrics(
                sketch_metrics=sketch_metrics,
                es_metrics=es_metrics,
                tolerance=CONSISTENCY_CHECK_TOLERANCE,
            )
            if discrepancies:
                logger.warning(
                    "{} discrepancies between sketch and ES",
                    len(discrepancies),
                )
                for d in discrepancies[:5]:
                    logger.warning("  {}", d)
            log_node_metric_comparisons(
                correlation_id=correlation_id,
                sketch_metrics=sketch_metrics,
                es_metrics=es_metrics,
                sketch_top_entities=sketch_top,
                es_top_entities=es_top,
            )

    logger.info("Sweep complete ({} epochs).", args.end_epoch - args.start_epoch + 1)


if __name__ == "__main__":
    main()

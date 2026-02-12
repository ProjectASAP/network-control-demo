import csv
import datetime
import json
import os
import threading
from typing import Any
from loguru import logger

from config import E2E_LOG_CSV, QUERY_COMPARE_CSV, QUERY_RTT_CSV

_RTT_LOG_LOCK = threading.Lock()
_QUERY_COMPARE_LOG_LOCK = threading.Lock()
_E2E_LOG_LOCK = threading.Lock()


def log_record(log_path: str, **kwargs):
    # Generic logging function that can be extended for different log types.
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds") + "Z"
    log_entry = {"timestamp": timestamp, **kwargs}
    with _RTT_LOG_LOCK:
        try:
            needs_header = True
            try:
                needs_header = os.path.getsize(log_path) == 0
            except OSError:
                needs_header = True
            with open(log_path, "a", newline="") as handle:
                writer = csv.writer(handle)
                if needs_header:
                    writer.writerow(log_entry.keys())
                writer.writerow(log_entry.values())
        except Exception as exc:
            logger.error(f"failed to write RTT log: {exc}")


def log_rtt(
    request_id: str,
    correlation_id: str | None,
    request_type: str,
    target: str,
    duration_ms: float,
    status: int | None,
    ok: bool,
    error: str | None = None,
) -> None:
    # Append request timing and status info to the RTT CSV log.
    error_text = error or ""
    correlation_text = correlation_id or ""
    with _RTT_LOG_LOCK:
        try:
            needs_header = True
            try:
                needs_header = os.path.getsize(QUERY_RTT_CSV) == 0
            except OSError:
                needs_header = True
            with open(QUERY_RTT_CSV, "a", newline="") as handle:
                writer = csv.writer(handle)
                if needs_header:
                    writer.writerow(
                        [
                            "request_id",
                            "correlation_id",
                            "request_type",
                            "target",
                            "duration_ms",
                            "status",
                            "ok",
                            "error",
                        ]
                    )
                writer.writerow(
                    [
                        request_id,
                        correlation_text,
                        request_type,
                        target,
                        f"{duration_ms:.3f}",
                        "" if status is None else status,
                        "1" if ok else "0",
                        error_text,
                    ]
                )
        except Exception as exc:
            logger.error(f"failed to write RTT log: {exc}")


def _format_result(value: Any) -> str:
    # Normalize values for CSV logging.
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def _format_top_entities(items: list[Any]) -> str:
    # Serialize top-entity results as field=entity:value pairs.
    if not items:
        return ""
    parts = []
    for item in sorted(items, key=lambda entry: str(entry.field)):
        parts.append(f"{item.field}={item.entity_key}:{item.value:.6g}")
    return ";".join(parts)


def log_node_metric_comparisons(
    correlation_id: str | None,
    sketch_metrics: dict[str, Any],
    es_metrics: dict[str, Any],
    sketch_top_entities: list[Any] | None = None,
    es_top_entities: list[Any] | None = None,
) -> None:
    # Emit wide comparison rows for node metrics.
    all_node_ids = sorted(set(sketch_metrics.keys()) | set(es_metrics.keys()))
    header = [
        "correlation_id",
        "node_id",
        "cpu_p25_sk",
        "cpu_p25_es",
        "cpu_p50_sk",
        "cpu_p50_es",
        "cpu_p75_sk",
        "cpu_p75_es",
        "cpu_p90_sk",
        "cpu_p90_es",
        "memory_p25_sk",
        "memory_p25_es",
        "memory_p50_sk",
        "memory_p50_es",
        "memory_p75_sk",
        "memory_p75_es",
        "memory_p90_sk",
        "memory_p90_es",
        "network_p25_sk",
        "network_p25_es",
        "network_p50_sk",
        "network_p50_es",
        "network_p75_sk",
        "network_p75_es",
        "network_p90_sk",
        "network_p90_es",
        "cpu_sum_sk",
        "cpu_sum_es",
        "memory_sum_sk",
        "memory_sum_es",
        "network_sum_sk",
        "network_sum_es",
        "top_entity_sk",
        "top_entity_es",
    ]
    top_entity_sk = _format_top_entities(sketch_top_entities or [])
    top_entity_es = _format_top_entities(es_top_entities or [])

    with _QUERY_COMPARE_LOG_LOCK:
        try:
            try:
                needs_header = os.path.getsize(QUERY_COMPARE_CSV) == 0
            except OSError:
                needs_header = True
            with open(QUERY_COMPARE_CSV, "a", newline="") as handle:
                writer = csv.writer(handle)
                if needs_header:
                    writer.writerow(header)
                for node_id in all_node_ids:
                    sketch = sketch_metrics.get(node_id)
                    es = es_metrics.get(node_id)
                    sketch_cum = sketch.cumulative if sketch is not None else None
                    es_cum = es.cumulative if es is not None else None
                    writer.writerow(
                        [
                            "" if correlation_id is None else correlation_id,
                            node_id,
                            _format_result(None if sketch is None else sketch.cpu_p25),
                            _format_result(None if es is None else es.cpu_p25),
                            _format_result(None if sketch is None else sketch.cpu_p50),
                            _format_result(None if es is None else es.cpu_p50),
                            _format_result(None if sketch is None else sketch.cpu_p75),
                            _format_result(None if es is None else es.cpu_p75),
                            _format_result(None if sketch is None else sketch.cpu_p90),
                            _format_result(None if es is None else es.cpu_p90),
                            _format_result(None if sketch is None else sketch.memory_p25),
                            _format_result(None if es is None else es.memory_p25),
                            _format_result(None if sketch is None else sketch.memory_p50),
                            _format_result(None if es is None else es.memory_p50),
                            _format_result(None if sketch is None else sketch.memory_p75),
                            _format_result(None if es is None else es.memory_p75),
                            _format_result(None if sketch is None else sketch.memory_p90),
                            _format_result(None if es is None else es.memory_p90),
                            _format_result(None if sketch is None else sketch.network_p25),
                            _format_result(None if es is None else es.network_p25),
                            _format_result(None if sketch is None else sketch.network_p50),
                            _format_result(None if es is None else es.network_p50),
                            _format_result(None if sketch is None else sketch.network_p75),
                            _format_result(None if es is None else es.network_p75),
                            _format_result(None if sketch is None else sketch.network_p90),
                            _format_result(None if es is None else es.network_p90),
                            _format_result(None if sketch_cum is None else sketch_cum.cpu_cores),
                            _format_result(None if es_cum is None else es_cum.cpu_cores),
                            _format_result(None if sketch_cum is None else sketch_cum.memory_gb),
                            _format_result(None if es_cum is None else es_cum.memory_gb),
                            _format_result(None if sketch_cum is None else sketch_cum.network_mbps),
                            _format_result(None if es_cum is None else es_cum.network_mbps),
                            top_entity_sk,
                            top_entity_es,
                        ]
                    )
        except Exception as exc:
            print(f"failed to write query comparison log: {exc}")


def log_e2e(
    duration_ms: float,
    curr_offset: float,
    tasks_to_schedule: int,
    ran_solver: bool,
    metrics_source: str,
    assignment: dict[str, str] | None,
    correlation_id: str | None = None,
) -> None:
    # Write a single scheduler run record to the E2E CSV log.
    assignment_text = ""
    if assignment is not None:
        assignment_text = json.dumps(assignment, separators=(",", ":"), sort_keys=True)
    timestamp = datetime.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
    with _E2E_LOG_LOCK:
        try:
            needs_header = os.path.getsize(E2E_LOG_CSV) == 0
        except OSError:
            needs_header = True
        with open(E2E_LOG_CSV, "a", newline="") as handle:
            writer = csv.writer(handle)
            if needs_header:
                writer.writerow(
                    [
                        "timestamp",
                        "correlation_id",
                        "offset_s",
                        "tasks_to_schedule",
                        "ran_solver",
                        "metrics_source",
                        "duration_ms",
                        "assignment",
                    ]
                )
            writer.writerow(
                [
                    timestamp,
                    "" if correlation_id is None else correlation_id,
                    f"{curr_offset:.3f}",
                    str(tasks_to_schedule),
                    "1" if ran_solver else "0",
                    metrics_source,
                    f"{duration_ms:.3f}",
                    assignment_text,
                ]
            )

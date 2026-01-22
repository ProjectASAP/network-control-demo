import csv
import datetime
import json
import os
import threading
from typing import Any

from config import E2E_LOG_CSV, QUERY_COMPARE_CSV, QUERY_RTT_CSV

_RTT_LOG_LOCK = threading.Lock()
_QUERY_COMPARE_LOG_LOCK = threading.Lock()
_E2E_LOG_LOCK = threading.Lock()


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
            print(f"failed to write RTT log: {exc}")


def _format_result(value: Any) -> str:
    # Normalize values for CSV logging.
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def log_query_comparison(
    correlation_id: str | None,
    request_type: str,
    label: str,
    field: str,
    sketch_result: Any,
    es_result: Any,
) -> None:
    # Write a single sketch vs ES comparison row.
    with _QUERY_COMPARE_LOG_LOCK:
        try:
            try:
                needs_header = os.path.getsize(QUERY_COMPARE_CSV) == 0
            except OSError:
                needs_header = True
            with open(QUERY_COMPARE_CSV, "a", newline="") as handle:
                writer = csv.writer(handle)
                if needs_header:
                    writer.writerow(
                        [
                            "correlation_id",
                            "request_type",
                            "label",
                            "field",
                            "sketch_result",
                            "es_result",
                        ]
                    )
                writer.writerow(
                    [
                        "" if correlation_id is None else correlation_id,
                        request_type,
                        label,
                        field,
                        _format_result(sketch_result),
                        _format_result(es_result),
                    ]
                )
        except Exception as exc:
            print(f"failed to write query comparison log: {exc}")


def log_node_metric_comparisons(
    correlation_id: str | None,
    sketch_metrics: dict[str, Any],
    es_metrics: dict[str, Any],
) -> None:
    # Emit comparison rows for node metrics.
    all_node_ids = sorted(set(sketch_metrics.keys()) | set(es_metrics.keys()))
    for node_id in all_node_ids:
        sketch = sketch_metrics.get(node_id)
        es = es_metrics.get(node_id)
        label = node_id

        sketch_cpu = sketch.cpu_p50 if sketch is not None else None
        es_cpu = es.cpu_p50 if es is not None else None
        if sketch_cpu is not None or es_cpu is not None:
            log_query_comparison(
                correlation_id=correlation_id,
                request_type="node_metrics",
                label=label,
                field="cpu_cores_p50",
                sketch_result=sketch_cpu,
                es_result=es_cpu,
            )

        sketch_mem = sketch.memory_p50 if sketch is not None else None
        es_mem = es.memory_p50 if es is not None else None
        if sketch_mem is not None or es_mem is not None:
            log_query_comparison(
                correlation_id=correlation_id,
                request_type="node_metrics",
                label=label,
                field="memory_gb_p50",
                sketch_result=sketch_mem,
                es_result=es_mem,
            )

        sketch_net = sketch.network_p50 if sketch is not None else None
        es_net = es.network_p50 if es is not None else None
        if sketch_net is not None or es_net is not None:
            log_query_comparison(
                correlation_id=correlation_id,
                request_type="node_metrics",
                label=label,
                field="network_mbps_p50",
                sketch_result=sketch_net,
                es_result=es_net,
            )

        sketch_cumulative = sketch.cumulative if sketch is not None else None
        es_cumulative = es.cumulative if es is not None else None
        if sketch_cumulative is not None or es_cumulative is not None:
            log_query_comparison(
                correlation_id=correlation_id,
                request_type="node_metrics",
                label=label,
                field="cpu_cores_sum",
                sketch_result=(
                    None if sketch_cumulative is None else sketch_cumulative.cpu_cores
                ),
                es_result=None if es_cumulative is None else es_cumulative.cpu_cores,
            )
            log_query_comparison(
                correlation_id=correlation_id,
                request_type="node_metrics",
                label=label,
                field="memory_gb_sum",
                sketch_result=(
                    None if sketch_cumulative is None else sketch_cumulative.memory_gb
                ),
                es_result=None if es_cumulative is None else es_cumulative.memory_gb,
            )
            log_query_comparison(
                correlation_id=correlation_id,
                request_type="node_metrics",
                label=label,
                field="network_mbps_sum",
                sketch_result=(
                    None
                    if sketch_cumulative is None
                    else sketch_cumulative.network_mbps
                ),
                es_result=None if es_cumulative is None else es_cumulative.network_mbps,
            )


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

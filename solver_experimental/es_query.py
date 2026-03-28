import time
import uuid
import json
from dataclasses import dataclass
from loguru import logger
import requests
import httpx
import os

from config import (
    ES_API_KEY,
    ES_INDEX_NAME,
    ES_URL,
    ES_TIME_FIELD,
    SKETCH_API_KEY,
    SKETCH_URL,
)
from logging_utils import log_rtt, log_record

# Environment variables for ES query config.
NODE_LABEL = os.getenv("ES_NODE_LABEL", "cluster")
TASK_LABEL = os.getenv("ES_TASK_LABEL", "task")


def fetch_task_usage(
    task_ids: list[str],
    epoch: int,
    client: httpx.Client | None = None,
    use_es: bool = False,
    metrics: list[str] = ["cpu_cores", "memory_gb", "network_mbps"], 
    percentiles: list[int] = [0, 50, 90, 100],
    log_path: str = "fetch_tasks_rtt.csv",
) -> dict[str, float] | None:
    """
    Fetch task-level usage metrics and return a dict of metric values.
    """

    close_client = False
    if client is None:
        client = httpx.Client()
        close_client = True

    try:
        task_metrics, elapsed_ms = fetch_task_metrics(
            client=client,
            task_ids=task_ids,
            epoch=epoch,
            use_es=use_es,
            metrics=metrics,
            percentiles=percentiles
        )
    except Exception as e:
        logger.warning(f"Failed to get task metrics for {task_ids}: {e}")
        return None
    finally:
        if close_client:
            client.close() # type: ignore

    # Log the RTT for this query, along with the number of tasks and which backend was used.
    server = "ES" if use_es else "sketch"
    log_data = dict(
        epoch=epoch,
        request_id="batch_query", 
        duration_ms=elapsed_ms, 
        backend=server, 
        task_count=len(task_ids)
    )
    log_record(log_path=log_path, **log_data)

    return task_metrics


def fetch_task_metrics(
    client: httpx.Client, 
    task_ids: list[str], 
    epoch: int,
    use_es: bool, 
    metrics: list[str], 
    percentiles: list[int] 
) -> tuple[dict[str, float], float]:
    server = "ES" if use_es else "sketch"
    try:
        if use_es:
            data, elapsed_ms = query_es_tasks(
                client=client,
                es_url=ES_URL,
                es_index=ES_INDEX_NAME,
                api_key=ES_API_KEY,
                tasks=task_ids,
                metrics=metrics,
                percentiles=percentiles,
                connect_timeout=0.5,
                read_timeout=2.0,
                epoch=epoch,
            )
        else:
            data, elapsed_ms = query_server_batch(
                client=client,
                server_url=SKETCH_URL,
                epoch=epoch,
                task_ids=task_ids,
                metrics=metrics,
                percentiles=percentiles,
                connect_timeout=0.5,
                read_timeout=2.0,
            )
    except Exception as e:
        logger.warning(f"{server} batch query failed: {e}")
        return {}, 0.0
    
    logger.debug(f"{server} batch query took {elapsed_ms:.1f} ms")
    return data.get("results", {}), elapsed_ms


def query_server_batch(
    client: httpx.Client,
    server_url: str,
    epoch: int, # Sketch server does not support epoch indexing.
    task_ids: list[str],
    metrics: list[str],
    percentiles: list[int],
    connect_timeout: float,
    read_timeout: float,
) -> tuple[dict, float]:
    url = f"{server_url}/cluster-metrics/_batch"
    payload = {
        "keys": task_ids,
        "fields": metrics,
        "aggs": ["percentiles"],
        "percents": percentiles,
    }
    t0 = time.perf_counter()
    resp = client.post(
        url,
        json=payload,
        timeout=(connect_timeout, read_timeout),
    )
    resp.raise_for_status()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return resp.json(), elapsed_ms


def query_es_tasks(
    client: httpx.Client,
    es_url: str,
    es_index: str,
    api_key: str | None,
    tasks: list[str],
    connect_timeout: float,
    read_timeout: float,
    epoch: int,
    metrics: list[str],
    percentiles: list[int]
) -> tuple[dict, float]:
    
    def es_headers(api_key: str | None) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"ApiKey {api_key}"
        return headers
    
    headers = es_headers(api_key)
    url = f"{es_url}/{es_index}/_search"
    results: dict[str, dict[str, object]] = {}
    t0 = time.perf_counter()

    aggs = {}
    for metric in metrics:
        aggs[metric] = {"percentiles": {"field": metric, "percents": percentiles}}

    for tid in tasks:
        payload = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {TASK_LABEL: tid}},
                        {"term": {"epoch": epoch}},
                    ]
                }
            },
            "aggs": aggs,
        }
        resp = client.post(
            url,
            headers=headers,
            json=payload,
            timeout=(connect_timeout, read_timeout),
        )
        resp.raise_for_status()
        results[tid] = resp.json()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return results, elapsed_ms


def _get_agg_value(container: dict, key: str) -> float | None:
    # Extract a numeric value from an aggregation dict.
    agg = container.get(key)
    if not isinstance(agg, dict):
        return None
    value = agg.get("value")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def pick_percentile(
    values: dict, percentile: float, default: float | None
) -> float | None:
    # Resolve percentile values that may use stringified numeric keys.
    direct = values.get(str(percentile))
    if direct is not None:
        return direct
    if percentile.is_integer():
        direct_int = values.get(str(int(percentile)))
        if direct_int is not None:
            return direct_int
    alt = values.get(f"{percentile:.1f}")
    if alt is not None:
        return alt
    numeric = values.get(percentile)
    if numeric is not None:
        return numeric
    if percentile.is_integer():
        numeric_int = values.get(int(percentile))
        if numeric_int is not None:
            return numeric_int
    return default


def check_es_available(timeout_s: float = 1.0) -> bool:
    # Ping Elasticsearch to decide whether direct queries are possible.
    es_url = ES_URL.strip()
    if not es_url:
        return False
    headers = {}
    if ES_API_KEY:
        headers["Authorization"] = f"ApiKey {ES_API_KEY}"
    try:
        response = requests.get(es_url, headers=headers, timeout=timeout_s)
        return response.ok
    except Exception:
        return False



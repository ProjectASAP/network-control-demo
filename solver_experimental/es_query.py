from elasticsearch import Elasticsearch
import time
import csv
import uuid
import threading
from itertools import combinations
from dataclasses import dataclass
from loguru import logger
import requests
import os
from urllib.parse import urlparse

from scheduler.entities import RunningTask


# Environment variables for ES query config.
NODE_LABEL = os.getenv('ES_NODE_LABEL', 'cluster.keyword')
TASK_LABEL = os.getenv('ES_TASK_LABEL', 'task.keyword')
ES_URL = os.getenv('ES_URL', 'http://localhost:10101')
ES_INDEX_NAME = os.getenv('ES_INDEX_NAME', 'cluster-metrics')
ES_API_KEY = os.getenv('ES_API_KEY', 'TWg0S01wc0JhR1AxOFVUcUY5N2w6bGR0TjIySHRZTHVwdmZLTmtqcGtGQQ==')
ES_BACKEND_URL = os.getenv('ES_BACKEND_URL', 'http://localhost:9200')
ES_BACKEND_API_KEY = os.getenv('ES_BACKEND_API_KEY', ES_API_KEY)
ES_BACKEND_TIMEOUT = float(os.getenv('ES_BACKEND_TIMEOUT', '2.0'))
FREQUENCY_TOLERANCE = float(os.getenv('ES_FREQUENCY_TOLERANCE', '0.5'))
_ES_BACKEND_CLIENT: Elasticsearch | None = None
RTT_LOG_PATH = os.getenv('QUERY_RTT_CSV', 'query_rtt.csv')
_RTT_LOG_LOCK = threading.Lock()


@dataclass
class TopEntityResult:
    """Result from top_entities query - entity with highest value for a field."""
    field: str
    entity_key: str
    value: float


@dataclass
class CumulativeResult:
    """Result from cumulative query - total resource usage for a task."""
    cpu_cores: float
    memory_gb: float
    network_mbps: float


@dataclass
class FrequencyResult:
    """Result from frequency query - how often a value appears."""
    field: str
    value: float
    count: int


@dataclass
class TaskMetricsSnapshot:
    """Aggregated metrics for a single task from all query types."""
    task_id: str
    node_id: str
    cpu_p50: float | None = None
    memory_p50: float | None = None
    cumulative: CumulativeResult | None = None
    frequency_results: list[FrequencyResult] | None = None


def update_tasks_with_metrics(
    running_tasks: dict[str, RunningTask],
    session: requests.Session = None,
    quantiles: list[int] = None
) -> dict[str, TaskMetricsSnapshot]:
    """
    Update running tasks with metrics from all query types and return snapshots.
    """
    if session is None:
        session = requests.Session()
    if quantiles is None:
        quantiles = [50]

    snapshots: dict[str, TaskMetricsSnapshot] = {}

    top_entities: list[TopEntityResult] = []
    if running_tasks:
        try:
            top_entities = get_top_entities(session=session)
            for top in top_entities:
                logger.debug(f"Top entity for {top.field}: {top.entity_key} = {top.value}")
        except Exception as e:
            logger.warning(f"Failed to get top entities: {e}")

    for task_id, running_task in running_tasks.items():
        node_id = running_task.node_id
        snapshot = TaskMetricsSnapshot(task_id=task_id, node_id=node_id)

        try:
            metric_quantiles = get_metric_quantiles(
                node_id=node_id,
                task_id=task_id,
                session=session,
                quantiles=quantiles,
            )
            cpu_quantiles = metric_quantiles.get('cpu', {})
            memory_quantiles = metric_quantiles.get('memory', {})

            snapshot.cpu_p50 = pick_percentile(cpu_quantiles, 50.0, None)
            snapshot.memory_p50 = pick_percentile(memory_quantiles, 50.0, None)
        except Exception as e:
            logger.error(f'Error fetching quantiles for Task {task_id} on Node {node_id}: {e}')

        try:
            snapshot.cumulative = get_cumulative_usage(
                node_id=node_id,
                task_id=task_id,
                session=session,
            )
            logger.debug(
                "Cumulative for %s: CPU=%s, Memory=%s, Network=%s",
                task_id,
                snapshot.cumulative.cpu_cores,
                snapshot.cumulative.memory_gb,
                snapshot.cumulative.network_mbps,
            )
        except Exception as e:
            logger.warning(f'Error fetching cumulative for Task {task_id}: {e}')

        try:
            snapshot.frequency_results = get_resource_frequency(
                node_id=node_id,
                task_id=task_id,
                cpu_value=running_task.task.initial_cpu,
                memory_value=running_task.task.initial_memory,
                session=session,
            )
            for freq in snapshot.frequency_results:
                logger.debug(
                    "Frequency for %s %s=%s: count=%s",
                    task_id,
                    freq.field,
                    freq.value,
                    freq.count,
                )
        except Exception as e:
            logger.warning(f'Error fetching frequency for Task {task_id}: {e}')

        _apply_metrics_to_task(running_task, snapshot, top_entities)
        snapshots[task_id] = snapshot

    return snapshots


def _apply_metrics_to_task(
    running_task: RunningTask,
    snapshot: TaskMetricsSnapshot,
    top_entities: list[TopEntityResult]
) -> None:
    """
    Apply collected metrics to update task resource estimates.
    """
    initial_cpu = running_task.task.initial_cpu
    initial_memory = running_task.task.initial_memory

    if snapshot.cpu_p50 is not None:
        running_task.task.initial_cpu = snapshot.cpu_p50
    if snapshot.memory_p50 is not None:
        running_task.task.initial_memory = snapshot.memory_p50

    if snapshot.frequency_results:
        for freq in snapshot.frequency_results:
            if freq.count == 0:
                logger.warning(
                    "Task %s: %s=%s has zero frequency - estimate may be unusual",
                    snapshot.task_id,
                    freq.field,
                    freq.value,
                )

    task_key = f"{snapshot.node_id};{snapshot.task_id}"
    for top in top_entities:
        if top.entity_key == task_key:
            logger.info(
                "Task %s is top consumer for %s: %s",
                snapshot.task_id,
                top.field,
                top.value,
            )

    if (
        running_task.task.initial_cpu != initial_cpu
        or running_task.task.initial_memory != initial_memory
    ):
        logger.debug(
            "Updated Task %s - CPU: %s -> %s, Memory: %s -> %s",
            snapshot.task_id,
            initial_cpu,
            running_task.task.initial_cpu,
            initial_memory,
            running_task.task.initial_memory,
        )


def update_tasks_with_quantiles(
    running_tasks: dict[str, RunningTask],
    session=None,
    quantiles=None
):
    """
    Deprecated: Use update_tasks_with_metrics instead.
    """
    return update_tasks_with_metrics(running_tasks, session, quantiles)


def get_metric_quantiles(node_id: str, task_id: str, session=None, quantiles=None):
    """
    Get quantiles of CPU and memory usage for a given task on a node from sketchlib backed ES server.
    """

    if session is None:
        session = requests.Session()
    if quantiles is None:
        quantiles = [50]

    url = ES_URL
    index_name = ES_INDEX_NAME
    api_key = ES_API_KEY
    query = {
        'bool': {
            'must': [
                {'term': {NODE_LABEL: node_id}},
                {'term': {TASK_LABEL: task_id}},
                # {'range': {'@timestamp': {'gte': 'now-30s', 'lt': 'now'}}}
            ]
        }
    }
    aggs = {
        "cpu_quantiles": {
            "percentiles": {
                "field": "cpu_cores", 
                "percents": quantiles
            }
        },
        "memory_quantiles": {
            "percentiles": {
                "field": "memory_gb", 
                "percents": quantiles
            }
        }
    }

    response = send_search_request(
        session=session,
        request_type="percentile",
        query=query,
        aggs=aggs,
        url=url,
        index_name=index_name,
        api_key=api_key,
        also_es=True,
    )
    if response is None:
        raise RuntimeError("percentile request failed")

    output = response
    cpu_quantiles = output['aggregations']['cpu_quantiles']['values']
    memory_quantiles = output['aggregations']['memory_quantiles']['values']

    return {
        'cpu': cpu_quantiles,
        'memory': memory_quantiles
    }


def get_top_entities(
    session: requests.Session = None,
    fields: list[str] = None
) -> list[TopEntityResult]:
    """
    Query the top entity (highest value) for each specified metric field.
    """
    if session is None:
        session = requests.Session()
    if fields is None:
        fields = ["cpu_cores", "memory_gb", "network_mbps"]

    results = []
    for field in fields:
        aggs = {
            f"top_{field}": {"top_entities": {"field": field}}
        }
        response = send_search_request(
            session=session,
            request_type="top_entities",
            query=None,
            aggs=aggs,
            url=ES_URL,
            index_name=ES_INDEX_NAME,
            api_key=ES_API_KEY,
            also_es=True,
        )
        if response is not None:
            agg_result = response.get("aggregations", {}).get(f"top_{field}", {})
            entity_key = agg_result.get("key", "")
            value = agg_result.get("value", 0.0)
            if entity_key:
                results.append(
                    TopEntityResult(
                        field=field,
                        entity_key=entity_key,
                        value=float(value),
                    )
                )

    return results


def get_cumulative_usage(
    node_id: str,
    task_id: str,
    session: requests.Session = None
) -> CumulativeResult:
    """
    Query cumulative (total sum) resource usage for a specific task on a node.
    """
    if session is None:
        session = requests.Session()

    key = f"{node_id};{task_id}"
    fields = ["cpu_cores", "memory_gb", "network_mbps"]
    aggs = {
        f"{field}_cumulative": {"cumulative": {"field": field, "key": key}}
        for field in fields
    }

    response = send_search_request(
        session=session,
        request_type="cumulative",
        query=None,
        aggs=aggs,
        url=ES_URL,
        index_name=ES_INDEX_NAME,
        api_key=ES_API_KEY,
        also_es=True,
    )

    if response is None:
        return CumulativeResult(cpu_cores=0.0, memory_gb=0.0, network_mbps=0.0)

    aggregations = response.get("aggregations", {})
    return CumulativeResult(
        cpu_cores=float(aggregations.get("cpu_cores_cumulative", {}).get("value", 0)),
        memory_gb=float(aggregations.get("memory_gb_cumulative", {}).get("value", 0)),
        network_mbps=float(aggregations.get("network_mbps_cumulative", {}).get("value", 0)),
    )


def get_resource_frequency(
    node_id: str,
    task_id: str,
    cpu_value: float = None,
    memory_value: float = None,
    network_value: float = None,
    session: requests.Session = None
) -> list[FrequencyResult]:
    """
    Query how frequently a task uses specific resource values.
    """
    if session is None:
        session = requests.Session()

    key = f"{node_id};{task_id}"
    aggs = {}
    value_map = {
        "cpu_cores": cpu_value,
        "memory_gb": memory_value,
        "network_mbps": network_value,
    }

    for field, value in value_map.items():
        if value is not None:
            aggs[f"{field}_frequency"] = {
                "frequency": {"field": field, "key": key, "value": value}
            }

    if not aggs:
        return []

    response = send_search_request(
        session=session,
        request_type="frequency",
        query=None,
        aggs=aggs,
        url=ES_URL,
        index_name=ES_INDEX_NAME,
        api_key=ES_API_KEY,
        also_es=True,
    )

    if response is None:
        return []

    results = []
    aggregations = response.get("aggregations", {})
    for field, value in value_map.items():
        if value is not None:
            agg_key = f"{field}_frequency"
            agg_result = aggregations.get(agg_key, {})
            count = int(agg_result.get("count", 0))
            results.append(FrequencyResult(field=field, value=value, count=count))

    return results


def send_top_entities_query(session=None) -> None:
    get_top_entities(session=session)


def send_cumulative_query(node_id: str, task_id: str, session=None) -> None:
    get_cumulative_usage(node_id=node_id, task_id=task_id, session=session)


def send_frequency_query(node_id: str, task_id: str, value: float, session=None) -> None:
    get_resource_frequency(
        node_id=node_id,
        task_id=task_id,
        cpu_value=value,
        session=session,
    )


def send_search_request(
    session: requests.Session,
    request_type: str,
    query: dict | None,
    aggs: dict,
    url: str,
    index_name: str,
    api_key: str,
    also_es: bool,
) -> dict | None:
    request_id = uuid.uuid4().hex
    payload = {
        "size": 0,
        "aggs": aggs,
    }
    if query is not None:
        payload["query"] = query

    endpoint = f'{url}/{index_name}/_search'
    headers = {
        "Authorization": f"ApiKey {api_key}",
        "Content-Type": "application/json",
        "X-Request-Id": request_id,
        "X-Request-Type": request_type,
    }

    if also_es:
        send_backend_es_query(
            request_id=request_id,
            request_type=request_type,
            query=query or {},
            aggs=aggs,
            index_name=index_name,
        )

    start_t = time.perf_counter()
    try:
        response = session.post(endpoint, json=payload, headers=headers)
        duration_ms = (time.perf_counter() - start_t) * 1000.0
        log_rtt(
            request_id=request_id,
            request_type=request_type,
            target="server",
            duration_ms=duration_ms,
            status=response.status_code,
            ok=response.ok,
        )
        if not response.ok:
            return None
        return response.json()
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_t) * 1000.0
        log_rtt(
            request_id=request_id,
            request_type=request_type,
            target="server",
            duration_ms=duration_ms,
            status=None,
            ok=False,
            error=str(exc),
        )
        return None


def pick_percentile(values: dict, percentile: float, default: float | None) -> float | None:
    direct = values.get(str(percentile))
    if direct is not None:
        return direct
    alt = values.get(f"{percentile:.1f}")
    if alt is not None:
        return alt
    return default


def send_backend_es_query(
    request_id: str,
    request_type: str,
    query: dict,
    aggs: dict,
    index_name: str,
) -> None:
    client = get_backend_es_client()
    if client is None:
        return
    es_query, es_aggs = build_backend_es_request(request_type, query, aggs)
    if es_aggs is None:
        return
    if es_query is None:
        es_query = {"match_all": {}}
    start_t = time.perf_counter()
    try:
        response = client.search(index=index_name, query=es_query, aggs=es_aggs, size=0)
        duration_ms = (time.perf_counter() - start_t) * 1000.0
        status = getattr(getattr(response, "meta", None), "status", None)
        log_rtt(
            request_id=request_id,
            request_type=request_type,
            target="es",
            duration_ms=duration_ms,
            status=status,
            ok=True,
        )
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_t) * 1000.0
        log_rtt(
            request_id=request_id,
            request_type=request_type,
            target="es",
            duration_ms=duration_ms,
            status=None,
            ok=False,
            error=str(exc),
        )
        logger.warning(f'ES backend query failed: {exc}')


def build_backend_es_request(
    request_type: str,
    query: dict,
    aggs: dict,
) -> tuple[dict | None, dict | None]:
    if request_type == "percentile":
        return query, aggs

    if request_type == "top_entities":
        agg_name, field = extract_agg_field(aggs, "top_entities")
        if field is None or agg_name is None:
            return None, None
        script = (
            f"if (doc['{NODE_LABEL}'].size()==0 || doc['{TASK_LABEL}'].size()==0) "
            "{return 'unknown;unknown';} "
            f"return doc['{NODE_LABEL}'].value + ';' + doc['{TASK_LABEL}'].value;"
        )
        es_aggs = {
            agg_name: {
                "terms": {
                    "script": {"lang": "painless", "source": script},
                    "size": 1,
                    "order": {"max_value": "desc"},
                },
                "aggs": {
                    "max_value": {"max": {"field": field}},
                },
            }
        }
        return None, es_aggs

    if request_type == "cumulative":
        agg_name, field = extract_agg_field(aggs, "cumulative")
        key = extract_agg_key(aggs, "cumulative")
        if field is None or agg_name is None or key is None:
            return None, None
        filters = build_key_filters(key)
        es_query = {"bool": {"filter": filters}} if filters else None
        es_aggs = {
            agg_name: {
                "sum": {"field": field},
            }
        }
        return es_query, es_aggs

    if request_type == "frequency":
        agg_name, field = extract_agg_field(aggs, "frequency")
        key = extract_agg_key(aggs, "frequency")
        value = extract_agg_value(aggs, "frequency")
        if field is None or agg_name is None or key is None or value is None:
            return None, None
        rounded = int(round(value))
        min_value = rounded - FREQUENCY_TOLERANCE
        max_value = rounded + FREQUENCY_TOLERANCE
        filters = build_key_filters(key)
        filters.append({
            "range": {field: {"gte": min_value, "lte": max_value}}
        })
        es_query = {"bool": {"filter": filters}}
        es_aggs = {
            agg_name: {
                "value_count": {"field": field},
            }
        }
        return es_query, es_aggs

    return None, None


def extract_agg_field(aggs: dict, agg_type: str) -> tuple[str | None, str | None]:
    for name, agg in aggs.items():
        spec = agg.get(agg_type)
        if isinstance(spec, dict):
            field = spec.get("field")
            return name, field
    return None, None


def extract_agg_key(aggs: dict, agg_type: str) -> str | None:
    for agg in aggs.values():
        spec = agg.get(agg_type)
        if isinstance(spec, dict):
            key = spec.get("key")
            if isinstance(key, str):
                return key
    return None


def extract_agg_value(aggs: dict, agg_type: str) -> float | None:
    for agg in aggs.values():
        spec = agg.get(agg_type)
        if isinstance(spec, dict):
            value = spec.get("value")
            if isinstance(value, (int, float)):
                return float(value)
    return None


def build_key_filters(key: str) -> list[dict]:
    key = key.strip()
    if not key:
        return []
    cluster = None
    task = None
    if ";" in key:
        parts = key.split(";", 1)
        cluster = parts[0].strip()
        task = parts[1].strip()
    else:
        cluster = key
    filters: list[dict] = []
    if cluster:
        filters.append({"term": {NODE_LABEL: cluster}})
    if task:
        filters.append({"term": {TASK_LABEL: task}})
    return filters


def get_backend_es_client() -> Elasticsearch | None:
    global _ES_BACKEND_CLIENT
    if _ES_BACKEND_CLIENT is not None:
        return _ES_BACKEND_CLIENT

    backend_url = ES_BACKEND_URL.strip()
    if not backend_url:
        return None

    parsed = urlparse(backend_url)
    scheme = parsed.scheme or "http"
    host = parsed.hostname or "localhost"
    port = parsed.port or 9200
    api_key = ES_BACKEND_API_KEY.strip() if ES_BACKEND_API_KEY else None

    try:
        _ES_BACKEND_CLIENT = Elasticsearch(
            hosts=[{"host": host, "port": port, "scheme": scheme}],
            api_key=api_key,
            request_timeout=ES_BACKEND_TIMEOUT,
        )
    except Exception as exc:
        logger.warning(f'Failed to create ES backend client: {exc}')
        return None

    return _ES_BACKEND_CLIENT


def log_rtt(
    request_id: str,
    request_type: str,
    target: str,
    duration_ms: float,
    status: int | None,
    ok: bool,
    error: str | None = None,
) -> None:
    error_text = error or ""
    with _RTT_LOG_LOCK:
        try:
            needs_header = True
            try:
                needs_header = os.path.getsize(RTT_LOG_PATH) == 0
            except OSError:
                needs_header = True
            with open(RTT_LOG_PATH, "a", newline="") as handle:
                writer = csv.writer(handle)
                if needs_header:
                    writer.writerow([
                        "request_id",
                        "request_type",
                        "target",
                        "duration_ms",
                        "status",
                        "ok",
                        "error",
                    ])
                writer.writerow([
                    request_id,
                    request_type,
                    target,
                    f"{duration_ms:.3f}",
                    "" if status is None else status,
                    "1" if ok else "0",
                    error_text,
                ])
        except Exception as exc:
            logger.warning(f"failed to write RTT log: {exc}")


def query_elasticsearch():
    """
    Example function to query Elasticsearch for task metrics.
    """


    # Elasticsearch vs Sketch lib rust.
    client = Elasticsearch(
        hosts=[{"host": "localhost", "port": 9200, "scheme": "http"}],
        api_key=ES_API_KEY
    )
    index_name = ES_INDEX_NAME
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

from elasticsearch import Elasticsearch
import time
import csv
import uuid
import threading
from itertools import combinations
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


def update_tasks_with_quantiles(
        running_tasks: dict[str, RunningTask],
        session=None,
        quantiles=None
    ):
    """
    Update running tasks with CPU and memory quantiles fetched from ES.
    Args:
        running_tasks: Dictionary of task ids (str) and their corresponding RunningTask objects.
        session: Optional requests session for making HTTP requests.
        quantiles: Optional list of quantiles to fetch.
    """
    if session is None:
        session = requests.Session()
    quantiles = [50]

    if running_tasks:
        send_top_entities_query(session=session)

    for task_id, running_task in running_tasks.items():
        node_id = running_task.node_id
        try:
            metric_quantiles = get_metric_quantiles(
                node_id=node_id,
                task_id=task_id,
                session=session,
                quantiles=quantiles,
            )
        except Exception as e:
            logger.error(f'Error fetching quantiles for Task {task_id} on Node {node_id}: {e}')
            continue

        send_cumulative_query(node_id=node_id, task_id=task_id, session=session)
        send_frequency_query(
            node_id=node_id,
            task_id=task_id,
            value=running_task.task.initial_cpu,
            session=session,
        )

        cpu_quantiles = metric_quantiles.get('cpu', {})
        memory_quantiles = metric_quantiles.get('memory', {})
        if not cpu_quantiles or not memory_quantiles:
            logger.warning(f'No quantiles found for Task {task_id} on Node {node_id}. Skipping update.')
            continue

        initial_cpu = running_task.task.initial_cpu
        initial_memory = running_task.task.initial_memory

        median_cpu = pick_percentile(cpu_quantiles, 50.0, initial_cpu)
        median_memory = pick_percentile(memory_quantiles, 50.0, initial_memory)

        running_task.task.initial_cpu = median_cpu
        running_task.task.initial_memory = median_memory

        logger.debug(f"Updated Task {task_id} on Node {node_id} - CPU: {initial_cpu} -> {median_cpu}, Memory: {initial_memory} -> {median_memory}")


def get_metric_quantiles(node_id: str, task_id: str, session=None, quantiles=None):
    """
    Get quantiles of CPU and memory usage for a given task on a node from sketchlib backed ES server.
    """

    if session is None:
        session = requests.Session()
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


def send_top_entities_query(session=None) -> None:
    if session is None:
        session = requests.Session()
    aggs = {
        "top_cpu": {"top_entities": {"field": "cpu_cores"}}
    }
    send_search_request(
        session=session,
        request_type="top_entities",
        query=None,
        aggs=aggs,
        url=ES_URL,
        index_name=ES_INDEX_NAME,
        api_key=ES_API_KEY,
        also_es=True,
    )


def send_cumulative_query(node_id: str, task_id: str, session=None) -> None:
    if session is None:
        session = requests.Session()
    key = f"{node_id};{task_id}"
    aggs = {
        "cpu_cumulative": {"cumulative": {"field": "cpu_cores", "key": key}}
    }
    send_search_request(
        session=session,
        request_type="cumulative",
        query=None,
        aggs=aggs,
        url=ES_URL,
        index_name=ES_INDEX_NAME,
        api_key=ES_API_KEY,
        also_es=True,
    )


def send_frequency_query(node_id: str, task_id: str, value: float, session=None) -> None:
    if session is None:
        session = requests.Session()
    key = f"{node_id};{task_id}"
    aggs = {
        "cpu_frequency": {
            "frequency": {"field": "cpu_cores", "key": key, "value": value}
        }
    }
    send_search_request(
        session=session,
        request_type="frequency",
        query=None,
        aggs=aggs,
        url=ES_URL,
        index_name=ES_INDEX_NAME,
        api_key=ES_API_KEY,
        also_es=True,
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


def pick_percentile(values: dict, percentile: float, default: float) -> float:
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

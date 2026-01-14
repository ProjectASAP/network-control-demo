from elasticsearch import Elasticsearch
import time
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
_ES_BACKEND_CLIENT: Elasticsearch | None = None


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
    if quantiles is None:
        quantiles = [10 * i for i in range(1, 10)]

    for task_id, running_task in running_tasks.items():
        node_id = running_task.node_id
        try:
            metric_quantiles = get_metric_quantiles(node_id=node_id, task_id=task_id)
        except Exception as e:
            logger.error(f'Error fetching quantiles for Task {task_id} on Node {node_id}: {e}')
            continue

        cpu_quantiles = metric_quantiles.get('cpu', {})
        memory_quantiles = metric_quantiles.get('memory', {})
        if not cpu_quantiles or not memory_quantiles:
            logger.warning(f'No quantiles found for Task {task_id} on Node {node_id}. Skipping update.')
            continue

        initial_cpu = running_task.task.initial_cpu
        initial_memory = running_task.task.initial_memory

        median_cpu = metric_quantiles.get('50.0', initial_cpu)
        median_memory = metric_quantiles.get('50.0', initial_memory)

        running_task.task.initial_cpu = median_cpu
        running_task.task.initial_memory = median_memory

        logger.debug(f"Updated Task {task_id} on Node {node_id} - CPU: {initial_cpu} -> {median_cpu}, Memory: {initial_memory} -> {median_memory}")


def get_metric_quantiles(node_id: str, task_id: str, session=None, quantiles=None):
    """
    Get quantiles of CPU and memory usage for a given task on a node from sketchlib backed ES server.
    """

    if session is None:
        session = requests.Session()
    if quantiles is None:
        quantiles = [10 * i for i in range(1, 10)]

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

    payload = {
        "size": 0,
        "query": query,
        "aggs": aggs
    }

    send_backend_es_query(query=query, aggs=aggs, index_name=index_name)

    endpoint = f'{url}/{index_name}/_search'
    response = session.post(endpoint, json=payload, headers={
        "Authorization": f"ApiKey {api_key}",
        "Content-Type": "application/json"
    })

    output = response.json()
    cpu_quantiles = output['aggregations']['cpu_quantiles']['values']
    memory_quantiles = output['aggregations']['memory_quantiles']['values']

    return {
        'cpu': cpu_quantiles,
        'memory': memory_quantiles
    }


def send_backend_es_query(query: dict, aggs: dict, index_name: str) -> None:
    client = get_backend_es_client()
    if client is None:
        return
    try:
        client.search(index=index_name, query=query, aggs=aggs, size=0)
    except Exception as exc:
        logger.warning(f'ES backend query failed: {exc}')


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

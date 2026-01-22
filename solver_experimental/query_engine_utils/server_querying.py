import os
import sys
import yaml
import time
import requests
import argparse
import datetime
import numpy as np
import logging
from itertools import combinations
from collections import deque
from dataclasses import dataclass

folder_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(folder_path)

# import urllib3
from loguru import logger
from typing import Dict
from elasticsearch import Elasticsearch
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import (
    QueryManagerConfig,
    QueryGroupConfig,
    ServerType,
    ServerConfig,
    QueryResult,
)
from .update_task_info import UPDATE_METHOD_NAME_MAPPING
from scheduler.entities import RunningTask, Task


class QueryManager:
    """
    Handles making PromQL queries to the Query Engine of choice (Prometheus or Elasticsearch), and updating task information based on those queries.
    """

    def __init__(self, query_config: QueryManagerConfig) -> None:
        self.clients = {}
        for stype in ServerType:
            server_config = query_config.server_configs[stype]
            if stype == ServerType.PROMETHEUS:
                self.clients[stype] = PromQLClient(server_config)
            elif stype == ServerType.ELASTICSEARCH:
                self.clients[stype] = ElasticsearchClient(server_config)
        self.query_config = query_config

    def execute_queries(self) -> dict[int, list["QueryResult"]]:
        """
        Execute all query groups and return their results. If no data is fetched for a specific query group, the group ID will not be present.

        Returns:
            A dictionary mapping query group IDs to their respective results.
        """
        all_results: dict[int, list["QueryResult"]] = {}
        for query_group in self.query_config.query_groups:
            logger.info(f"Executing Query Group ID: {query_group.id}")
            group_results = self.execute_query_group(query_group)
            if group_results:
                all_results[query_group.id] = group_results
        return all_results

    def execute_query_group(self, query_group: QueryGroupConfig) -> list["QueryResult"]:
        """
        Execute a single query group and return its results. If no data is fetched for a specific query, the query string will not be present.

        Returns:
            A dictionary mapping query strings to their respective results (follows Prometheus format).
        """
        results = []
        delay = max(query_group.options.get("query_time_offset", 0), 0)
        for query in query_group.queries:
            backend = query_group.backend
            client = self.clients[backend]
            try:
                logger.debug(f"Executing query on {backend.value} server: {query}")
                res = client.query(query)
                results.append(res)
                logger.trace(f"Query Response Data: {res}")
            except Exception as e:
                logger.error(f"Error executing query: {e}")
                continue
            # Process data as needed
            if delay > 0:
                time.sleep(delay)
        return results

    def update_task_metrics(self, running_tasks: dict[str, RunningTask]) -> None:
        """
        Update the metrics of running tasks based on query results and update rules (which may be a udf).
        """
        if not running_tasks:
            logger.debug(
                "No running tasks found. Skipping task resource specification updates."
            )
            return

        query_results = self.execute_queries()
        if not query_results:
            logger.warning(
                "No query results found. Skipping task resource specification updates (this could lead to stale resource requirement estimates)."
            )
            return

        update_rules = self.query_config.task_update_rules
        for rule in update_rules:
            query_group_id = rule.query_group_id
            if query_group_id not in query_results:
                logger.warning(f"Query Group ID {query_group_id} not found in results.")
                continue

            update_func = UPDATE_METHOD_NAME_MAPPING[rule.update_method]
            group_results = query_results[query_group_id]

            update_func(group_results, running_tasks, **rule.options)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for client in self.clients.values():
            client.close()


class PromQLClient:
    """
    Client for executing PromQL queries against a Prometheus server.
    """

    def __init__(self, server_config: ServerConfig) -> None:
        self.server_url = server_config.url
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        task_label = server_config.options.get("task_label")
        self.task_label = task_label if task_label else "task_id"

    def query(self, query: str) -> QueryResult:
        """
        Execute a single PromQL query and return its result.

        Returns:
            The result of the PromQL query (follows Prometheus format).
        """
        endpoint = self.server_url
        response = self.session.get(endpoint, params={"query": query})
        response.raise_for_status()
        data = response.json()
        logger.debug(f'PromQL Query (status={data["status"]}): {query}')

        # Process Prometheus response data.
        if data["status"] == "success":
            logger.trace(f"Query Response Data: {data}")
            raw = data["data"]
            res = self._process_response(data=raw, query=query)
            return res

        return QueryResult(query=query, buckets=[])

    def _process_response(self, data: dict, query: str) -> QueryResult:
        task_label = self.task_label
        result = data["result"]
        result_type = data["resultType"]
        buckets = []
        if result_type == "vector":
            for metric in result:
                task_id = (
                    metric["metric"][task_label]
                    if task_label in metric["metric"]
                    else None
                )
                value = float(metric["value"][1])
                buckets.append(QueryResult.Bucket(task_id=task_id, value=value))
        elif result_type == "scalar":
            value = float(result[1])
            buckets.append(QueryResult.Bucket(task_id=None, value=value))
        else:
            logger.warning(f"Unsupported Prometheus result type: {result_type}")

        return QueryResult(query=query, buckets=buckets)

    def close(self):
        self.session.close()


class ElasticsearchClient:
    """
    Client for executing queries against an Elasticsearch server.
    """

    def __init__(self, server_config: ServerConfig) -> None:
        api_key = server_config.api_key
        endpoint = server_config.url
        self.es = Elasticsearch(hosts=[endpoint], api_key=api_key)

    def query(self, query: dict) -> QueryResult:
        """
        Execute a single Elasticsearch query and return its result.

        Returns:
            The result of the Elasticsearch query (follows Query DSL format).
        """
        logger.debug(f"Elasticsearch Query: {query}")
        data = self.es.search(size=0, **query)
        logger.debug(f"Elasticsearch Response: {data}")

        # Process Elasticsearch response data.
        res = self._process_response(data=data.to_dict(), query=query)
        return res

    def _process_response(self, data: dict, query: dict) -> QueryResult:
        # NOTE: This processing assumes a specific aggregation structure in the Elasticsearch response.
        # More specifiically, it assumes that the response only contains single level numeric aggregations, with optional grouping by task_id.
        buckets = []
        aggregations = data.get("aggregations", {})
        if not aggregations:
            logger.warning("No aggregations found in Elasticsearch response.")
            return QueryResult(query=query, buckets=buckets)

        # Assuming a terms aggregation on 'task_id' with a sub-aggregation for the metric value.
        for agg_name, agg_data in aggregations.items():
            if "buckets" in agg_data:
                for bucket in agg_data["buckets"]:
                    task_id = bucket.get("key_as_string") or str(bucket.get("key"))
                    # Each bucket may contain multiple numeric metric aggregations.
                    for metric_name, metric_value_agg in bucket.items():
                        if not isinstance(metric_value_agg, dict):
                            continue
                        value = metric_value_agg.get("value")
                        if value is not None:
                            buckets.append(
                                QueryResult.Bucket(task_id=task_id, value=value)
                            )
            else:
                # Single value aggregation without task grouping.
                buckets.append(
                    QueryResult.Bucket(task_id=None, value=agg_data.get("value"))
                )

        return QueryResult(query=query, buckets=buckets)

    def close(self):
        self.es.close()

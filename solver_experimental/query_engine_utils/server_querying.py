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

folder_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
sys.path.append(folder_path)

# import urllib3
from loguru import logger
from typing import Dict
import threading
import subprocess
import concurrent.futures
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import QueryManagerConfig, QueryGroupConfig
from .update_task_info import UPDATE_METHOD_NAME_MAPPING
from scheduler.entities import RunningTask, Task


class QueryManager():
    """
    Handles making PromQL queries to the Query Engine of choice (ASAP or Prometheus), and updating task information based on those queries.
    """

    def __init__(self, server_url, query_config: QueryManagerConfig) -> None:
        self.session = requests.Session()
        self.endpoint = f"{server_url}"
        self.query_config = query_config

    def execute_queries(self) -> Dict[int, Dict[str, dict]]:
        """
        Execute all query groups and return their results. If no data is fetched for a specific query group, the group ID will not be present.

        Returns:
            A dictionary mapping query group IDs to their respective results. 
        """
        all_results = {}
        for query_group in self.query_config.query_groups:
            logger.info(f"Executing Query Group ID: {query_group.id}")
            group_results = self.execute_query_group(query_group)
            if group_results:
                all_results[query_group.id] = group_results
        return all_results

    def execute_query_group(self, query_group: QueryGroupConfig) -> Dict[str, dict]:
        """
        Execute a single query group and return its results. If no data is fetched for a specific query, the query string will not be present.

        Returns:
            A dictionary mapping query strings to their respective results (follows Prometheus format).
        """
        results = {}
        delay = max(query_group.options.get('query_time_offset', 0), 0)
        for query in query_group.queries:
            try:
                response = self.session.get(
                    self.endpoint,
                    params={"query": query}
                )
                response.raise_for_status()
                data = response.json()
                if data['status'] == 'success':
                    results[query] = data
                # Process data as needed
            except requests.RequestException as e:
                logger.error(f"Error executing query '{query}': {e}")
            if delay > 0:
                time.sleep(delay)
        return results
    
    def update_task_metrics(self, running_tasks: Dict[str, RunningTask]) -> None:
        """
        Update the metrics of running tasks based on query results and update rules (which may be a udf).
        """
        query_results = self.execute_queries()
        update_rules = self.query_config.task_update_rules
        if not query_results:
            logger.warning("No query results found. Skipping task resource specification updates (this could lead to stale resource requirement estimates).")
            return

        for rule in update_rules:
            query_group_id = rule.query_group_id
            if query_group_id not in query_results:
                logger.warning(f"Query Group ID {query_group_id} not found in results.")
                continue

            update_func = UPDATE_METHOD_NAME_MAPPING[rule.update_function]
            group_results = query_results[query_group_id]
            
            update_func(group_results, running_tasks)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.session.close()

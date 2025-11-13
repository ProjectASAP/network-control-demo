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

# import urllib3
from loguru import logger
import pulp
from typing import Dict
import threading
import subprocess
import concurrent.futures
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from omegaconf import DictConfig, OmegaConf

from .config import QueryManagerConfig, QueryGroupConfig


class QueryManager():

    def __init__(self, server_url, query_config: QueryManagerConfig) -> None:
        self.session = requests.Session()
        self.endpoint = f"{server_url}/query"
        self.query_config = query_config

    def execute_queries(self) -> Dict[int, Dict[str, dict]]:
        all_results = {}
        for query_group in self.query_config.query_groups:
            logger.info(f"Executing Query Group ID: {query_group.id}")
            group_results = self.execute_query_group(query_group)
            all_results[query_group.id] = group_results
        return all_results

    def execute_query_group(self, query_group: QueryGroupConfig) -> Dict[str, dict]:
        results = {}
        for query in query_group.queries:
            try:
                response = self.session.get(
                    self.endpoint,
                    params={"query": query}
                )
                response.raise_for_status()
                data = response.json()
                results[query] = data
                # Process data as needed
            except requests.RequestException as e:
                logger.error(f"Error executing query '{query}': {e}")
        return results

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.session.close()

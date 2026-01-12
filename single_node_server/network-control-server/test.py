# this is extracted from solver to make server testing easier

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

def main():
    quantiles = [10 * i for i in range(1, 10)]
    sketch_query_url = "http://localhost:10101/metrics/cpu_cores"
    payload = {'quantiles': [f'p{q}' for q in quantiles]}
    start_t = time.time()
    sketch_response = requests.post(sketch_query_url, json=payload)
    print(f'Sketch query took {time.time() - start_t} seconds (Sketch)')
    print(f'Aggregations: {sketch_response.json()}')



if __name__ == "__main__":
    main()
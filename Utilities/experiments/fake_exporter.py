from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily, REGISTRY
from prometheus_client import start_http_server
from prometheus_client.registry import Collector
import argparse
import time
import numpy
import os
import itertools
import numpy as np
from typing import List, Dict, Any, Tuple
import csv
import datetime
import math
from collections import defaultdict


class CustomCollector(Collector):
    def __init__(
        self, scale, dataset, num_labels, num_values_per_label: List[int], metric_type
    ):
        self.scale = scale
        # self.timeseries_id_start = timeseries_id_start
        self.dataset = dataset
        self.rng = numpy.random.default_rng(0)
        self.total_samples = 0
        self.const_1M = 1000000
        self.const_2M = 2000000
        self.const_3M = 3000000

        self.metric_type = metric_type

        self.uniform_counter = 0
        self.dynamic_counter = 0
        self.zipf_counter = 0
        self.normal_counter = 0

        self.num_labels: int = num_labels
        self.labels = [f"label_{i}" for i in range(self.num_labels)]
        self.num_values_per_label: List[int]
        self.values_per_label: List[List[str]] = []
        self.label_value_combinations: List[List[str]] = []

        self.label_value_combinations = self.compute_labels(
            num_labels, num_values_per_label
        )

        # print("values_per_label")
        # [print(sublist) for sublist in self.values_per_label]
        # print("label_value_combinations")
        # [print(sublist) for sublist in self.label_value_combinations]
        # assert False

    def compute_labels(
        self, num_labels: int, num_values_per_label: List[int]
    ) -> List[List[str]]:
        if len(num_values_per_label) == 1:
            self.num_values_per_label = [
                num_values_per_label[0] for _ in range(num_labels)
            ]
        else:
            if len(num_values_per_label) != num_labels:
                raise ValueError(
                    "Number of num_values_per_label must be equal to num_labels"
                )
            self.num_values_per_label = num_values_per_label

        num_timeseries = np.prod(self.num_values_per_label)

        for label_idx in range(self.num_labels):
            values = [
                f"value_{label_idx}_value_{value_idx}"
                for value_idx in range(self.num_values_per_label[label_idx])
            ]
            self.values_per_label.append(values)

        label_value_combinations = list(
            itertools.product(*self.values_per_label))
        assert len(label_value_combinations) == num_timeseries
        # convert from list[tuple[str]] to list[list[str]]
        label_value_combinations = [
            list(label_value_combination)
            for label_value_combination in label_value_combinations
        ]
        return label_value_combinations

    def get_uniform_value_gauge(self):
        value = -1
        while value < 0 or value > self.scale:
            # value = numpy.random.uniform() * self.scale
            value = self.rng.uniform(0, self.scale)
        return value

    def get_normal_value_gauge(self):
        value = -1
        while value < 0 or value > self.scale:
            value = self.rng.normal(loc=self.scale / 2, scale=self.scale)
        return value

    def get_zipf_value_gauge(self):
        value = -1
        while value < 0 or value > self.scale:
            # value = numpy.random.zipf(1.01)
            value = self.rng.zipf(1.01)
        return value

    def get_dynamic_value_gauge(self):
        value = -1
        while value < 0 or value > self.scale:
            if self.total_samples < self.const_1M:
                # value = numpy.random.zipf(1.01)
                value = self.rng.zipf(1.01)
            elif self.total_samples < self.const_2M:
                # value = numpy.random.uniform() * self.scale
                value = self.rng.uniform(0, self.scale)
            else:
                value = self.rng.normal(loc=self.scale / 2, scale=self.scale)
        self.total_samples = (self.total_samples + 1) % self.const_3M
        return value

    def get_uniform_value_counter(self):
        value = -1
        while value < 0 or value > self.scale:
            # value = numpy.random.uniform() * self.scale
            value = self.rng.uniform(0, self.scale)
        self.uniform_counter += value
        return self.uniform_counter

    def get_normal_value_counter(self):
        value = -1
        while value < 0 or value > self.scale:
            value = self.rng.normal(loc=self.scale / 2, scale=self.scale)
        self.normal_counter += value
        return self.normal_counter

    def get_zipf_value_counter(self):
        value = -1
        while value < 0 or value > self.scale:
            # value = numpy.random.zipf(1.01)
            value = self.rng.zipf(1.01)
        self.zipf_counter += value
        return self.zipf_counter

    def get_dynamic_value_counter(self):
        value = -1
        while value < 0 or value > self.scale:
            if self.total_samples < self.const_1M:
                # value = numpy.random.zipf(1.01)
                value = self.rng.zipf(1.01)
            elif self.total_samples < self.const_2M:
                # value = numpy.random.uniform() * self.scale
                value = self.rng.uniform(0, self.scale)
            else:
                value = self.rng.normal(loc=self.scale / 2, scale=self.scale)
        self.total_samples = (self.total_samples + 1) % self.const_3M
        self.dynamic_counter += value
        return self.dynamic_counter

    def collect(self):
        if self.metric_type == "counter":
            fake_metric = CounterMetricFamily(
                "fake_metric",
                "Generating fake time series data with {} dataset".format(
                    self.dataset),
                labels=self.labels,
            )
        elif self.metric_type == "gauge":
            fake_metric = GaugeMetricFamily(
                "fake_metric",
                "Generating fake time series data with {} dataset".format(
                    self.dataset),
                labels=self.labels,
            )
        else:
            fake_metric = GaugeMetricFamily(
                "fake_metric",
                "Generating fake time series data with {} dataset".format(
                    self.dataset),
                labels=self.labels,
            )

        for label_value_combination in self.label_value_combinations:
            if self.metric_type == "counter":
                if self.dataset == "uniform":
                    value = self.get_uniform_value_counter()
                elif self.dataset == "normal":
                    value = self.get_normal_value_counter()
                elif self.dataset == "zipf":
                    value = self.get_zipf_value_counter()
                elif self.dataset == "dynamic":
                    value = self.get_dynamic_value_counter()
                else:
                    value = self.get_dynamic_value_counter()
            else:  # gauge
                if self.dataset == "uniform":
                    value = self.get_uniform_value_gauge()
                elif self.dataset == "normal":
                    value = self.get_normal_value_gauge()
                elif self.dataset == "zipf":
                    value = self.get_zipf_value_gauge()
                elif self.dataset == "dynamic":
                    value = self.get_dynamic_value_gauge()
                else:
                    value = self.get_dynamic_value_gauge()

            # labels = [f"label_value_{i}" for d in range(self.num_labels)]
            # fake_metric.add_metric(labels, value=value)
            fake_metric.add_metric(label_value_combination, value)

        yield fake_metric


"""
Module-level caches for the network control demo datasets.
These allow the collector to access parsed CSV rows if needed.
"""
_NCD_RESOURCES: List[Dict[str, Any]] = []
_NCD_BANDWIDTH: List[Dict[str, Any]] = []


def _read_csv_as_dicts(path: str) -> List[Dict[str, str]]:
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def read_network_control_demo_data() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    telemetry_resource_file_path = os.path.join(
        os.path.dirname(
            __file__), "../../DataGen/sample_output/telemetry_resources.csv"
    )
    if not os.path.exists(telemetry_resource_file_path):
        raise FileNotFoundError(
            "CSV file not found at {}. Please make sure the file exists.".format(
                telemetry_resource_file_path
            )
        )

    telemetry_bandwidth_file_path = os.path.join(
        os.path.dirname(
            __file__), "../../DataGen/sample_output/telemetry_edge_bandwidth.csv"
    )
    if not os.path.exists(telemetry_bandwidth_file_path):
        raise FileNotFoundError(
            "CSV file not found at {}. Please make sure the file exists.".format(
                telemetry_bandwidth_file_path
            )
        )

    # Parse CSVs
    resources_raw = _read_csv_as_dicts(telemetry_resource_file_path)
    bandwidth_raw = _read_csv_as_dicts(telemetry_bandwidth_file_path)

    # Convert numeric fields to appropriate types while keeping labels as strings
    resources: List[Dict[str, Any]] = []
    for row in resources_raw:
        try:
            resources.append(
                {
                    "timestamp": row.get("timestamp", ""),
                    "node_id": row.get("node_id", ""),
                    "task_id": row.get("task_id", ""),
                    "cpu_usage": float(row.get("cpu_usage", "nan")),
                    "memory_usage": float(row.get("memory_usage", "nan")),
                }
            )
        except Exception:
            # Skip malformed rows
            continue

    bandwidth: List[Dict[str, Any]] = []
    for row in bandwidth_raw:
        try:
            bandwidth.append(
                {
                    "timestamp": row.get("timestamp", ""),
                    "source_node_id": row.get("source_node_id", ""),
                    "target_node_id": row.get("target_node_id", ""),
                    "available_bandwidth_usage": float(
                        row.get("available_bandwidth_usage", "nan")
                    ),
                }
            )
        except Exception:
            # Skip malformed rows
            continue

    # Populate module-level caches for potential use by the collector
    global _NCD_RESOURCES, _NCD_BANDWIDTH
    _NCD_RESOURCES = resources
    _NCD_BANDWIDTH = bandwidth

    return resources, bandwidth


class NetworkControlDemoCollector(Collector):
    """
    Collector that replays the CSV data, one timestamp chunk per scrape.
    Emits three metrics:
      - cpu_usage{node_id, task_id}
      - memory_usage{node_id, task_id}
      - bandwidth_usage{source_task_id, target_task_id}
    """

    def __init__(self, resources: List[Dict[str, Any]], bandwidth: List[Dict[str, Any]]):
        self.resources = resources
        self.bandwidth = bandwidth

        # Group rows by timestamp string for efficient per-scrape replay
        self.resource_groups: Dict[str, List[Dict[str, Any]]] = {}
        for row in self.resources:
            ts = row.get("timestamp", "")
            self.resource_groups.setdefault(ts, []).append(row)
        self.resource_timestamps = sorted(self.resource_groups.keys())

        self.bandwidth_groups: Dict[str, List[Dict[str, Any]]] = {}
        for row in self.bandwidth:
            ts = row.get("timestamp", "")
            self.bandwidth_groups.setdefault(ts, []).append(row)
        self.bandwidth_timestamps = sorted(self.bandwidth_groups.keys())

        self._res_idx = 0
        self._bw_idx = 0

    @staticmethod
    def _ts_to_epoch_seconds(ts_str: str) -> float:
        # Expecting format like 2025-10-13T20:56:53
        try:
            dt = datetime.datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=datetime.timezone.utc
            )
            return dt.timestamp()
        except Exception:
            return float("nan")

    def collect(self):
        cpu_metric = GaugeMetricFamily(
            "cpu_usage", "CPU usage percent (from CSV)", labels=["node_id", "task_id"]
        )
        mem_metric = GaugeMetricFamily(
            "memory_usage",
            "Memory usage percent (from CSV)",
            labels=["node_id", "task_id"],
        )
        bw_metric = GaugeMetricFamily(
            "available_bandwidth_usage",
            "Available bandwidth (from CSV)",
            labels=["source_node_id", "target_node_id"],
        )
        cpu_task_metric = GaugeMetricFamily(
            "cpu_usage_task_id",
            "CPU usage percent (from CSV) merging task_id",
            labels=["node_id"],
        )
        cpu_node_metric = GaugeMetricFamily(
            "cpu_usage_node_id",
            "CPU usage percent (from CSV) merging node_id",
            labels=["task_id"],
        )
        mem_task_metric = GaugeMetricFamily(
            "memory_usage_task_id",
            "Memory usage percent (from CSV) merging task_id",
            labels=["node_id"],
        )
        mem_node_metric = GaugeMetricFamily(
            "memory_usage_node_id",
            "Memory usage percent (from CSV) merging node_id",
            labels=["task_id"],
        )
        bw_target_node_metric = GaugeMetricFamily(
            "available_bandwidth_usage_target_node_id",
            "Available bandwidth (from CSV) merging target_node_id",
            labels=["source_node_id"],
        )
        bw_source_node_metric = GaugeMetricFamily(
            "available_bandwidth_usage_source_node_id",
            "Available bandwidth (from CSV) merging source_node_id",
            labels=["target_node_id"],
        )

        # Emit next resource timestamp group
        if self.resource_timestamps:
            res_ts_str = self.resource_timestamps[self._res_idx]
            res_epoch = self._ts_to_epoch_seconds(res_ts_str)
            res_rows = self.resource_groups.get(res_ts_str, [])
            # Raw samples
            for row in res_rows:
                node_id = row.get("node_id", "")
                task_id = row.get("task_id", "")
                cpu = row.get("cpu_usage", float("nan"))
                mem = row.get("memory_usage", float("nan"))
                if cpu == cpu and res_epoch == res_epoch:  # not NaN
                    cpu_metric.add_metric(
                        [node_id, task_id], cpu, timestamp=res_epoch)
                else:
                    cpu_metric.add_metric([node_id, task_id], cpu)
                if mem == mem and res_epoch == res_epoch:
                    mem_metric.add_metric(
                        [node_id, task_id], mem, timestamp=res_epoch)
                else:
                    mem_metric.add_metric([node_id, task_id], mem)
            # Aggregated by node (merge task_id)
            cpu_by_node: Dict[str, float] = defaultdict(float)
            mem_by_node: Dict[str, float] = defaultdict(float)
            # Aggregated by task (merge node_id)
            cpu_by_task: Dict[str, float] = defaultdict(float)
            mem_by_task: Dict[str, float] = defaultdict(float)
            for row in res_rows:
                node_id = row.get("node_id", "")
                task_id = row.get("task_id", "")
                cpu = row.get("cpu_usage", float("nan"))
                mem = row.get("memory_usage", float("nan"))
                if isinstance(cpu, (int, float)) and not math.isnan(cpu):
                    cpu_by_node[node_id] += float(cpu)
                    cpu_by_task[task_id] += float(cpu)
                if isinstance(mem, (int, float)) and not math.isnan(mem):
                    mem_by_node[node_id] += float(mem)
                    mem_by_task[task_id] += float(mem)
            for node_id, val in cpu_by_node.items():
                if res_epoch == res_epoch:
                    cpu_task_metric.add_metric(
                        [node_id], val, timestamp=res_epoch)
                else:
                    cpu_task_metric.add_metric([node_id], val)
            for node_id, val in mem_by_node.items():
                if res_epoch == res_epoch:
                    mem_task_metric.add_metric(
                        [node_id], val, timestamp=res_epoch)
                else:
                    mem_task_metric.add_metric([node_id], val)
            # Emit per-task aggregates (merge node_id)
            for t_id, val in cpu_by_task.items():
                if res_epoch == res_epoch:
                    cpu_node_metric.add_metric(
                        [t_id], val, timestamp=res_epoch)
                else:
                    cpu_node_metric.add_metric([t_id], val)
            for t_id, val in mem_by_task.items():
                if res_epoch == res_epoch:
                    mem_node_metric.add_metric(
                        [t_id], val, timestamp=res_epoch)
                else:
                    mem_node_metric.add_metric([t_id], val)
            # Advance pointer
            self._res_idx = (self._res_idx + 1) % len(self.resource_timestamps)

        # Emit next bandwidth timestamp group
        if self.bandwidth_timestamps:
            bw_ts_str = self.bandwidth_timestamps[self._bw_idx]
            bw_epoch = self._ts_to_epoch_seconds(bw_ts_str)
            bw_rows = self.bandwidth_groups.get(bw_ts_str, [])
            # Raw samples
            for row in bw_rows:
                s = row.get("source_node_id", "")
                t = row.get("target_node_id", "")
                val = row.get("available_bandwidth_usage", float("nan"))
                if val == val and bw_epoch == bw_epoch:
                    bw_metric.add_metric([s, t], val, timestamp=bw_epoch)
                else:
                    bw_metric.add_metric([s, t], val)
            # Aggregations
            by_source: Dict[str, float] = defaultdict(
                float)  # merge target_task_id
            by_target: Dict[str, float] = defaultdict(
                float)  # merge source_task_id
            for row in bw_rows:
                s = row.get("source_node_id", "")
                t = row.get("target_node_id", "")
                val = row.get("available_bandwidth_usage", float("nan"))
                if isinstance(val, (int, float)) and not math.isnan(val):
                    by_source[s] += float(val)
                    by_target[t] += float(val)
            for source, val in by_source.items():
                if bw_epoch == bw_epoch:
                    bw_target_node_metric.add_metric(
                        [source], val, timestamp=bw_epoch)
                else:
                    bw_target_node_metric.add_metric([source], val)
            for target, val in by_target.items():
                if bw_epoch == bw_epoch:
                    bw_source_node_metric.add_metric(
                        [target], val, timestamp=bw_epoch)
                else:
                    bw_source_node_metric.add_metric([target], val)
            # Advance pointer
            self._bw_idx = (self._bw_idx + 1) % len(self.bandwidth_timestamps)

        yield cpu_metric
        yield mem_metric
        yield bw_metric
        yield cpu_task_metric
        yield mem_task_metric
        yield cpu_node_metric
        yield mem_node_metric
        yield bw_target_node_metric
        yield bw_source_node_metric


def main(args):
    if args.output_dir is not None:
        os.makedirs(args.output_dir, exist_ok=True)

    if args.dataset == "network_control_demo":
        resources, bandwidth = read_network_control_demo_data()
        network_control_demo_collector = NetworkControlDemoCollector(
            resources, bandwidth)
        REGISTRY.register(network_control_demo_collector)
    else:
        metric_collector = CustomCollector(
            args.valuescale,
            args.dataset,
            args.num_labels,
            args.num_values_per_label,
            args.metric_type,
        )
        REGISTRY.register(metric_collector)
    start_http_server(port=args.port)
    print("Fake exporter started on port {}".format(args.port))
    while True:
        time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir", type=str, required=False)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--valuescale", type=int, required=False)
    # parser.add_argument("--start_instanceid", type=int, required=True)
    # parser.add_argument("--batchsize", type=int, required=True)
    parser.add_argument("--dataset", type=str, required=True)
    parser.add_argument("--num_labels", type=int, required=False)
    parser.add_argument("--num_values_per_label", type=str, required=False)
    parser.add_argument("--metric_type", type=str, required=False)
    args = parser.parse_args()

    if not (args.num_values_per_label is None):
        args.num_values_per_label = [
            int(i) for i in args.num_values_per_label.split(",")]

    # if (
    #     args.port is None
    #     or args.valuescale is None
    #     or args.start_instanceid is None
    #     or args.batchsize is None
    #     or args.dataset is None
    # ):
    #     print("Fake exporter missing argument")
    #     sys.exit(0)
    main(args)

import time
import jsonlines
import numpy as np
import networkx as nx
from dataclasses import dataclass, field
from cattrs import structure, Converter
from loguru import logger
import math

from scheduler.entities import RunningTask, Task, Node, Edge, NetworkTopology
from scheduler.load_info import load_nodes, load_edges


@dataclass
class TaskMetrics:
    cpu_usage: np.ndarray
    memory_usage: np.ndarray


@dataclass
class MetricsRecord:
    task_id: str
    cpu_usage: float
    memory_usage: float


class MetricsEmulator:
    """
    Emulates telemetry metrics for running tasks.
    """

    def __init__(self, network: NetworkTopology) -> None:
        self.network = network
        self.metrics: dict[str, TaskMetrics] = {}

    def emulate_metrics(self, running_tasks: dict[str, RunningTask]) -> dict[str, TaskMetrics]:
        """
        Emulates telemetry metrics for running tasks.

        Args:
            running_tasks: Dictionary of task ids (str) and their corresponding RunningTask objects.
        Returns:
            Dictionary mapping task ids to their emulated metrics.
        """
        for t_id, running_task in running_tasks.items():
            task = running_task.task
            size = int(task.duration_s)
            if t_id not in self.metrics:
                self.metrics[t_id] = TaskMetrics(
                    cpu_usage=generate_timeseries(size=size, base_value=task.initial_cpu),
                    memory_usage=generate_timeseries(size=size, base_value=task.initial_memory)
                )
        return self.metrics
    
    def emit_metrics(self, running_tasks: dict[str, RunningTask], interval=60) -> dict[str, TaskMetrics]:
        """
        Emit emulated metrics for running tasks over a specified interval.
        """
        # Group tasks (ids) by assigned node (ids).
        nodes_to_tasks: dict[str, list[str]] = {}
        for t_id, running_task in running_tasks.items():
            assigned_node = running_task.node_id
            task_list = nodes_to_tasks.setdefault(assigned_node, [])
            task_list.append(t_id)

        # Emulate node resource usage based on assigned tasks.
        metrics: dict[str, TaskMetrics] = {}
        for node_id, task_ids in nodes_to_tasks.items():
            node = self.network.get_node(node_id)

            total_cpu_usage = np.zeros(interval)
            total_memory_usage = np.zeros(interval)

            # Aggregate task metrics to get node usage.
            for t_id in task_ids:
                task_metrics = self.metrics[t_id]
                start_time = running_tasks[t_id].start_time_s
                elapsed_time = time.time() - start_time
                offset = min(int(elapsed_time), len(task_metrics.cpu_usage))
                
                cpu_slice = task_metrics.cpu_usage[offset:offset + interval]
                memory_slice = task_metrics.memory_usage[offset:offset + interval]

                metrics[t_id] = TaskMetrics(
                    cpu_usage=cpu_slice,
                    memory_usage=memory_slice
                )

                total_cpu_usage += np.pad(cpu_slice, (0, interval - len(cpu_slice)), 'constant')
                total_memory_usage += np.pad(memory_slice, (0, interval - len(memory_slice)), 'constant')
            
            # Update node used resources (for demonstration purposes).
            node.used_cpu = np.median(total_cpu_usage)
            node.used_memory = np.median(total_memory_usage)
            logger.info(f"Node {node_id} used CPU: {node.used_cpu} ({node.used_cpu / node.cpu_capacity}), used Memory: {node.used_memory} ({node.used_memory / node.memory_capacity})")

        return metrics


def initialize_start_times(running_tasks: list[RunningTask]) -> None:
    """
    Initializes the start times of running tasks to the current time.

    Args:
        running_tasks: List of RunningTask objects.
    """
    current_time = time.time()
    for rt in running_tasks:
        rt.start_time_s = current_time


def generate_timeseries(size: int, base_value: float = 1) -> np.ndarray:
    """
    Generates a timeseries of emulated metric values.

    Args:
        size: Number of data points to generate.
        base_value: Base value around which to generate data.
    Returns:
        List of emulated metric values.
    """
    rng = np.random.default_rng()

    period = rng.uniform(0, 10 * size)
    a = rng.uniform(0.05, 0.95)
    b = 2 * np.pi / period
    c = rng.uniform(0, 10 * size)

    scale_factor = 1 + a * np.sin( b * (np.arange(size) - c) )
    noise = rng.normal(loc=0, scale=0.1, size=size)
    return base_value * (scale_factor + noise)


def create_metrics_records(metrics: dict[str, TaskMetrics]) -> list[dict]:
    """
    Creates serializable records from metrics data.

    Args:
        metrics: Dictionary mapping task ids to their metrics.
    Returns:
        List of dictionaries representing the metrics records.
    """    
    records = []
    for task_id, task_metrics in metrics.items():
        record = {
            "task": task_id,
            "cpu_cores": task_metrics.cpu_usage.tolist(),
            "memory_gb": task_metrics.memory_usage.tolist(),
            "network_mbps": [0] * len(task_metrics.cpu_usage) # Ignore network for now.
        }
        records.append(record)
    return records

if __name__ == "__main__":
    nodes = load_nodes("dummy_data/nodes.csv")
    edges = load_edges("dummy_data/edges.csv")
    network = NetworkTopology(nodes.values(), edges.values())

    emulator = MetricsEmulator(network=network)

    with jsonlines.open('assignments_log.jsonl', mode='r') as reader:
        for obj in reader:
            if not obj:
                continue
            running_tasks = structure(obj, list[RunningTask])
            logger.debug(f"Running tasks: {running_tasks}")

            running_tasks = {rt.task.task_id: rt for rt in running_tasks}

            emulator.emulate_metrics(running_tasks=running_tasks)
            metrics = emulator.emit_metrics(running_tasks=running_tasks, interval=60)

            # emulate_metrics(running_tasks={rt.task.task_id: rt for rt in running_tasks}, network=network, interval=60)
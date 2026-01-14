import sys
import time
import jsonlines
import numpy as np
import networkx as nx
from dataclasses import dataclass, field
from cattrs import structure, Converter
from loguru import logger
from typing import Iterable
import httpx
from fastapi import FastAPI
import uvicorn
import asyncio
from contextlib import asynccontextmanager

from scheduler.entities import RunningTask, Task, Node, Edge, NetworkTopology
from scheduler.load_info import load_nodes, load_edges


# Paramters for sending metrics to server.
SERVER_URL = 'http://localhost:10101'
INTERVAL = 60  # seconds
TIMEOUT = 5  # seconds


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the ML model
    background_loop = asyncio.create_task(periodically_send_metrics())  # Adjust the sleep duration as needed
    yield
    background_loop.cancel()
    try:
        await background_loop
    except asyncio.CancelledError:
        logger.info("Application shutdown initiated. Background task cancelled.")


app = FastAPI(lifespan=lifespan)


@app.post("/ingest")
async def ingest(assignments: list[dict]):
    running_tasks = structure(assignments, list[RunningTask])
    logger.debug(f"Running tasks: {running_tasks}")

    running_tasks = {rt.task.task_id: rt for rt in running_tasks}
    emulator.emulate_metrics(running_tasks=running_tasks)
    
    return {"message": "Tasks ingested successfully."}


async def periodically_send_metrics():
    async with httpx.AsyncClient() as client:
        while True:
            records = emulator.create_metrics_records()
            posts = []
            for record in records:
                logger.trace(f"Sending record: {record}")
                posts.append(client.post(SERVER_URL, json=record, timeout=TIMEOUT))
            for record in asyncio.as_completed(posts):
                try:
                    response = await record
                    response.raise_for_status()
                except Exception as e:
                    logger.error(f'Error sending metrics to {SERVER_URL}: {e}')
            await asyncio.sleep(INTERVAL)


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
        self.task_metrics: dict[str, TaskMetrics] = {}
        self.running_tasks: dict[str, RunningTask] = {}

    def create_task_metrics(self, task: Task) -> TaskMetrics:
        size = int(task.duration_s)
        cpu_usage = generate_timeseries(size=size, base_value=task.initial_cpu)
        memory_usage = generate_timeseries(size=size, base_value=task.initial_memory)
        return TaskMetrics(cpu_usage=cpu_usage, memory_usage=memory_usage)

    def emulate_metrics(self, running_tasks: dict[str, RunningTask]) -> dict[str, TaskMetrics]:
        """
        Emulates telemetry metrics for running tasks.

        Args:
            running_tasks: Dictionary of task ids (str) and their corresponding RunningTask objects.
        Returns:
            Dictionary mapping task ids to their emulated metrics.
        """
        self.running_tasks.update(running_tasks)
        for t_id, running_task in running_tasks.items():
            task = running_task.task
            if t_id not in self.task_metrics:
                self.task_metrics[t_id] = self.create_task_metrics(task)
        return self.task_metrics
    
    def _emit_metrics(self, interval=60) -> dict[str, TaskMetrics]:
        """
        Emit emulated metrics for running tasks over a specified interval.
        """
        # Group tasks (ids) by assigned node (ids).
        nodes_to_tasks: dict[str, list[str]] = {}
        for t_id, running_task in self.running_tasks.items():
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
                task_metrics = self.task_metrics[t_id]
                start_time = self.running_tasks[t_id].start_time_s
                elapsed_time = time.time() - start_time
                offset = min(int(elapsed_time), len(task_metrics.cpu_usage))

                if offset >= len(task_metrics.cpu_usage):
                    logger.warning(f"Task {t_id} has completed its duration. Skipping metric emission.")
                    continue
                
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
    

    def create_metrics_records(self, interval=INTERVAL) -> Iterable[dict]:
        """
        Creates serializable records from metrics data.

        Args:
            metrics: Dictionary mapping task ids to their metrics.
        Returns:
            List of dictionaries representing the metrics records.
        """
        metrics = self._emit_metrics(interval=interval)    
        for task_id, task_metrics in metrics.items():
            running_task = self.running_tasks[task_id]
            record = {
                "task": [task_id] * len(task_metrics.cpu_usage),
                "cluster": [running_task.node_id] * len(task_metrics.cpu_usage),
                "cpu_cores": task_metrics.cpu_usage.tolist(),
                "memory_gb": task_metrics.memory_usage.tolist(),
                "network_mbps": [0] * len(task_metrics.cpu_usage) # Ignore network for now.
            }
            yield record


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


def create_emulator() -> MetricsEmulator:
    nodes = load_nodes('dummy_data/nodes.csv')
    edges = load_edges('dummy_data/edges.csv')
    network = NetworkTopology(nodes=nodes.values(), edges=edges.values(), undirected=True)
    return MetricsEmulator(network=network)


if __name__ == "__main__":
    HOST = '127.0.0.1'
    PORT = 8000
    LOG_LEVEL = 'debug'

    emulator = create_emulator()

    logger.remove()
    logger.add(sys.stderr, level=LOG_LEVEL)

    uvicorn.run(app, host=HOST, port=PORT, log_level=LOG_LEVEL)
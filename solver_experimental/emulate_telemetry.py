import sys
import time
import json
import jsonlines
import datetime
import numpy as np
import networkx as nx
from dataclasses import dataclass, field
from pprint import pformat
from cattrs import structure, unstructure
from loguru import logger
from typing import Iterable
import httpx
from fastapi import FastAPI
import uvicorn
import asyncio
from contextlib import asynccontextmanager

from scheduler.entities import RunningTask, Task, Node, Edge, NetworkTopology
from scheduler.load_info import load_nodes, load_edges
from config import (
    ES_API_KEY,
    ES_INDEX_NAME,
    ES_INGEST_ENABLED,
    ES_URL,
    SKETCH_INGEST_ENABLED,
    SKETCH_URL,
)

# Paramters for sending metrics to server.
SERVER_URL = SKETCH_URL
INTERVAL = 5  # seconds
TIMEOUT = 5  # seconds


def build_es_bulk_payload(records: list[dict]) -> str:
    # Build an NDJSON bulk payload for Elasticsearch indexing.
    lines = []
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds") + "Z"
    for record in records:
        tasks = record.get("task", [])
        clusters = record.get("cluster", [])
        cpu = record.get("cpu_cores", [])
        memory = record.get("memory_gb", [])
        network = record.get("network_mbps", [])
        count = min(len(tasks), len(clusters), len(cpu), len(memory), len(network))
        for idx in range(count):
            lines.append(json.dumps({"index": {}}))
            doc = {
                "task": tasks[idx],
                "cluster": clusters[idx],
                "cpu_cores": cpu[idx],
                "memory_gb": memory[idx],
                "network_mbps": network[idx],
                "@timestamp": timestamp,
            }
            lines.append(json.dumps(doc))
    if not lines:
        return ""
    return "\n".join(lines) + "\n"


async def send_es_bulk(client: httpx.AsyncClient, records: list[dict]) -> None:
    # Send batched telemetry records to Elasticsearch using the bulk API.
    if not ES_INGEST_ENABLED or not ES_URL:
        return
    payload = build_es_bulk_payload(records)
    if not payload:
        return
    headers = {"Content-Type": "application/x-ndjson"}
    if ES_API_KEY:
        headers["Authorization"] = f"ApiKey {ES_API_KEY}"
    endpoint = f"{ES_URL.rstrip('/')}/{ES_INDEX_NAME}/_bulk"
    try:
        response = await client.post(
            endpoint, content=payload, headers=headers, timeout=TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        if data.get("errors"):
            logger.warning("Bulk ingest reported errors from Elasticsearch.")
    except Exception as exc:
        logger.error(f"Error sending metrics to Elasticsearch {ES_URL}: {exc}")

# Random seed for reproducibility.
SEED = 42
_RNG = np.random.default_rng(SEED)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Launch background metric emission while the API is running.
    # Load the ML model
    background_loop = asyncio.create_task(
        periodically_send_metrics()
    )  # Adjust the sleep duration as needed
    yield
    background_loop.cancel()
    try:
        await background_loop
    except asyncio.CancelledError:
        logger.info("Application shutdown initiated. Background task cancelled.")


app = FastAPI(lifespan=lifespan)


@app.get("/active_tasks")
async def active_tasks():
    running_tasks: dict[str, RunningTask] = {}
    completed_tasks: dict[str, RunningTask] = {}
    for tid, rt in emulator.running_tasks.items():
        curr_time = time.time()
        elapsed = curr_time - rt.start_time_s
        metrics = emulator.task_metrics[tid]
        if elapsed >= metrics.projected_duration:
            logger.info(f"Task {tid} has completed.")
            rt.end_time_s = rt.start_time_s + metrics.projected_duration
            completed_tasks[tid] = rt
            continue
        logger.info(f"Running task: {rt.task.task_id}, elapsed: {elapsed:.2f}s / projected {metrics.projected_duration:.2f}s")
        running_tasks[tid] = rt
    result = {
        "running_tasks": unstructure(running_tasks),
        "completed_tasks": unstructure(completed_tasks),
    }
    return result


@app.post("/ingest")
async def ingest(assignments: list[dict]):
    # Receive task assignments and update the emulator state.
    running_tasks = structure(assignments, list[RunningTask])
    logger.debug(f"Ingesting assignments: {running_tasks}")

    running_tasks = {rt.task.task_id: rt for rt in running_tasks}
    emulator.emulate_metrics(running_tasks=running_tasks)

    return {"message": "Tasks ingested successfully."}


async def periodically_send_metrics():
    # Periodically push emulated metrics to sketch server and ES.
    async with httpx.AsyncClient() as client:
        while True:
            records = list(emulator.create_metrics_records())
            logger.info(f"Generated {len(records)} metrics records to send.")
            if SKETCH_INGEST_ENABLED:
                posts = []
                for record in records:
                    logger.trace(f"Sending record: {pformat(record, indent=2)}")
                    posts.append(client.post(SERVER_URL, json=record, timeout=TIMEOUT))
                for record in asyncio.as_completed(posts):
                    try:
                        response = await record
                        response.raise_for_status()
                    except Exception as e:
                        logger.error(f"Error sending metrics to {SERVER_URL}: {e}")
            if ES_INGEST_ENABLED:
                await send_es_bulk(client, records)
            await asyncio.sleep(INTERVAL)


@dataclass
class TaskMetrics:
    # Per metric data generators for a task.
    cpu_usage: "MetricGenerator"
    memory_usage: "MetricGenerator"
    network_usage: "MetricGenerator"
    projected_duration: float

    def generate_buffers(
            self, 
            start_time,
            allocated_cpu: float,
            allocated_memory: float,
            allocated_network: float,
            interval: int = 60,
        ) -> "MetricBuffers | None":
        # Create empty buffers for the task metrics.
        elapsed_time = time.time() - start_time
        offset = min(elapsed_time, self.projected_duration)

        if offset >= self.projected_duration:
            return None
        
        size = int(min(interval, self.projected_duration - offset))
        start = offset / self.projected_duration
        stop = (offset + size) / self.projected_duration

        cpu_slice, cpu_duration_factor = self.cpu_usage.generate(start=start, stop=stop, num=size, value=allocated_cpu)
        memory_slice, memory_duration_factor = self.memory_usage.generate(start=start, stop=stop, num=size, value=allocated_memory)
        network_slice, network_duration_factor = self.network_usage.generate(start=start, stop=stop, num=size, value=allocated_network)

        # Pad to same length to send to server and maintain temporal consistency.
        padded_cpu_slice = np.pad(
            cpu_slice, (0, interval - len(cpu_slice)), "constant"
        )
        padded_memory_slice = np.pad(
            memory_slice, (0, interval - len(memory_slice)), "constant"
        )
        padded_network_slice = np.pad(
            network_slice, (0, interval - len(network_slice)), "constant"
        )

        buffers = MetricBuffers(
            cpu_usage=padded_cpu_slice, memory_usage=padded_memory_slice, network_usage=padded_network_slice
        )

        # Adjust task's projected duration based on speed-up factors.
        logger.debug(f"Duration adjust factors - CPU: {cpu_duration_factor}, Memory: {memory_duration_factor}, Network: {network_duration_factor}")
        new_duration = self.projected_duration * max(cpu_duration_factor, memory_duration_factor, network_duration_factor)
        self.projected_duration = new_duration
        return buffers

@dataclass
class MetricBuffers:
    # Per-task timeseries buffers for CPU/memory.
    cpu_usage: np.ndarray
    memory_usage: np.ndarray
    network_usage: np.ndarray


@dataclass
class MetricsRecord:
    # Point-in-time metrics for a task.
    task_id: str
    cpu_usage: float
    memory_usage: float
    network_usage: float


@dataclass
class MetricGenerator:
    """
    Generates emulated metric values using a noisy sinusoidal model.
    """
    a: float
    b: float
    c: float
    base_value: float = 1.0
    noise_scale: float = 0.1
    p_scalable: float = 0

    def __post_init__(self):
        num = 1000
        # Precompute the proportion of the metric that is scalable (for Amdahl's law).
        # Estimate p_scalable as the fraction of generated values above the base value (indicates that they scale with resource allocation).
        values, _ = self.generate(num=num, value=self.base_value)
        p = (values > self.base_value).sum() / num
        self.p_scalable = p

    @classmethod
    def create(cls, base_value: float = 1.0) -> "MetricGenerator":
        # Generate a noisy sinusoidal time series around the base value.
        """
        Creates a MetricGenerator with random parameters.
    
        Args:
            base_value: Base value around which to generate data.
        Returns:
            A MetricGenerator instance.
        """
        rng = _RNG
        period = rng.uniform(0, 10)
        a = rng.uniform(0.05, 0.95)
        b = 2 * np.pi / period
        c = b * rng.uniform(0, 10)
    
        return cls(a=a, b=b, c=c, base_value=base_value)

    def generate(self, stop: float = 1.0, start: float = 0.0, num: int = 60, value: float = 1.0) -> tuple[np.ndarray, float]:
        # Generate a noisy sinusoidal time series around the base value.
        """
        Generates a timeseries of emulated metric values over a specified range of the metric duration. Also returns the adjusted duration factor, the
        multiplicative factor by which the task duration should be adjusted based on the speed-up from Amdahl's law.
    
        Args:
            start: Start time (in [0, 1]) of the metric duration.
            stop: Stop time (in [0, 1]) of the metric duration.
            num: Number of samples to generate.
            value: Represents the nominal amount of resources allocated to the task.
        Returns:
            Array of emulated metric values and the adjusted duration factor.
        """
        if self.base_value == 0:
            return np.zeros(num), 1.0
        rng = _RNG
        # value_scale = value / self.base_value

        # speed_up = amdahl_factor(self.p_scalable, value_scale)
        a = self.a
        # Compress timeseries by corresponding factor.
        b = self.b
        c = self.c
        
        t = np.linspace(start, stop, num=num)
    
        scale_factor = 1 + a * np.sin(b * t - c)
        noise = rng.normal(loc=0, scale=self.noise_scale, size=len(scale_factor))
        buffer = self.base_value * (scale_factor + noise)

        # Reduce projected duration when usage is higher than allocated value.
        clipped = np.clip(buffer, a_min=value, a_max=None)
        weight = clipped.mean() * (stop - start) + value * (1.0 - (stop - start))
        resource_usage = value / weight

        # Calculate slow down using Amdahl's law with the observed value scale. Use for duration adjustment.
        empirical_speed_up = amdahl_factor(self.p_scalable, resource_usage)
        duration_adjust_factor = self.duration_adjust_factor(speed_up=empirical_speed_up, stop=stop)

        return buffer, duration_adjust_factor
    
    def duration_adjust_factor(self, speed_up: float = 1.0, stop: float = 1.0) -> float:
        # Compute scale factor to adjust the projected duration for remainder of task based on the speed-up factor.
        interval = 1.0 - stop
        adjusted_interval = interval / speed_up
        adjusted_duration = stop + adjusted_interval
        return adjusted_duration


def amdahl_factor(p: float, n: float):
    time_scale = (1 - p) + p / n
    speed_up = 1 / time_scale
    return speed_up


class TaskMetricsEmulator:
    """
    Emulates telemetry metrics for running tasks.
    """

    def __init__(self, network: NetworkTopology) -> None:
        # Hold network topology and the current task time series buffers.
        self.network = network
        self.task_metrics: dict[str, TaskMetrics] = {}
        self.running_tasks: dict[str, RunningTask] = {}
        self.completed_tasks: dict[str, RunningTask] = {}

    @classmethod
    def create_emulator(cls, node_path: str = "dummy_data/nodes.jsonl", edge_path: str = "dummy_data/edges.jsonl") -> "TaskMetricsEmulator":
        # Construct an emulator using the dummy network topology.
        nodes = load_nodes(node_path)
        edges = load_edges(edge_path)
        network = NetworkTopology(
            nodes=nodes.values(), edges=edges.values(), undirected=True
        )
        return cls(network=network)

    def create_task_metrics(self, task: Task) -> TaskMetrics:
        # Generate a full-duration timeseries for a new task.
        size = int(task.duration_s)
        cpu_usage = MetricGenerator.create(base_value=task.initial_cpu)
        memory_usage = MetricGenerator.create(base_value=task.initial_memory)
        network_usage = MetricGenerator.create(base_value=sum(task.peer_bandwidths.values()))
        return TaskMetrics(
            cpu_usage=cpu_usage, 
            memory_usage=memory_usage, 
            network_usage=network_usage, 
            projected_duration=size
        )

    def emulate_metrics(
        self, running_tasks: dict[str, RunningTask]
    ) -> dict[str, TaskMetrics]:
        """
        Emulates telemetry metrics for running tasks.

        Args:
            running_tasks: Dictionary of task ids (str) and their corresponding RunningTask objects.
        Returns:
            Dictionary mapping task ids to their emulated metrics.
        """
        # Merge new running tasks and ensure each has a metrics buffer.
        self.running_tasks.update(running_tasks)
        for t_id, running_task in running_tasks.items():
            task = running_task.task
            if t_id not in self.task_metrics:
                self.task_metrics[t_id] = self.create_task_metrics(task)
        return self.task_metrics

    def _emit_metrics(self, interval=60) -> dict[str, MetricBuffers]:
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
        metrics: dict[str, MetricBuffers] = {}
        for node_id, task_ids in nodes_to_tasks.items():
            node = self.network.get_node(node_id)

            total_cpu_usage = np.zeros(interval)
            total_memory_usage = np.zeros(interval)
            total_network_usage = np.zeros(interval)
            # Aggregate task metrics to get node usage and per-task slices.
            for t_id in task_ids:
                task_metrics = self.task_metrics[t_id]
                start_time = self.running_tasks[t_id].start_time_s
                old_duration = task_metrics.projected_duration

                metrics_buffers = task_metrics.generate_buffers(
                    start_time=start_time, 
                    allocated_cpu=self.running_tasks[t_id].task.initial_cpu,
                    allocated_memory=self.running_tasks[t_id].task.initial_memory,
                    allocated_network=sum(self.running_tasks[t_id].task.peer_bandwidths.values()),
                    interval=interval)
                
                if metrics_buffers is None:
                    logger.warning(
                        f"Task {t_id} has completed its duration. Skipping metric emission."
                    )
                    continue
                metrics[t_id] = metrics_buffers

                total_cpu_usage += metrics_buffers.cpu_usage
                total_memory_usage += metrics_buffers.memory_usage
                total_network_usage += metrics_buffers.network_usage

                # # Adjust task's projected duration based on speed-up factors.
                new_duration = task_metrics.projected_duration
                logger.info(f'Updated projected duration for task {t_id}: {old_duration} -> {new_duration} s')

            # Update node used resources (for demonstration purposes).
            node.used_cpu = np.median(total_cpu_usage)
            node.used_memory = np.median(total_memory_usage)
            node.used_network = np.median(total_network_usage)

            # If network capacity is None, sum capacities of connected edges.
            network_capacity = node.network_capacity
            if network_capacity is None:
                network_capacity = 0.0
                for nbr in self.network._graph.neighbors(node_id):
                    edge = self.network.get_edge((node_id, nbr))
                    network_capacity += edge.capacity

            logger.info(
                f"Node {node_id} used CPU: {node.used_cpu} ({node.used_cpu / node.cpu_capacity}), used Memory: {node.used_memory} ({node.used_memory / node.memory_capacity}), used Network: {node.used_network} ({node.used_network / network_capacity})"
            )

        return metrics

    def create_metrics_records(self, interval=INTERVAL) -> Iterable[dict]:
        """
        Creates serializable records from metrics data.

        Args:
            metrics: Dictionary mapping task ids to their metrics.
        Returns:
            List of dictionaries representing the metrics records.
        """
        # Convert slices into the schema expected by sketch/ES ingestion.
        metrics = self._emit_metrics(interval=interval)
        for task_id, task_metrics in metrics.items():
            running_task = self.running_tasks[task_id]
            record = {
                "task": [task_id] * len(task_metrics.cpu_usage),
                "cluster": [running_task.node_id] * len(task_metrics.cpu_usage),
                "cpu_cores": task_metrics.cpu_usage.tolist(),
                "memory_gb": task_metrics.memory_usage.tolist(),
                "network_mbps": task_metrics.network_usage.tolist(),
            }
            yield record


if __name__ == "__main__":
    # Start the FastAPI telemetry emulator.
    HOST = "127.0.0.1"
    PORT = 8000
    LOG_LEVEL = "debug"

    emulator = TaskMetricsEmulator.create_emulator()

    logger.remove()
    logger.add(sys.stderr, level=LOG_LEVEL.upper())

    uvicorn.run(app, host=HOST, port=PORT, log_level=LOG_LEVEL)

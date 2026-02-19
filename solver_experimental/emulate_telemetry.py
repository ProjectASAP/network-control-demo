import argparse
import sys
import os
import time
import json
import datetime as dt
import numpy as np
import networkx as nx
from dataclasses import dataclass, field
from pprint import pformat
from cattrs import structure, unstructure
from loguru import logger
from typing import Iterable, Callable
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
    ES_URL,
    SKETCH_URL,
)
from logging_utils import log_record

# Paramters for sending metrics to server.
SERVER_URL = SKETCH_URL
TIMEOUT = int(os.getenv("INGEST_TIMEOUT_SECONDS", "30"))  # seconds

# Emulator instance (initialized in main).
emulator: "TaskMetricsEmulator"
# Function to get current time based on epoch (initialized in main).
get_current_time: Callable[[], float]

# Ingest parameters.
SKETCH_INGEST_ENABLED = True
ES_INGEST_ENABLED = True

# Logging parameters.
SKETCH_INGEST_LOG_PATH: str | None = None
ES_INGEST_LOG_PATH: str | None = None

global_epoch = 0


def build_es_bulk_payload(records: list[dict]) -> str:
    # Build an NDJSON bulk payload for Elasticsearch indexing from column oriented records.
    lines = []
    for record in records:
        row_records = _column_to_row_orient(record)
        for doc in row_records:
            lines.append(json.dumps({"index": {}}))
            lines.append(json.dumps(doc))
    if not lines:
        return ""
    return "\n".join(lines) + "\n"


def _column_to_row_orient(data: dict):
    # Convert column-oriented data to row-oriented format.
    rows = []
    for col, values in data.items():
        for idx, value in enumerate(values):
            if idx >= len(rows):
                rows.append({})
            rows[idx][col] = value
    return rows


def _split_record(record: dict, max_rows: int) -> list[dict]:
    if max_rows <= 0:
        return [record]
    row_count = len(record.get("task", []))
    if row_count <= max_rows:
        return [record]
    split_records: list[dict] = []
    for start in range(0, row_count, max_rows):
        end = min(start + max_rows, row_count)
        chunk: dict = {}
        for key, value in record.items():
            if isinstance(value, list):
                chunk[key] = value[start:end]
            else:
                chunk[key] = value
        split_records.append(chunk)
    return split_records


def _split_records(records: list[dict], max_rows: int) -> list[dict]:
    if max_rows <= 0:
        return records
    chunks: list[dict] = []
    for record in records:
        chunks.extend(_split_record(record, max_rows))
    return chunks


async def send_es_bulk(
    client: httpx.AsyncClient, records: list[dict], refresh: str | None = None
) -> None:
    # Send batched telemetry records to Elasticsearch using the bulk API.
    if not ES_INGEST_ENABLED or not ES_URL:
        return
    headers = {"Content-Type": "application/x-ndjson"}
    if ES_API_KEY:
        headers["Authorization"] = f"ApiKey {ES_API_KEY}"
    endpoint = f"{ES_URL.rstrip('/')}/{ES_INDEX_NAME}/_bulk"
    params = {"refresh": refresh} if refresh else None

    # ES expects a single record per line, so convert from column-oriented format and split into batches if needed.
    for record in records:
        record['epoch'] = [record['epoch']] * len(record.get('task', [])) # Ensure epoch is included as a column for ES indexing.

    for chunk in _split_records(records, 1000):
        payload = build_es_bulk_payload([chunk])
        if not payload:
            continue
        try:
            response = await client.post(
                endpoint, content=payload, headers=headers, params=params, timeout=TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
            if data.get("errors"):
                logger.warning("Bulk ingest reported errors from Elasticsearch.")
        except Exception as exc:
            logger.error(
                "Error sending metrics to Elasticsearch {}: {} ({})",
                ES_URL,
                exc,
                type(exc).__name__,
            )


async def send_records(records: list[dict], refresh: str | None = None) -> None:
    # Push records to sketch and ES (optionally waiting for refresh).
    if not records:
        return
    
    global global_epoch
    async with httpx.AsyncClient() as client:
        # Get number of rows.
        num_rows = 0
        for record in records:
            if record:
                key = next((k for k, v in record.items() if isinstance(v, list)), None)
                if key:
                    num_rows += len(record[key])
        if SKETCH_INGEST_ENABLED:
            posts = []
            for record in _split_records(records, 1000):
                logger.trace(f"Sending record: {record}")
                posts.append(client.post(SERVER_URL, json=record, timeout=TIMEOUT))
            t0 = time.perf_counter()
            for record in asyncio.as_completed(posts):
                try:
                    response = await record
                    response.raise_for_status()
                except Exception as e:
                    logger.error(
                        "Error sending metrics to {}: {} ({})",
                        SERVER_URL,
                        e,
                        type(e).__name__,
                    )
            elapsed = time.perf_counter() - t0
            if SKETCH_INGEST_LOG_PATH:
                log_record(log_path=SKETCH_INGEST_LOG_PATH, epoch=global_epoch, duration_ms=elapsed * 1000, num_rows_ingested=num_rows)
        if ES_INGEST_ENABLED:
            t0 = time.perf_counter()
            await send_es_bulk(client, records, refresh=refresh)
            elapsed = time.perf_counter() - t0
            if ES_INGEST_LOG_PATH:
                log_record(log_path=ES_INGEST_LOG_PATH, epoch=global_epoch, duration_ms=elapsed * 1000, num_rows_ingested=num_rows)

# Random seed for reproducibility.
SEED = 42
_RNG = np.random.default_rng(SEED)


app = FastAPI()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/active_tasks")
async def active_tasks():
    running_tasks: dict[str, RunningTask] = {}
    completed_tasks: dict[str, RunningTask] = {}
    for tid, rt in emulator.running_tasks.items():
        curr_time = get_current_time()
        start_time = emulator.ingest_wall_time_s.get(tid, curr_time)
        elapsed = curr_time - start_time
        metrics = emulator.task_metrics[tid]
        if elapsed >= metrics.projected_duration:
            rt.end_time_s = rt.start_time_s + metrics.projected_duration
            completed_tasks[tid] = rt
            continue
        logger.debug(f"Running task: {rt.task.task_id}, elapsed: {elapsed:.2f}s / projected {metrics.projected_duration:.2f}s")
        running_tasks[tid] = rt
    logger.info(f"Completed tasks: {list(completed_tasks.keys())}")
    logger.info(f"Active tasks: {list(running_tasks.keys())}")
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

    if running_tasks:
        running_tasks = {rt.task.task_id: rt for rt in running_tasks}
        emulator.emulate_metrics(running_tasks=running_tasks)
        await force_es_refresh()
    else:
        logger.warning("No running tasks ingested.")

    # Increment epoch after processing each batch of assignments to simulate time progression in the emulator.
    global global_epoch
    global_epoch += 1
    logger.info(f"Advanced to epoch {global_epoch}. Ingested {len(running_tasks)} running tasks.")

    return {"message": "Tasks ingested successfully."}


async def force_es_refresh() -> None:
    # Push metrics immediately and wait for ES refresh so queries can see them.
    records = list(emulator.create_metrics_records())
    if not records:
        return
    await send_records(records, refresh="wait_for")


@dataclass
class TaskMetrics:
    # Per metric data generators for a task.
    cpu_usage: "MetricGenerator"
    memory_usage: "MetricGenerator"
    network_usage: "MetricGenerator"
    projected_duration: float
    # NOTE: Tracks actual task arrival time on assigned node. This assumes tasks can be suspended and resumed when reassigned.
    ingest_wall_time_s: float = field(default_factory=lambda: time.time())

    def generate_buffers(
            self, 
            current_time_s: float,
            allocated_cpu: float,
            allocated_memory: float,
            allocated_network: float,
            epoch_length_s: float = 60.0,
            data_rate: int = 1
        ) -> "MetricBuffers | None":
        """
        Generates timeseries buffers for the task metrics based on the elapsed time since the task was ingested, the allocated resources, and the projected duration.
        Also adjusts the projected duration based on the observed speed-up from the allocated resources.
        """
        # Create empty buffers for the task metrics.
        elapsed_time = current_time_s - self.ingest_wall_time_s
        offset = elapsed_time

        if elapsed_time >= self.projected_duration:
            return None
        
        size_s = int(min(epoch_length_s, self.projected_duration - offset))
        size = size_s * data_rate # Actual number of datapoints to generate based on epoch length and data rate.
        start = offset / self.projected_duration
        stop = (offset + size_s) / self.projected_duration

        if size <= 0:
            return None

        cpu_slice, cpu_duration_factor = self.cpu_usage.generate(start=start, stop=stop, num=size, max_value=allocated_cpu)
        memory_slice, memory_duration_factor = self.memory_usage.generate(start=start, stop=stop, num=size, max_value=allocated_memory)
        network_slice, network_duration_factor = self.network_usage.generate(start=start, stop=stop, num=size, max_value=allocated_network)

        # Pad to same length to send to server and maintain temporal consistency.
        max_buffer_length = int(epoch_length_s * data_rate)
        padded_cpu_slice = np.pad(
            cpu_slice, (0, max_buffer_length - len(cpu_slice)), "constant"
        )
        padded_memory_slice = np.pad(
            memory_slice, (0, max_buffer_length - len(memory_slice)), "constant"
        )
        padded_network_slice = np.pad(
            network_slice, (0, max_buffer_length - len(network_slice)), "constant"
        )

        buffers = MetricBuffers(
            cpu_usage=padded_cpu_slice, memory_usage=padded_memory_slice, network_usage=padded_network_slice
        )

        # Adjust task's projected duration based on speed-up factors.
        logger.debug(f"Duration adjust factors - CPU: {cpu_duration_factor}, Memory: {memory_duration_factor}, Network: {network_duration_factor}")
        new_duration = self.projected_duration * max(cpu_duration_factor, memory_duration_factor, network_duration_factor)
        self.projected_duration = round(new_duration, 3) # Round for nicer logging and numerical stability.
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
        # Randomly assign a portion of the task as scalable to resources for more realistic variability.
        self.p_scalable = np.random.uniform(0.1, 0.9)

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
        a = rng.uniform(0.05, 0.65)
        b = 2 * np.pi / period
        c = b * rng.uniform(0, 10)
    
        return cls(a=a, b=b, c=c, base_value=base_value)

    def generate(self, stop: float = 1.0, start: float = 0.0, num: int = 60, max_value: float = 1.0) -> tuple[np.ndarray, float]:
        # Generate a noisy sinusoidal time series around the base value.
        """
        Generates a timeseries of emulated metric values over a specified range of the metric duration. Also returns the adjusted duration factor, the
        multiplicative factor by which the task duration should be adjusted based on the speed-up from Amdahl's law.
    
        Args:
            start: Start time (in [0, 1]) of the metric duration.
            stop: Stop time (in [0, 1]) of the metric duration.
            num: Number of samples to generate.
            max_value: Represents the nominal amount of resources allocated to the task.
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
        min_arr = np.clip(buffer, a_min=max_value, a_max=None)
        weight = min_arr.mean() * (stop - start) + max_value * (1.0 - (stop - start))
        resource_usage = max_value / weight

        # Calculate slow down using Amdahl's law with the observed value scale. Use for duration adjustment.
        empirical_speed_up = amdahl_factor(self.p_scalable, resource_usage)
        duration_adjust_factor = self.duration_adjust_factor(speed_up=empirical_speed_up, stop=stop)

        clipped_buffer = np.clip(buffer, a_min=0, a_max=max_value)

        return clipped_buffer, duration_adjust_factor
    
    def duration_adjust_factor(self, speed_up: float = 1.0, stop: float = 1.0) -> float:
        # Compute scale factor to adjust the projected duration for remainder of task based on the "speed-up" factor.
        interval = 1.0 - stop
        adjusted_interval = interval / round(speed_up, 3)
        adjusted_duration = stop + adjusted_interval
        return round(adjusted_duration, 3) # Round for nicer logging and numerical stability.


def amdahl_factor(p: float, n: float):
    time_scale = (1 - p) + p / n
    speed_up = 1 / time_scale
    return speed_up


class TaskMetricsEmulator:
    """
    Emulates telemetry metrics for running tasks.
    """

    def __init__(self, network: NetworkTopology, epoch_length_s: float = 60.0, data_rate: int = 1) -> None:
        # Hold network topology and the current task time series buffers.
        self.network = network
        # self.base_epoch_ms = base_epoch_ms
        self.task_metrics: dict[str, TaskMetrics] = {}
        self.running_tasks: dict[str, RunningTask] = {}
        self.completed_tasks: dict[str, RunningTask] = {}

        self.epoch_length_s: float = epoch_length_s
        self.data_rate: int = data_rate

        self.ingest_wall_time_s: dict[str, float] = {}
        self.ingest_offset_s: dict[str, float] = {}

    @classmethod
    def create_emulator(
        cls, 
        node_path: str = "dummy_data/nodes.jsonl", 
        edge_path: str = "dummy_data/edges.jsonl",
        **kwargs
    ) -> "TaskMetricsEmulator":
        # Construct an emulator using the dummy network topology.
        nodes = load_nodes(node_path)
        edges = load_edges(edge_path)
        network = NetworkTopology(
            nodes=nodes.values(), edges=edges.values(), undirected=True
        )
        return cls(network=network, **kwargs)

    def create_task_metrics(self, task: Task) -> TaskMetrics:
        # Generate a full-duration timeseries for a new task.
        size = int(task.duration_s)
        cpu_usage = MetricGenerator.create(base_value=task.initial_cpu)
        memory_usage = MetricGenerator.create(base_value=task.initial_memory)
        network_usage = MetricGenerator.create(base_value=sum(task.peer_bandwidths.values()))
        current_time_s = get_current_time()
        return TaskMetrics(
            cpu_usage=cpu_usage, 
            memory_usage=memory_usage, 
            network_usage=network_usage, 
            projected_duration=size,
            ingest_wall_time_s=current_time_s,
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
                self.ingest_wall_time_s[t_id] = get_current_time()
        return self.task_metrics

    def _emit_metrics(self) -> dict[str, MetricBuffers]:
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

            num_datapoints = int(self.epoch_length_s * self.data_rate)

            total_cpu_usage = np.zeros(num_datapoints)
            total_memory_usage = np.zeros(num_datapoints)
            total_network_usage = np.zeros(num_datapoints)
            # Aggregate task metrics to get node usage and per-task slices.
            for t_id in task_ids:
                task_metrics = self.task_metrics[t_id]
                running_task = self.running_tasks[t_id]

                old_duration = task_metrics.projected_duration
                current_time_s = get_current_time()
                metrics_buffers = task_metrics.generate_buffers(
                    current_time_s=current_time_s,
                    allocated_cpu=self.running_tasks[t_id].task.initial_cpu,
                    allocated_memory=self.running_tasks[t_id].task.initial_memory,
                    allocated_network=sum(self.running_tasks[t_id].task.peer_bandwidths.values()),
                    epoch_length_s=self.epoch_length_s,
                    data_rate=self.data_rate)
                
                if metrics_buffers is None:
                    continue
                metrics[t_id] = metrics_buffers

                total_cpu_usage += metrics_buffers.cpu_usage
                total_memory_usage += metrics_buffers.memory_usage
                total_network_usage += metrics_buffers.network_usage

                # Adjust task projected duration based on resource usage (adjustment is side effect in generate_buffers).
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
                f"Node {node_id} used CPU: {node.used_cpu} ({node.used_cpu / node.cpu_capacity}), used Memory: {node.used_memory} ({node.used_memory / node.memory_capacity}), used Network: {node.used_network} ({node.used_network / network_capacity if network_capacity != 0.0 else 'NA'})"
            )
            
        logger.info(f"Emitting metrics for {len(metrics)} tasks across {len(nodes_to_tasks)} nodes.")

        return metrics

    def create_metrics_records(self) -> Iterable[dict]:
        """
        Creates serializable records from metrics data. These are in column oriented format (except for the epoch field which is scalar).

        Args:
            metrics: Dictionary mapping task ids to their metrics.
        Returns:
            List of dictionaries representing the metrics records.
        """
        # Convert slices into the schema expected by sketch/ES ingestion.
        global global_epoch
        metrics = self._emit_metrics()
        epoch = global_epoch
        for task_id, task_metrics in metrics.items():
            running_task = self.running_tasks[task_id]
            # NOTE: Sketch server wants a scalar for "epoch".
            record = {
                "epoch": epoch,
                "task": [task_id] * len(task_metrics.cpu_usage),
                "cluster": [running_task.node_id] * len(task_metrics.cpu_usage),
                "cpu_cores": task_metrics.cpu_usage.tolist(),
                "memory_gb": task_metrics.memory_usage.tolist(),
                "network_mbps": task_metrics.network_usage.tolist()
            }
            yield record


def create_virtual_clock(epoch_length_s: int = 60) -> Callable[[], float]:
    """
    Creates a virtual clock function that simulates time progression in the emulator. The clock returns the current time based on a specified epoch length.

    Args:
        epoch_length_s: The nominal duration of each epoch in seconds. Should be consistent with the epoch length used for the controller to maintain temporal consistency.
    Returns:
        A function that returns the current virtual time and epoch index when called.
    """
    # Simple virtual clock to track time in the emulator.
    base_time_s = time.time()

    def get_time():
        global global_epoch
        return base_time_s + global_epoch * epoch_length_s

    return get_time


if __name__ == "__main__":
    # Parse CLI options.
    parser = argparse.ArgumentParser(description="Generates synthetic telemetry data for testing the network control loop.")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--epoch-length-s", type=float, default=60.0)
    parser.add_argument("--data-rate", type=int, default=1)
    parser.add_argument("--log-level", type=str, default="INFO")
    parser.add_argument("--sketch-ingest-log-path", type=str, default=None)
    parser.add_argument("--es-ingest-log-path", type=str, default=None)
    parser.add_argument("--no-sketch-ingest", action='store_true')
    parser.add_argument("--no-es-ingest", action='store_true')

    args = parser.parse_args()

    # Start the FastAPI telemetry emulator.
    emulator = TaskMetricsEmulator.create_emulator(epoch_length_s=args.epoch_length_s, data_rate=args.data_rate)

    get_current_time = create_virtual_clock(epoch_length_s=args.epoch_length_s)

    SKETCH_INGEST_LOG_PATH = args.sketch_ingest_log_path
    ES_INGEST_LOG_PATH = args.es_ingest_log_path
    SKETCH_INGEST_ENABLED = not args.no_sketch_ingest
    ES_INGEST_ENABLED = not args.no_es_ingest

    logger.remove()
    logger.add(sys.stderr, level=args.log_level.upper())

    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level.lower())
#!/usr/bin/env python3
"""
Synthetic network control task telemetry data generator.

Produces three CSV files:
  - tasks.csv: coarse-grained task metadata (arrival, duration, initial resources, node assignments)
  - telemetry_resources.csv: per-task CPU and memory usage samples on each node
  - telemetry_bandwidth.csv: bandwidth usage samples for task-to-task communications

Usage:
    python generate_network_tasks.py --num-tasks 100 --telemetry-steps 30
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import math
import random
from pathlib import Path
from typing import Dict, Iterable, List


@dataclasses.dataclass
class TaskSpec:
    task_id: str
    node_id: str
    arrival_offset_s: float
    duration_s: float
    initial_cpu: float
    initial_memory: float
    initial_bandwidth: float
    peer_bandwidths: Dict[str, float] = dataclasses.field(default_factory=dict)


def truncated_normal(rng: random.Random, mean: float, std_dev: float, minimum: float) -> float:
    """Sample from a normal distribution but enforce a lower bound."""
    for _ in range(10):
        sample = rng.gauss(mean, std_dev)
        if sample >= minimum:
            return sample
    return max(minimum, mean)


def generate_tasks(
    rng: random.Random,
    num_tasks: int,
    num_nodes: int,
    interarrival_mean_s: float,
    duration_mean_s: float,
    duration_std_dev_s: float,
    cpu_bounds: tuple[float, float],
    memory_bounds: tuple[float, float],
    bandwidth_bounds: tuple[float, float],
) -> List[TaskSpec]:
    tasks: List[TaskSpec] = []
    arrival_time = 0.0

    for idx in range(1, num_tasks + 1):
        interarrival = rng.expovariate(1.0 / interarrival_mean_s) if interarrival_mean_s > 0 else 0.0
        arrival_time += interarrival

        duration = truncated_normal(rng, duration_mean_s, duration_std_dev_s, minimum=max(30.0, duration_mean_s * 0.3))
        cpu = rng.uniform(*cpu_bounds)
        memory = rng.uniform(*memory_bounds)

        node_id = f"N{rng.randint(1, num_nodes):03d}"

        tasks.append(
            TaskSpec(
                task_id=f"T{idx:04d}",
                node_id=node_id,
                arrival_offset_s=arrival_time,
                duration_s=duration,
                initial_cpu=cpu,
                initial_memory=memory,
                initial_bandwidth=0.0,
            )
        )

    return tasks


def assign_task_peers(
    rng: random.Random,
    tasks: List[TaskSpec],
    min_peers: int,
    max_peers: int,
    bandwidth_bounds: tuple[float, float],
) -> None:
    """Assign peer communication partners and baseline bandwidth per link."""
    if min_peers < 0 or max_peers < 0:
        raise ValueError("Peer counts must be non-negative.")

    for task in tasks:
        candidates = [t for t in tasks if t.task_id != task.task_id]
        if not candidates:
            task.peer_bandwidths = {}
            task.initial_bandwidth = 0.0
            continue

        max_available = min(max_peers, len(candidates))
        min_available = min(min_peers, max_available)

        if max_available == 0:
            task.peer_bandwidths = {}
            task.initial_bandwidth = 0.0
            continue

        if min_available > max_available:
            min_available = max_available

        peer_count = rng.randint(min_available, max_available) if max_available > 0 else 0
        selected = rng.sample(candidates, peer_count) if peer_count > 0 else []

        peer_map: Dict[str, float] = {}
        for peer in selected:
            peer_map[peer.task_id] = rng.uniform(*bandwidth_bounds)

        task.peer_bandwidths = peer_map
        task.initial_bandwidth = sum(peer_map.values())


def _resource_series(
    rng: random.Random,
    base_value: float,
    steps: int,
    daily_oscillation: float,
    volatility: float,
) -> List[float]:
    """Generate a positive resource usage series with smooth drift and noise."""
    if steps <= 0:
        return []

    values: List[float] = []
    trend = rng.uniform(-0.15, 0.25)
    phase_shift = rng.uniform(0, 2 * math.pi)
    current = max(base_value * 0.25, rng.gauss(base_value, base_value * 0.1))

    for step in range(steps):
        progress = step / max(steps - 1, 1)
        seasonal = 1.0 + daily_oscillation * math.sin(2 * math.pi * progress + phase_shift)
        drift = 1.0 + trend * progress
        shock = rng.gauss(0, volatility)
        current = max(base_value * 0.05, current * (1 + shock))
        value = max(base_value * 0.05, current * drift * seasonal)
        values.append(value)

    return values


def generate_telemetry(
    rng: random.Random,
    tasks: Iterable[TaskSpec],
    telemetry_steps: int,
    sampling_interval_s: float,
    daily_oscillation: float,
    volatility: float,
    peer_dropout_chance: float,
    base_timestamp: dt.datetime,
) -> tuple[List[dict], List[dict]]:
    resource_rows: List[dict] = []
    bandwidth_rows: List[dict] = []
    task_list = list(tasks)

    for task in task_list:
        cpu_series = _resource_series(rng, task.initial_cpu, telemetry_steps, daily_oscillation, volatility)
        mem_series = _resource_series(rng, task.initial_memory, telemetry_steps, daily_oscillation, volatility * 0.8)
        peer_series = {
            peer_id: _resource_series(
                rng,
                base_value,
                telemetry_steps,
                daily_oscillation * 1.2,
                volatility * 1.1,
            )
            for peer_id, base_value in task.peer_bandwidths.items()
        }

        for step in range(telemetry_steps):
            timestamp = base_timestamp + dt.timedelta(seconds=task.arrival_offset_s + sampling_interval_s * step)

            resource_rows.append(
                {
                    "timestamp": timestamp.isoformat(timespec="seconds"),
                    "node_id": task.node_id,
                    "task_id": task.task_id,
                    "cpu_usage": f"{cpu_series[step]:.3f}",
                    "memory_usage": f"{mem_series[step]:.3f}",
                }
            )

            for peer_id, series in peer_series.items():
                if rng.random() <= peer_dropout_chance:
                    continue
                bandwidth_rows.append(
                    {
                        "timestamp": timestamp.isoformat(timespec="seconds"),
                        "source_task_id": task.task_id,
                        "target_task_id": peer_id,
                        "bandwidth_usage": f"{series[step]:.3f}",
                    }
                )

    return resource_rows, bandwidth_rows


def write_csv(path: Path, fieldnames: List[str], rows: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic network control task telemetry data.")
    parser.add_argument("--num-tasks", type=int, default=50, help="Number of tasks to simulate.")
    parser.add_argument("--telemetry-steps", type=int, default=20, help="Number of telemetry samples per task.")
    parser.add_argument("--sampling-interval", type=float, default=15.0, help="Seconds between telemetry samples.")
    parser.add_argument("--num-nodes", type=int, default=10, help="Number of nodes available for task assignment.")
    parser.add_argument("--min-peers", type=int, default=1, help="Minimum number of peer tasks each task communicates with.")
    parser.add_argument("--max-peers", type=int, default=3, help="Maximum number of peer tasks each task communicates with.")
    parser.add_argument("--interarrival-mean", type=float, default=45.0, help="Average seconds between task arrivals.")
    parser.add_argument("--duration-mean", type=float, default=600.0, help="Mean task duration (seconds).")
    parser.add_argument("--duration-stddev", type=float, default=120.0, help="Standard deviation for task durations.")
    parser.add_argument("--cpu-range", type=float, nargs=2, default=(8.0, 28.0), metavar=("MIN", "MAX"), help="Initial CPU requirement range.")
    parser.add_argument("--memory-range", type=float, nargs=2, default=(16.0, 96.0), metavar=("MIN", "MAX"), help="Initial memory requirement range (GB).")
    parser.add_argument("--bandwidth-range", type=float, nargs=2, default=(1.0, 10.0), metavar=("MIN", "MAX"), help="Initial bandwidth range (Gbps).")
    parser.add_argument("--daily-oscillation", type=float, default=0.25, help="Amplitude of oscillation in the telemetry time series.")
    parser.add_argument("--volatility", type=float, default=0.12, help="Random noise level for telemetry time series.")
    parser.add_argument("--peer-dropout", type=float, default=0.1, help="Probability that a telemetry sample has no peer task.")
    parser.add_argument("--seed", type=int, default=2024, help="Random seed for reproducibility.")
    parser.add_argument("--output-dir", type=Path, default=Path("output"), help="Directory to place generated CSV files.")
    parser.add_argument("--base-timestamp", type=str, default=None, help="ISO timestamp for task arrival time zero (defaults to now).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)

    if args.cpu_range[0] <= 0 or args.memory_range[0] <= 0 or args.bandwidth_range[0] <= 0:
        raise ValueError("Resource ranges must be positive.")
    if args.telemetry_steps < 1:
        raise ValueError("Telemetry steps must be at least 1.")
    if args.num_tasks < 1:
        raise ValueError("At least one task must be generated.")
    if args.num_nodes < 1:
        raise ValueError("At least one node must be available.")
    if args.max_peers < args.min_peers:
        raise ValueError("max-peers must be greater than or equal to min-peers.")

    base_timestamp = (
        dt.datetime.fromisoformat(args.base_timestamp) if args.base_timestamp else dt.datetime.utcnow()
    )

    tasks = generate_tasks(
        rng=rng,
        num_tasks=args.num_tasks,
        num_nodes=args.num_nodes,
        interarrival_mean_s=args.interarrival_mean,
        duration_mean_s=args.duration_mean,
        duration_std_dev_s=args.duration_stddev,
        cpu_bounds=tuple(args.cpu_range),
        memory_bounds=tuple(args.memory_range),
        bandwidth_bounds=tuple(args.bandwidth_range),
    )

    assign_task_peers(
        rng=rng,
        tasks=tasks,
        min_peers=args.min_peers,
        max_peers=args.max_peers,
        bandwidth_bounds=tuple(args.bandwidth_range),
    )

    task_rows = [
        {
            "task_id": task.task_id,
            "arrival_offset_s": f"{task.arrival_offset_s:.1f}",
            "start_timestamp": (base_timestamp + dt.timedelta(seconds=task.arrival_offset_s)).isoformat(timespec="seconds"),
            "duration_s": f"{task.duration_s:.1f}",
            "initial_cpu": f"{task.initial_cpu:.3f}",
            "initial_memory": f"{task.initial_memory:.3f}",
            "peer_task_ids": ";".join(sorted(task.peer_bandwidths)),
            "peer_bandwidths": ";".join(f"{bandwidth:.3f}" for bandwidth in [task.peer_bandwidths[p] for p in sorted(task.peer_bandwidths)]),
        }
        for task in tasks
    ]

    telemetry_resource_rows, telemetry_bandwidth_rows = generate_telemetry(
        rng=rng,
        tasks=tasks,
        telemetry_steps=args.telemetry_steps,
        sampling_interval_s=args.sampling_interval,
        daily_oscillation=args.daily_oscillation,
        volatility=args.volatility,
        peer_dropout_chance=min(max(args.peer_dropout, 0.0), 1.0),
        base_timestamp=base_timestamp,
    )

    tasks_path = args.output_dir / "tasks.csv"
    telemetry_resource_path = args.output_dir / "telemetry_resources.csv"
    telemetry_bandwidth_path = args.output_dir / "telemetry_bandwidth.csv"

    task_fieldnames = [
        "task_id",
        "arrival_offset_s",
        "start_timestamp",
        "duration_s",
        "initial_cpu",
        "initial_memory",
        "peer_task_ids",
        "peer_bandwidths",
    ]
    resource_fieldnames = ["timestamp", "node_id", "task_id", "cpu_usage", "memory_usage"]
    bandwidth_fieldnames = ["timestamp", "source_task_id", "target_task_id", "bandwidth_usage"]

    write_csv(tasks_path, task_fieldnames, task_rows)
    write_csv(telemetry_resource_path, resource_fieldnames, telemetry_resource_rows)
    write_csv(telemetry_bandwidth_path, bandwidth_fieldnames, telemetry_bandwidth_rows)

    print(f"Wrote {len(task_rows)} tasks to {tasks_path}")
    print(f"Wrote {len(telemetry_resource_rows)} resource telemetry samples to {telemetry_resource_path}")
    print(f"Wrote {len(telemetry_bandwidth_rows)} bandwidth telemetry samples to {telemetry_bandwidth_path}")


if __name__ == "__main__":
    main()

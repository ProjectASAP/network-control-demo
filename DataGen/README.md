# Synthetic Network Task Telemetry Generator

This repository provides a Python-based utility for producing synthetic datasets for a network control task assignment scenario. It creates three CSV files:

- `tasks.csv` lists each generated task with arrival time, duration, and initial resource requirements, plus peer communication metadata.
- `telemetry_resources.csv` captures time-series CPU and memory usage for each task on its assigned node.
- `telemetry_bandwidth.csv` tracks bandwidth usage for individual task-to-task communications.

## Quick start

```bash
python3 generate_network_tasks.py \
  --num-tasks 100 \
  --telemetry-steps 40 \
  --sampling-interval 10 \
  --output-dir data/output
```

This command produces `data/output/tasks.csv`, `data/output/telemetry_resources.csv`, and `data/output/telemetry_bandwidth.csv`. By default, a deterministic seed (`2024`) is used so repeated runs yield identical data; provide `--seed` to vary the dataset.

## Key CLI options

- `--num-tasks`: number of synthetic tasks to create.
- `--telemetry-steps`: telemetry samples per task (each sample corresponds to one timestamp).
- `--sampling-interval`: seconds between telemetry samples for a given task.
- `--num-nodes`: number of nodes available for task assignment.
- `--min-peers` / `--max-peers`: bounds on how many peer tasks each task communicates with.
- `--interarrival-mean`: expected arrival spacing between tasks (seconds, exponential interarrival).
- `--duration-mean` / `--duration-stddev`: controls task duration distribution (seconds).
- `--*_range`: bounds for initial resource requirements (CPU units, GB memory, Gbps bandwidth).
- `--daily-oscillation`, `--volatility`: adjust telemetry smoothness vs. variability.
- `--peer-dropout`: probability that a telemetry record has no peer assignment.
- `--base-timestamp`: ISO 8601 string to anchor arrival time zero (defaults to current UTC).

Run `python3 generate_network_tasks.py --help` to see every option.

## Output schema

`tasks.csv` columns:

- `task_id`: unique identifier (e.g., `T0007`).
- `arrival_offset_s`: seconds after the base timestamp when the task arrives.
- `start_timestamp`: ISO 8601 timestamp for the task arrival.
- `duration_s`: task duration in seconds.
- `initial_cpu`, `initial_memory`: baseline compute and memory requirements.
- `peer_task_ids`: semicolon-delimited list of peer tasks this task communicates with.
- `peer_bandwidths`: semicolon-delimited bandwidth requirements aligned by index with `peer_task_ids` (Gbps).

`telemetry_resources.csv` columns:

- `timestamp`: ISO 8601 sample time.
- `node_id`: node hosting the task at that timestamp.
- `task_id`: emitting task.
- `cpu_usage`, `memory_usage`: sampled CPU and memory consumption.

`telemetry_bandwidth.csv` columns:

- `timestamp`: ISO 8601 sample time.
- `source_task_id`: task initiating communication.
- `target_task_id`: peer task in the exchange.
- `bandwidth_usage`: sampled bandwidth consumption (Gbps).

## Verification

You can generate a small sample to validate everything is wired correctly:

```bash
python3 generate_network_tasks.py --num-tasks 3 --telemetry-steps 4 --output-dir sample_output
head sample_output/tasks.csv
head sample_output/telemetry_resources.csv
head sample_output/telemetry_bandwidth.csv
```

The output directory is created automatically if it is missing.

# CLAUDE.md — solver_experimental

## What This Is

The Python side of the network control demo: a **task scheduling optimizer** that queries node metrics from either the Sketch server or Elasticsearch, runs an ILP solver to place tasks on nodes, and benchmarks the two backends for latency and correctness.

## Package Manager & Python Version

- **uv** (see `pyproject.toml`, `uv.lock`). Use `uv run <script>` to execute.
- Requires **Python 3.13+**.
- Key deps: pulp, networkx, elasticsearch, fastapi, cattrs, loguru, pandas, requests, httpx

## Entry Points

```bash
# Full run (telemetry emulator + solver loop)
bash run_main.sh

# Solver only (assumes telemetry emulator already running on :8000)
uv run main.py --node-path dummy_data/nodes.jsonl --edge-path dummy_data/edges.jsonl \
  --task-path dummy_data/tasks.jsonl --query-manager-config configs/sample.yml

# Telemetry emulator only (FastAPI on :8000)
uv run emulate_telemetry.py

# Benchmarks
uv run bench_queries.py
```

`run_main.sh` starts `emulate_telemetry.py` in the background, then runs `main.py`.

## Directory Structure

```
solver_experimental/
├── main.py                    # Orchestrator: event loop, solver invocation, metric comparison
├── config.py                  # All env-var-based configuration
├── emulate_telemetry.py       # FastAPI sidecar: generates and sends synthetic metrics
├── es_query.py                # Builds queries for both backends, compares results
├── logging_utils.py           # CSV logging: RTT, e2e, metric comparisons
├── bench_queries.py           # Query RTT benchmark suite
├── analyze_logs.py            # Server log analysis
├── run_main.sh                # Shell entry point
│
├── scheduler/                 # PuLP-based task scheduler
│   ├── entities.py            # Core types: Node, Edge, Task, RunningTask, NetworkTopology
│   ├── solver.py              # TaskScheduler: ILP formulation with PuLP
│   └── load_info.py           # Loads nodes/edges/tasks from CSV or JSONL
│
├── query_engine_utils/        # Query abstraction layer
│   ├── config.py              # QueryManagerConfig, ServerType, QueryGroupConfig
│   ├── server_querying.py     # QueryManager + PromQL/ES clients
│   └── update_task_info.py    # Task metric update methods
│
├── python_solver/             # OR-Tools solver (independent, more mature)
│   ├── src/network_controller/
│   │   ├── solver.py          # NetworkControllerSolver (OR-Tools MILP)
│   │   └── io.py              # JSON/CSV/JSONL I/O
│   ├── tests/test_solver.py   # Unit tests
│   └── examples/run_from_files.py
│
├── convex-optimization-project/  # CVXPY solver (experimental, incomplete)
│
├── configs/sample.yml         # Query manager configuration
├── dummy_data/                # JSONL test data (nodes, edges, tasks)
├── pyproject.toml             # Project metadata and dependencies
└── uv.lock                    # Lockfile
```

## Core Data Types (`scheduler/entities.py`)

| Type | Fields | Description |
|---|---|---|
| `Node` | node_id, cpu_capacity, memory_capacity, network_capacity?, used_cpu/memory/network | Compute node with capacity and current usage |
| `Edge` | edge_id (tuple), capacity, used_bandwidth | Network link between two nodes |
| `Task` | task_id, arrival_offset_s, duration_s, initial_cpu, initial_memory, peer_bandwidths | Task needing placement |
| `RunningTask` | node_id, start_time_s, task | A placed task — used for serialization with cattrs |
| `NetworkTopology` | — | networkx graph wrapping Node/Edge objects |

`NetworkTopology` is undirected by default. `peer_bandwidths` on `Task` is `dict[str, float]` mapping peer task IDs to bandwidth requirements.

## Data Formats (JSONL)

**nodes.jsonl**: `{"node_id": "N001", "cpu_capacity": 16, "memory_capacity": 64, "network_capacity": 1000}`

**edges.jsonl**: `{"source": "N001", "target": "N002", "capacity": 100}`

**tasks.jsonl**: `{"task_id": "T001", "arrival_offset_s": 0, "duration_s": 3600, "initial_cpu": 2, "initial_memory": 8, "peer_bandwidths": {"T002": 10}}`

CSV format also supported (peer data semicolon-delimited in CSV).

## How `main.py` Works

`assign_tasks()` is a **generator** that yields assignment dicts. The main loop:

1. Reads CSV time bounds to determine epochs (`epoch_length_s = 3000`)
2. Instantiates tasks per epoch with unique IDs (e.g. `T001_e0`, `T001_e1`)
3. Interleaves two event streams: CSV row ingestion and task arrivals (by time offset)
4. For each batch of arrived tasks:
   - Queries node metrics from Sketch server (cumulative usage per node)
   - Optionally queries ES in parallel for comparison (`PARALLEL_BENCHMARK_ENABLED`)
   - Updates `node.used_cpu/memory` from cumulative metrics
   - Runs `TaskScheduler.solve()` — once with sketch metrics, optionally again with ES metrics
   - Compares solver assignments between backends
   - Logs RTT, e2e timing, and metric comparisons to CSVs
   - Pushes assignments back to telemetry emulator (`POST /ingest`)
5. Carries over unassigned tasks (with retry limit of 5 before marking as failed)

## Solver (`scheduler/solver.py`)

PuLP CBC-based integer linear program:

- **Decision vars**: `d[t][n]` binary (task t assigned to node n), `allocated[t]` binary
- **Objective**: minimize `-total_allocated + penalty * reassignments` (maximize placements, penalize reassignments)
- **Constraints**:
  1. Each task assigned to at most one node
  2. Node capacity: CPU, memory, optional network
  3. Communicating task pairs must use exactly one path (or co-locate)
  4. Edge bandwidth capacity
  5. Max reassignments limit
- **Returns**: `(assignments, leftover_tasks, objective_value, status_code)`

Tasks whose peer group is incomplete (not all peers present) are filtered out via `get_valid_task_graph()`.

## Telemetry Emulator (`emulate_telemetry.py`)

FastAPI server on `127.0.0.1:8000`:

| Endpoint | Method | Description |
|---|---|---|
| `/ingest` | POST | Receives task assignments, generates synthetic metrics |
| `/ingest_rows` | POST | Receives raw CSV rows, forwards to Sketch + ES |

Background loop periodically pushes `MetricsEmulator.create_metrics_records()` to both backends.

`MetricsEmulator` generates noisy sinusoidal timeseries for each task based on `initial_cpu`/`initial_memory`. Records are sent as parallel arrays matching the Sketch server's `IngestRecord` format.

## Query Layer (`es_query.py`)

Two payload builders:
- `build_sketch_node_metrics_payload()` — builds `aggs` with `cumulative` per node per field (percentiles and top_entities are commented out)
- `build_es_node_metrics_payload()` — builds ES filters aggregation with `sum` per node (uses painless script for time-based filtering by estimated_duration)

`fetch_node_usage()` → `get_node_metrics()` → `send_search_request_payload()` is the call chain. Returns `dict[str, NodeMetricsSnapshot]`.

`compare_node_metrics()` checks relative differences between sketch and ES snapshots with configurable tolerance.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `SKETCH_URL` | `http://localhost:10101` | Sketch server URL |
| `ES_URL` | `http://localhost:9200` | Elasticsearch URL |
| `ES_API_KEY` | (hardcoded default) | ES authentication |
| `SKETCH_API_KEY` | same as ES_API_KEY | Sketch server auth |
| `ES_INDEX_NAME` | `cluster-metrics` | ES index name |
| `ES_TIME_FIELD` | `@timestamp` | Timestamp field for ES queries |
| `CLUSTER_METRICS_CSV` | `~/cluster-metrics.csv` | Input metrics data |
| `TIME_RANGE_MS` | `3000000` | Query time window |
| `SCHEDULER_BATCH_SIZE` | `5` | Max tasks per solver batch |
| `PARALLEL_BENCHMARK_ENABLED` | `true` | Run both sketch + ES paths |
| `CONSISTENCY_CHECK_TOLERANCE` | `0.01` | Relative diff threshold for metric comparison |
| `SKETCH_INGEST_ENABLED` | `true` | Send data to sketch server |
| `ES_INGEST_ENABLED` | `true` | Send data to ES |
| `NODE_QUERY_LIMIT` | — | Limit nodes queried (for testing) |
| `INGEST_POST_TIMEOUT_SECONDS` | `30` | Timeout for ingest HTTP calls |

## Output CSVs

| File | Columns | Written by |
|---|---|---|
| `query_rtt.csv` | request_id, correlation_id, request_type, target, duration_ms, status, ok, error | `log_rtt()` |
| `e2e.csv` | timestamp, correlation_id, offset_s, tasks_to_schedule, ran_solver, metrics_source, duration_ms, assignment | `log_e2e()` |
| `query_compare.csv` | correlation_id, node_id, {metric}_{percentile}_{sk\|es}, {metric}_sum_{sk\|es}, top_entity_{sk\|es} | `log_node_metric_comparisons()` |

## Tests

```bash
# OR-Tools solver tests
uv run pytest python_solver/tests/
```

No tests for the PuLP scheduler (`scheduler/`) currently.

## Two Solvers

| | PuLP (`scheduler/solver.py`) | OR-Tools (`python_solver/`) |
|---|---|---|
| Used by | `main.py` (active) | Standalone / examples |
| Formulation | Minimize reassignments, maximize placements | Similar + migration penalties |
| Path routing | Single path per node pair (TODO: multi-path) | Flow variables |
| Status | Active, integrated with query/ingest pipeline | More mature formulation, not wired into main loop |

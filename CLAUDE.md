# CLAUDE.md — network-control-demo

## Project Overview

A proof-of-concept **network control system** that pairs a high-performance Rust metric aggregation server (using KLL sketches) with a Python-based network task scheduler (mixed-integer programming). The project benchmarks this approach against traditional Elasticsearch for both latency and correctness.

## Repository Layout

```
.
├── single_node_server/          # Rust HTTP server for metric aggregation
├── solver_experimental/         # Python solver, telemetry emulator, benchmarking
├── *.py                         # Root-level Python scripts (data gen, RTT sweeps, plotting)
├── evaluate_demo.sh             # Full pipeline orchestrator
├── logs/                        # Runtime server logs (gitignored)
├── *.csv / *.png                # Generated results and plots (not committed)
└── CLAUDE.md
```

## Component Details

### `single_node_server/network-control-server/` — Rust Sketch Server

An Axum-based HTTP server that ingests cluster metrics and serves aggregated queries using KLL quantile sketches instead of Elasticsearch.

- **Entry**: `src/main.rs` — loads configs, builds `AppState`, serves on `0.0.0.0:10101`
- **Config**: `src/config.rs` — `AggregationConfig` (from `agg-config.yaml`) and `NodesConfig` (node ID range)
- **Metrics store**: `src/metrics/store.rs` — `NodeStore` with per-node KLL sketches for CPU, memory, network
- **Handlers**: `src/server/handlers.rs` — routes: `POST /` (ingest), `POST /cluster-metrics/_search`, `POST /cluster-metrics/_batch`, `POST /metrics/:field`, `GET /healthz`
- **Query**: `src/server/query.rs` — percentile and cumulative aggregation queries against sketches
- **Types**: `src/server/types.rs` — `AppState`, request/response types
- **Upstream**: `src/server/upstream.rs` — forwards queries to Elasticsearch
- **External dep**: `sketchlib-rust` (local path: `/users/yuanyc/sketchlib-rust`)

**Build & run:**
```bash
cd single_node_server/network-control-server
cargo build          # or cargo run -- --timing
```

**Key env vars:** `UPSTREAM_URL` (ES endpoint, default `http://localhost:9200/cluster-metrics/_search`), `AGG_CONFIG_PATH`

### `solver_experimental/` — Python Solver & Benchmarking

The main Python package containing the task scheduler, query engine, telemetry emulator, and benchmarking tools.

**Package manager:** `uv` (see `pyproject.toml`, `uv.lock`). Requires Python 3.13+.

#### Core files

| File | Purpose |
|---|---|
| `main.py` | Orchestrator: loads topology, queries metrics, runs solver in batch loop, logs results |
| `config.py` | Env-var-based config (`SKETCH_URL`, `ES_URL`, `ES_API_KEY`, `TIME_RANGE_MS`, etc.) |
| `emulate_telemetry.py` | FastAPI server that generates and sends synthetic metrics to ES + Sketch server |
| `es_query.py` | ES/Sketch query builders, metric comparison, `NodeMetricsSnapshot` |
| `logging_utils.py` | CSV logging helpers (`log_rtt`, `log_e2e`, `log_node_metric_comparisons`) |
| `bench_queries.py` | Query RTT benchmark suite with plotting |
| `analyze_logs.py` | Server log analysis |

#### `scheduler/` — Task scheduling core

- `entities.py` — Data types: `Node`, `Edge`, `Task`, `RunningTask`, `TaskCommunication`, `NetworkTopology` (networkx)
- `load_info.py` — Loads nodes/edges/tasks from CSV or JSONL, builds task graph
- `solver.py` — `TaskScheduler` using PuLP ILP: placement constraints, capacity, link capacity, migration budget; objective = maximize priority

#### `python_solver/` — OR-Tools solver (more mature, independent)

- `src/network_controller/solver.py` — `NetworkControllerSolver`: task placement via OR-Tools MILP with migration penalties
- `src/network_controller/io.py` — JSON/CSV/JSONL I/O for nodes, tasks, edges, assignments
- `tests/test_solver.py` — Unit tests
- `examples/run_from_files.py` — Standalone usage example

#### `convex-optimization-project/` — CVXPY solver (experimental, incomplete)

CVXPY-based formulation with separate modules for decision variables, capacity constraints, and data loading.

#### `query_engine_utils/` — Query abstraction

- `config.py` — `QueryManagerConfig`, `ServerType` enum (PROMETHEUS, ELASTICSEARCH)
- `server_querying.py` — `QueryManager`: executes query groups against multiple backends
- `update_task_info.py` — Updates task metrics from query results

#### `configs/sample.yml` — Query manager configuration (server types, query groups, update rules)

**Entry point:**
```bash
cd solver_experimental
uv run main.py --node-path dummy_data/nodes.jsonl --edge-path dummy_data/edges.jsonl \
  --task-path dummy_data/tasks.jsonl --query-manager-config configs/sample.yml
```

### Root-level Python scripts

| Script | Purpose |
|---|---|
| `generate_cluster_metrics.py` | Generates synthetic cluster metrics CSV (small, 4 clusters, 6 tasks) |
| `generate_cluster_metrics_running_tasks.py` | Generates realistic metrics from solver topology data (~31 clusters) |
| `reset_es_index.py` | Resets Elasticsearch `cluster-metrics` index with field mappings |
| `reset_and_ingest.py` | Resets ES index + ingests metrics from CSV |
| `run_rtt_sweep.py` | RTT benchmark: server vs ES, configurable row counts and batch sizes |
| `run_rtt_sweep_epoch.py` | Epoch-based RTT sweep (measures RTT changes across data epochs) |
| `run_rtt_sweep_epoch_with_solver.py` | RTT sweep + solver integration per epoch |
| `plot_query_rtt.py` | Plots query RTT from `query_rtt.csv` |
| `plot_epoch_cumulative.py` | Cumulative RTT analysis across epochs |
| `plot_solver_comparison.py` | Compares multiple solver runs (reads `rtt_solver_*.csv`) |

### `evaluate_demo.sh` — Full pipeline

Orchestrates end-to-end execution:
1. Kills existing server on port 10101
2. Cleans previous result CSVs
3. Resets ES index
4. Builds and starts Rust server with `--timing`
5. Runs `solver_experimental/run_main.sh` (starts telemetry emulator + solver)

Usage: `bash evaluate_demo.sh [NODE_QUERY_LIMIT]`

## Key Environment Variables

| Variable | Default | Description |
|---|---|---|
| `UPSTREAM_URL` | `http://localhost:9200/cluster-metrics/_search` | ES upstream for Rust server |
| `SKETCH_URL` | — | Sketch server URL |
| `ES_URL` | — | Elasticsearch URL |
| `ES_API_KEY` | — | ES authentication key |
| `SKETCH_API_KEY` | — | Sketch server auth key |
| `CLUSTER_METRICS_CSV` | `~/cluster-metrics.csv` | Path to cluster metrics data |
| `TIME_RANGE_MS` | `3000000` | Query time window in ms |
| `SCHEDULER_BATCH_SIZE` | — | Tasks per solver batch |
| `NODE_QUERY_LIMIT` | — | Limit nodes queried (for testing) |
| `ES_INDEX_NAME` | `cluster-metrics` | ES index name |
| `ES_TIME_FIELD` | `@timestamp` | Timestamp field name |

## Build & Run

### Prerequisites
- Rust toolchain (for `single_node_server`)
- Python 3.13+ with `uv` package manager (for `solver_experimental`)
- Elasticsearch instance (for comparison benchmarks)
- Local `sketchlib-rust` crate at `/users/yuanyc/sketchlib-rust`

### Quick start
```bash
# Full pipeline
bash evaluate_demo.sh

# Rust server only
cd single_node_server/network-control-server && cargo run -- --timing

# Solver only (assumes server + ES running)
cd solver_experimental && bash run_main.sh

# RTT benchmarks
python3 run_rtt_sweep.py
python3 run_rtt_sweep_epoch.py
```

### Tests
```bash
# OR-Tools solver tests
cd solver_experimental && uv run pytest python_solver/tests/
```

## Architecture Notes

- The Rust server uses **KLL sketches** (from `sketchlib-rust`) for approximate quantile queries, providing O(1) query time vs ES's full scan
- Two solver implementations exist: **PuLP** (`scheduler/solver.py`) and **OR-Tools** (`python_solver/`). The OR-Tools version is more mature with migration penalties and reassignment limits
- The telemetry emulator (`emulate_telemetry.py`) runs as a FastAPI sidecar, sending identical data to both ES and Sketch server for consistency comparison
- Benchmark scripts measure both **latency** (RTT) and **correctness** (metric value comparison between backends)

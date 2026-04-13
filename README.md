# network-control-demo

Proof-of-concept network control demo for epoch-based task placement.

## Problem Specification

At each scheduling epoch, a controller must place arriving tasks onto a compute cluster.
Each node and link has finite resources:

- node CPU and memory
- inter-node link bandwidth
- optional placement constraints and migration limits

The controller depends on fresh telemetry to estimate available capacity before solving the
placement problem. Query latency can become a bottleneck as data grows. This demo compares:

- Elasticsearch queries over raw telemetry
- sketch-based approximate queries from a Rust server (KLL sketches)

The goal is to reduce query time while keeping placement quality and metric accuracy usable.

## Repository Components

- `single_node_server/network-control-server`: Rust HTTP server for ingest + sketch queries
- `solver_experimental`: Python orchestrator, telemetry emulator, benchmark scripts, PuLP solver
- `solver_experimental/python_solver`: OR-Tools-based solver package (used by benchmarks)
- `solver_experimental/convex-optimization-project`: CVXPY prototype solver
- `visualization`: live terminal dashboard for ingest/query/solve loop
- root scripts: Elasticsearch reset/ingest helpers and evaluation pipeline

## Quick Start

## Prerequisites

- Rust toolchain (for sketch server)
- Python 3.13+ and `uv` (for `solver_experimental`)
- Elasticsearch running locally or remotely
- local `sketchlib-rust` dependency available for the Rust server build

## 1) Start the sketch server (Rust)

```bash
cd single_node_server/network-control-server
cargo run -- --timing
```

Docker option:

```bash
cd single_node_server/network-control-server
./docker-build.sh -t network-control-server:latest
docker run --rm -p 10101:10101 network-control-server:latest
```

## 2) Run solver + telemetry loop

```bash
cd solver_experimental
bash run_main.sh
```

Or run directly:

```bash
cd solver_experimental
uv run main.py \
	--node-path dummy_data/nodes.jsonl \
	--edge-path dummy_data/edges.jsonl \
	--task-path dummy_data/tasks.jsonl \
	--query-manager-config configs/sample.yml
```

## 3) Run benchmark suites

```bash
bash scripts/run_rtt_sweep_all.sh
python3 scripts/run_rtt_sweep_epoch_full_ortools.py --run-solver --solver-backend SCIP
python3 scripts/run_dynamic_epoch_benchmark.py --solver-backend SCIP --max-epochs 50
```

## 4) Run full end-to-end demo pipeline

```bash
bash evaluate_demo.sh
```

This script resets index state, starts/restarts the sketch server, and runs the main solver flow.

## Outputs

- CSVs: `data/`
- plots: `plots/`
- logs: `logs/` (gitignored)

## Useful Environment Variables

- `UPSTREAM_URL`: Elasticsearch upstream for server fallback/search
- `SKETCH_URL`: sketch server URL used by solver/query scripts
- `ES_URL`, `ES_API_KEY`: Elasticsearch endpoint and auth
- `TIME_RANGE_MS`: telemetry lookback window
- `NODE_QUERY_LIMIT`, `SCHEDULER_BATCH_SIZE`: test-time scaling knobs

## Validation / Tests

```bash
cd solver_experimental
uv run pytest python_solver/tests/
```

## Notes

- Approximate telemetry from sketches accelerates query-phase latency, especially at scale.
- OR-Tools and PuLP solver paths coexist; OR-Tools is the more mature solver implementation.

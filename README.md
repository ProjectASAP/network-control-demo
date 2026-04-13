# network-control-demo

Proof-of-concept network control demo for epoch-based task placement with approximate analytics layer.

## Problem Specification

### Problem Definition

Given a set of arriving tasks with specific resource demands (CPU, RAM) and inter-task communication requirements (bandwidth), a central controller must maximize number of task allocations while adhering to physical cluster constraints.

The controller enforces joint optimization over:

- **Node Constraints:** Total CPU and Memory capacity.
- **Network Constraints:** Hierarchical tree topology with finite inter-node link bandwidth.

To simulate running tasks on a cluster, we run a separate emulator alongside our controller that emits telemetry based on received task placement and resource allocations.

### Experimental Evaluation

This repository evaluates a telemetry architecture designed to accelerate Elasticsearch analytic queries loop by leveraging sketch techniques. We compare three distinct approaches:

- **Exact Retrieval (Baseline):** Queries executed over raw telemetry stored in Elasticsearch.

- **Approximate Fast Layer:** Accelerated telemetry retrieval using a high-performance Rust-based sketch server (utilizing KLL sketches for rank-based statistics and quantile estimation).

- **Static Baseline:** Task placement based on initial capacity without periodic telemetry updates.

### Key Metrics:

- **End-to-End Control-Loop Latency:** Time elapsed from telemetry query initiation to solver completion.

- **Task Placement Quality:** Total tasks successfully assigned per epoch.

## Repository Components

- `single_node_server/network-control-server`: Rust HTTP server for ingest + sketch queries
- `solver_experimental/`: Python orchestrator, telemetry emulator, benchmark scripts, PuLP solver
- `solver_experimental/python_solver`: OR-Tools-based solver package (used by benchmarks)
- `visualization`: live terminal dashboard for ingest/query/solve loop
- root scripts: Elasticsearch reset/ingest helpers and evaluation pipeline

## Quick Start

## Prerequisites

- Rust toolchain (for sketch server)
- Python 3.13+ and `uv` (for `solver_experimental`)
- Elasticsearch running locally or remotely
- local `asap_sketchlib` dependency available for the Rust server build

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
# Starts the telemetry emulator, and then starts the controller/solver.
cd solver_experimental
bash run_main.sh
```

This will produce the following output under `logs/`:
- `assignments.csv`: Information on tasks attempted/allocated at each epoch.
- `loop_rtt.csv`: End to end runtime of controller loop and solver runtime.
- `query_rtt.csv`: Query execution time at each epoch. 
- `sketch_ingest.csv`: Time to ingest telemetry into server.

By default, the controller queries the sketch server. To make it use Elasticsearch instead, add the `--use-es` flag to the bottom `run_main.sh`. You may also have to remove the `--no-es-ingest` (and replace with `--no-sketch-ingest`) flag from the emulator so that the emulator pushes metrics to Elasticsearch instead. To see how to configure the correct Elasticsearch endpoint to use, see the [**environment variables section**](#useful-environment-variables).

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

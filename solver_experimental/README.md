# solver_experimental

Main Python package for the epoch-based network controller demo.

## Problem Context

Each epoch, new tasks arrive and must be assigned to cluster nodes subject to:

- node CPU and memory capacities
- link bandwidth constraints between communicating tasks
- optional migration limits from the previous epoch

To evaluate runtime bottlenecks, this package queries telemetry from both Elasticsearch and
the sketch server. The optimization objective is to maximize schedulable workload quality
(task priority) while respecting resource constraints.

## What Is Here

- `main.py`: orchestrator for query -> solve loop
- `scheduler/`: PuLP-based scheduling implementation
- `python_solver/`: OR-Tools solver package (more mature)
- `emulate_telemetry.py`: FastAPI telemetry generator/forwarder
- `es_query.py`: backend query and metric comparison helpers
- `bench_queries.py` and `../scripts/*.py`: benchmark drivers

## Prerequisites

- Python 3.13+
- `uv` package manager
- sketch server running (`single_node_server/network-control-server`)
- Elasticsearch running for A/B comparisons

## Quick Run

## Install dependencies

```bash
uv sync
```

## Run the main controller loop

```bash
uv run main.py \
	--node-path dummy_data/nodes.jsonl \
	--edge-path dummy_data/edges.jsonl \
	--task-path dummy_data/tasks.jsonl \
	--query-manager-config configs/sample.yml
```

## Run helper wrapper

```bash
bash run_main.sh
```

## Run telemetry emulator only

```bash
uv run emulate_telemetry.py
```

## Benchmarks

From repo root:

```bash
python3 scripts/run_rtt_sweep.py
python3 scripts/run_rtt_sweep_epoch_full.py --run-solver
python3 scripts/run_rtt_sweep_epoch_full_ortools.py --run-solver --solver-backend SCIP
python3 scripts/run_dynamic_epoch_benchmark.py --solver-backend SCIP --max-epochs 50
```

## Key Environment Variables

- `SKETCH_URL`: sketch server endpoint
- `ES_URL`, `ES_API_KEY`: Elasticsearch endpoint and auth
- `TIME_RANGE_MS`: metric lookback window
- `NODE_QUERY_LIMIT`: cap nodes queried per epoch for scaling tests
- `SCHEDULER_BATCH_SIZE`: tasks solved per batch
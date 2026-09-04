# CLAUDE.md — network-control-demo

## Maintenance Rule

**Keep this file up to date.** Whenever a non-trivial change is made — new files or modules, renamed/removed files, new features, API changes, changed entry points, updated dependencies, or altered build/run instructions — update the relevant sections of this document to reflect the current state of the project.

## Project Overview

A proof-of-concept **network control system** that pairs a high-performance Rust metric aggregation server (using KLL sketches) with a Python-based network task scheduler (mixed-integer programming). The project benchmarks this approach against traditional Elasticsearch for both latency and correctness.

## Repository Layout

```
.
├── scripts/                     # RTT sweep and plotting scripts
├── data/                        # Generated benchmark CSV outputs
├── plots/                       # Generated benchmark PNG plots
├── logs/                        # Runtime logs for RTT sweep scripts (gitignored)
├── single_node_server/          # Rust HTTP server for metric aggregation
├── solver_experimental/         # Python solver, telemetry emulator, benchmarking
├── *.py                         # Root-level utility scripts
├── evaluate_demo.sh             # Full pipeline orchestrator
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
- **External deps**: `asap_sketchlib = "0.2.2"` (crates.io) and `elasticsearch-dsl-ast` (git: `ProjectASAP/elasticsearch-dsl-ast`, rev pinned in `Cargo.lock`). No local path dependencies — a fresh clone builds standalone.
- **Docker**: `Dockerfile` + `docker-build.sh` are stale (they vendor `asap_sketchlib` and never copy `elasticsearch-dsl-ast`); the Docker build path does not currently work

**Build & run:**
```bash
cd single_node_server/network-control-server
cargo build          # or cargo run -- --timing
# Docker:
./docker-build.sh -t network-control-server:latest
docker run -p 10101:10101 network-control-server:latest
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

- `src/network_controller/solver.py` — `NetworkControllerSolver`: task placement via OR-Tools MILP with migration penalties. `AssignmentResult` carries `status` (`OPTIMAL` / `FEASIBLE` / `NOT_SOLVED`), `wall_time_ms` and `best_bound` (with `relative_gap()`), so a caller can tell a proven-optimal solve from one that stopped at `time_limit_s`. `solve()` also takes `raise_on_no_solution=False` (return an empty result on a deadline miss instead of raising), `mip_gap` (stop within a relative gap), and `migration_penalty` — the paper's **λ**, subtracted from the objective per task moved off its previous node; `max_task_movements` is the paper's **γ**. `Task.must_assign` forbids skipping a task, so running workloads can be reassigned but never evicted
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

### `scripts/` benchmark scripts

| Script | Purpose |
|---|---|
| `scripts/run_rtt_sweep.py` | RTT benchmark: server vs ES, configurable row counts and batch sizes |
| `scripts/run_rtt_sweep_epoch.py` | Epoch-based RTT sweep |
| `scripts/run_rtt_sweep_epoch_with_solver.py` | Epoch-based RTT sweep with solver timings |
| `scripts/run_rtt_sweep_epoch_full.py` | Epoch-based sweep: ingest + query + solver timing for both backends (PuLP) |
| `scripts/run_rtt_sweep_epoch_full_ortools.py` | Same as above but using OR-Tools solver instead of PuLP; supports `--solver-backend {CBC,SCIP,GLPK}` |
| `scripts/run_dynamic_epoch_benchmark.py` | Dynamic epoch benchmark using emulator-generated task telemetry + padding to target rows/epoch; measures ingest/query/solver for Sketch vs ES |
| `scripts/run_es_ingest_query_sweep.py` | ES-only ingest/query sweep over exponentially increasing row counts; writes detail + summary CSVs and a plot |
| `scripts/run_sketch_ingest_query_sweep.py` | Sketch-server-only ingest/query sweep over exponentially increasing row counts using the release binary; writes detail + summary CSVs and a plot |
| `scripts/rtt_sweep_common.py` | Shared helpers for RTT sweeps |
| `scripts/proc_monitor.py` | OS-level (`/proc`) resource measurement: resolve sketch/ES PIDs, read CPU ticks (utime+stime) + whole-process RSS (VmRSS/VmHWM), background `ResourceSampler`, `taskset` CPU pinning. Run `--selftest` to print resolved PIDs/counters |
| `scripts/run_resource_benchmark.py` | Resource (CPU + whole-process RSS) benchmark mirroring the latency experiment: `--runs` x `--epochs` at `--rows-per-epoch` (default 1M). Sketch vs ES, measured symmetrically via `/proc` in the same window as latency. Per measurement point uses ADAPTIVE repeats (queries until the wall window reaches `--min-measure-seconds`) so CPU accumulates above jiffy granularity for both backends. Pins client/server/ES to disjoint cores; subtracts idle CPU baseline; restarts the server per run; writes summary CSV + ingestion CSV + per-(run,epoch,backend) raw JSON sidecars |
| `scripts/plot_resource_benchmark.py` | Plots from the resource benchmark (auto-prunes files it no longer produces; `--no-prune` to keep). Headline CPU comparisons with readable value labels (`query_cpu_headline.png`, `ingestion_cpu_headline.png`); query-comparison-style per-epoch grouped bar trios `query_cpu_bars_*` / `ingestion_cpu_bars_*` (ingestion is compression-independent → its default/large are identical); and combined sketch-server-only resource panels `sketch_query.png` / `sketch_ingestion.png` (CPU + whole-process RSS side by side, absolute, with cumulative-rows annotation). ES RSS is intentionally NOT plotted as a memory baseline — it is just ES's fixed pre-allocated JVM heap, not actual usage. CPU is labelled "CPU time (all threads)" since it sums over threads and can exceed wall latency |
| `scripts/plot_query_rtt.py` | Plot query RTT logs |
| `scripts/plot_epoch_cumulative.py` | Plot cumulative epoch RTT |
| `scripts/plot_solver_comparison.py` | Plot solver comparison graphs |
| `scripts/run_rtt_sweep_all.sh` | Runs all three RTT sweeps with `data/` + `plots/` + `logs/` defaults |
| `scripts/run_dynamic_epoch_benchmark_all.sh` | Runs dynamic epoch benchmark across solver backends (e.g., CBC/SCIP) with standard `data/` + `plots/` + `logs/` outputs |
| `scripts/run_resource_benchmark_all.sh` | Runs the resource benchmark (selftest → benchmark → plots) with standard `data/` + `plots/resource/` + `logs/` outputs; honors `RUNS`/`DATA_VOLUMES`/`REPEATS`/`*_CORES` env overrides |
| `scripts/raw_data_prep.py` | Builds `data/raw_topology/{nodes,edges,tasks}.jsonl` from the `raw_data/` telemetry dump. Generates a **rolling-arrival** workload sized by offered load (see **raw_data experiment** below) |
| `scripts/run_raw_data_assignment.py` | Sketch-vs-ES assignment experiment on the raw_data cluster: replays `synthetic_cpu_var.csv` per epoch, queries both backends, and drives two *independent* scheduling simulations. `--runs N` repeats the trajectory for run-to-run error bars; every solve's MILP status is logged |
| `scripts/plot_raw_data_assignment.py` | Plots from that experiment → `plots/raw_data/{query_solver,assignment}.png` plus `data/raw_data_assignment_summary.csv`; lines are means over runs with min..max bands |
| `scripts/run_raw_data_completion.py` | **Completion-throughput experiment (paper Fig. 8/9/10)** on the raw_data cluster: static vs dynamic telemetry, γ/λ reassignments, per-task telemetry and a contention model. Scenario presets via `--figure 8\|9\|10\|all` |
| `scripts/plot_raw_data_completion.py` | Plots that experiment → `plots/raw_data/completion_fig{8,9,10}.png` + `data/raw_data_completion_summary.csv`; prints the per-scenario `% vs static` table. One figure per comparison, each with its own baseline — nine series in one panel would force a repeated categorical hue |

### raw_data experiment

Runs the paper's setup against a real cluster trace instead of uniform synthetic
rows. Inputs come from a `raw_data/` dump — located via `$RAW_DATA_DIR`, else
`~/raw_data/`, else `~/Downloads/raw_data/`:

| File | Used as |
|---|---|
| `cpu_alloc.csv` − `pod_reqs.csv` | static node CPU capacity, `(allocatable − requests)/1000` cores |
| `cpu_var.csv` | per-node memory capacity (max observed `memory_available`) |
| `bw.csv` | topology — 79 observed links reduced to a **maximum spanning tree** (43 edges), because the MILP's path constraint assumes a unique path between node pairs |
| `synthetic_cpu_var.csv` | per-epoch telemetry — 996,800 rows spanning exactly 300 s, i.e. one epoch, replayed per epoch with fresh lognormal jitter |

Node resource state fed to the solver follows the dump's own formula
(`cpu_available = allocatable − requests − usage`):

```
used_cpu    = telemetry_quantile(cpu_usage)             + running-task CPU
used_memory = capacity − telemetry_quantile(mem_avail)  + running-task memory
```

Notes and current limitations:
- **Tasks are synthetic.** `raw_data/` contains no task-level data (`cpu_var.csv`
  is explicitly background load with no tasks running), so `raw_data_prep.py`
  generates them per the paper's description (Rayleigh CPU/memory, Zipf bandwidth).
- **Two metrics only** (`cpu_cores`, `memory_gb`). `raw_data/` has no per-node
  network metric — `bw.csv` is per-edge — so the server runs against
  `raw-data-config.yaml`, which drops `network_mbps`. The MILP's link-bandwidth
  constraint is unaffected: it uses `bw.csv` edge capacity and synthetic task demand.
- **Timestamps are bucketed into epochs** (`epoch = floor((t − t0) / 300 s)`) rather
  than queried as a time range, keeping the ES filter a `term` match as in the
  other sweeps.
- 3 of the 47 nodes in `cpu_alloc.csv` (`UGS-17/18/19`) have no telemetry and are
  dropped, leaving 44.
- **The trace barely loads the cluster**: the background load leaves ~90% of CPU
  (125.5 of 139.4 cores) and ~99.9% of memory free. Telemetry only enters the MILP
  through that slack, so the workload has to push the cluster near saturation or
  the telemetry source cannot change any decision.

**Workload model.** Tasks arrive on a rolling schedule (`arrival_offset_s`, honored
by the experiment) rather than all at epoch 0, and are sized by *offered load*
rather than a total:

```
arrivals/epoch = --tasks-per-epoch, spread over --arrival-epochs
offered load   = mean_task_cpu * tasks_per_epoch * mean_lifetime_epochs
               = --load-factor * cluster_free_cpu
```

`--load-factor` just under 1 (default 0.95) is the operating point that matters:
the cluster sits near saturation so marginal placements flip, while the pending
queue stays ~60-100 tasks, small enough for the MILP to *prove* optimality inside
`--solver-time-limit-s`. A queue of ~300 does not — SCIP returns `NOT_SOLVED`
(no solution at all) at 60 s, which is why every solve's status is now recorded.
Memory demand is sized separately (`--memory-load-factor`, default 0.6) so CPU
stays the binding resource: the sketch/ES estimates diverge ~40% on CPU but
~0.1% on memory, so a memory-bound workload would show nothing.

Defaults produce 2400 tasks over 40 arrival epochs (60/epoch, mean 0.86 cores,
p95 1.67, mean lifetime 2.25 epochs, realized load 0.92x free CPU).

**Repeats.** `--runs N` re-runs the whole trajectory with a fresh telemetry jitter
draw, a reset ES index and a restarted sketch server, writing a `run` column. The
plot script then shows means with min..max bands and prints a per-metric
mean ± sd table: at n=1 a 3-task gap between backends is indistinguishable from
solver/jitter noise.

```bash
# ES must be running on :9200; the script starts/stops the sketch server itself
python scripts/raw_data_prep.py
python scripts/run_raw_data_assignment.py --runs 5 --epochs 45
python scripts/plot_raw_data_assignment.py
```

Run from an env that has the solver deps (`cd solver_experimental && uv run python
../scripts/run_raw_data_assignment.py ...`).

**What this experiment can and cannot show.** It measures query latency, quantile
accuracy and solver time (the paper's Fig. 4/6/7) on a real trace. It cannot show
a sketch-vs-ES *assignment* difference, and neither can the paper — Fig. 9 reports
Elasticsearch +14.3% vs the sketch layer +15.1% over a static baseline, i.e. a
<1% difference. Every claim about assignment quality in the paper is made against
a **static-telemetry baseline**, which this script does not have. That is
`run_raw_data_completion.py`.

### raw_data completion experiment (paper Fig. 8/9/10)

Tests the other claim: *dynamic* estimates beat *static* ones, and approximate
dynamic estimates capture that gain as well as exact ones. Three pieces the
assignment experiment lacks make it measurable:

1. **A static baseline** — a controller whose estimates never refresh: node
   background frozen at its epoch-0 reading, every running task charged at its
   original *request* instead of its measured usage (~45-50% mean CPU estimate
   error, versus <1% for both sketch and ES).
2. **Per-task telemetry** — the paper's update rule refreshes *running tasks*
   ("a running task's CPU/memory estimate is set to the p50 quantile, unless it
   approaches the current allocation, which triggers a 20% allocation increase"),
   keyed by task id. raw_data has no task-level data, so each running task emits a
   synthetic usage stream that drifts below its request and occasionally bursts
   above it. Served from a second sketch index, `task-metrics` (see
   `raw-data-full-config.yaml`), and an ES index `raw-task-metrics`.
The MILP is bounded by a scheduling window (`--max-candidates`, default 80
oldest pending tasks) and a reassignment-candidate cap
(`--max-reassign-candidates`, default 40, drawn from the most over-committed
nodes and limited to what the cluster's spare capacity can absorb). An
over-subscribed workload grows the queue without limit, and an unbounded model
stops being solvable to proven optimality — at which point scenario gaps become
solver search artefacts. Both caps apply identically to every scenario. A
running task offered for reassignment carries a dominant priority (100) rather
than `must_assign`, because a node whose *estimated* load already exceeds
capacity leaves nowhere to put its own tasks and makes the model INFEASIBLE;
`evicted` should stay at 0 and is the column to check if a scenario looks wrong.

3. **Contention** — "constrained tasks experience a performance penalty,
   increasing their execution duration ... proportionally to their excess resource
   demand." Each epoch a node's *true* load (exact background quantile + true task
   usage) is compared to capacity; tasks on an over-committed node make partial
   progress. Without this, a wrong estimate costs nothing and only MILP
   tie-breaking noise is left to measure.

Scenarios are independent trajectories over identical telemetry —
`name:estimator:rule:gamma:lam`, with `estimator ∈ {static, sketch, es}` and
`rule ∈ {request, p50, p90, avg, p50bump, window}`. Presets mirror the figures:

| `--figure` | Scenarios | Paper's result |
|---|---|---|
| `8` | static, dynamic, reassign, dynamic+reassign | +21.4% / +8.6% / +1.8% |
| `9` | static, sketch, es (all γ=10, λ=1) | ES +14.3%, sketch +15.1% |
| `10` | static + p50 / p90 / avg / p50bump / window-avg | +14.1-20.8%, window-avg +8.5% |

Fig. 8 measures against a static controller with **no** reassignments (the
paper's reference); Fig. 9 and 10 measure against a static controller that
*has* them, isolating the telemetry effect from the reassignment effect. The
run script dedups scenarios shared between figures, so Fig. 9/10's baseline is
stored under the name Fig. 8 gives it (`reassign`) and relabelled in the plot.

`avg` is an **oracle**, not a competitor: work delivered over an epoch is
usage x epoch length, so the contention model charges each task its *mean* usage
— exactly what `avg` (sum/samples) reports. Its estimate error is 0 by
construction and it bounds how much of any gap is estimator error at all.
`window` averages that same statistic over `--window-epochs` past epochs, so it
is stale rather than wrong (the paper's "recent window averaging" baseline).
Observed mean CPU estimate error, 3-epoch smoke: static 50.3%, p90 12.6%,
p50bump 4.9%, window 3.0%, p50 0.86%, avg 0%.

**Workload calibration matters more than anything else here.** A task is
*charged* its request when the controller has no telemetry but actually *uses*
~0.77x of it. If requests alone fit inside the cluster, the static controller
places everything too and no estimator can change the outcome — the experiment
returns a null for a reason unrelated to telemetry. The assignment experiment's
workload (`--load-factor 0.92`, sized on requests) is exactly that case: 83% of
CPU requested, 63% actually used. So this experiment uses its own heavier
workload, and the script prints the diagnostic and warns if it is too loose:

```bash
python scripts/raw_data_prep.py --load-factor 1.4 --memory-load-factor 0.8 \
    --out-dir data/raw_topology_completion      # requests 122% of CPU, true usage 93%
```

`data/raw_topology_completion` is picked up automatically when present.

Task communication is omitted (zero bandwidth demand → the link constraint is
inactive), which both matches what the estimators estimate and keeps the MILP
provably optimal — so a scenario gap is not solver search noise. Control-loop
overhead is excluded by design, as in the paper, so background telemetry is
subsampled (`--rows-per-epoch`, default 100k).

```bash
# ES on :9200; the script starts/stops its own sketch server on 10101
cd solver_experimental
uv run python ../scripts/run_raw_data_completion.py --figure all --runs 3 --epochs 30
uv run python ../scripts/plot_raw_data_completion.py
```

### Benchmark output convention

- **CSV output** defaults to `data/`
- **Plot output** defaults to `plots/`
- **Log output** defaults to `logs/`

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
- Network access on first build (crates.io + github.com) to fetch `asap_sketchlib` and `elasticsearch-dsl-ast`

### Quick start
```bash
# Full pipeline
bash evaluate_demo.sh

# Rust server only (local)
cd single_node_server/network-control-server && cargo run -- --timing

# Rust server via Docker
cd single_node_server/network-control-server && ./docker-build.sh -t network-control-server:latest
docker run -p 10101:10101 network-control-server:latest

# Solver only (assumes server + ES running)
cd solver_experimental && bash run_main.sh

# RTT benchmarks (local server)
bash scripts/run_rtt_sweep_all.sh
# RTT benchmarks (docker server)
bash scripts/run_rtt_sweep_all.sh --docker
bash scripts/run_rtt_sweep_all.sh --docker --docker-image=my-custom:tag
python3 scripts/run_rtt_sweep.py
python3 scripts/run_rtt_sweep_epoch.py
python3 scripts/run_rtt_sweep_epoch_with_solver.py --run-solver
python3 scripts/run_rtt_sweep_epoch_full.py --run-solver
python3 scripts/run_rtt_sweep_epoch_full_ortools.py --run-solver                        # default: CBC
python3 scripts/run_rtt_sweep_epoch_full_ortools.py --run-solver --solver-backend SCIP  # SCIP backend
python3 scripts/run_rtt_sweep_epoch_full_ortools.py --run-solver --solver-backend GLPK  # GLPK backend
python3 scripts/run_es_ingest_query_sweep.py
python3 scripts/run_sketch_ingest_query_sweep.py
python3 scripts/run_dynamic_epoch_benchmark.py --solver-backend SCIP --max-epochs 50 --rows-per-epoch 1000000
bash scripts/run_dynamic_epoch_benchmark_all.sh

# raw_data experiments (ES on :9200; each script runs its own sketch server)
cd solver_experimental
uv run python ../scripts/raw_data_prep.py                                          # topology + workload
uv run python ../scripts/run_raw_data_assignment.py --runs 5 --epochs 45           # latency/accuracy (Fig 4/6/7)
uv run python ../scripts/plot_raw_data_assignment.py
uv run python ../scripts/raw_data_prep.py --load-factor 1.4 --memory-load-factor 0.8 \
    --out-dir data/raw_topology_completion                                         # heavier workload
uv run python ../scripts/run_raw_data_completion.py --figure all --runs 3 --epochs 30   # completions (Fig 8/9/10)
uv run python ../scripts/plot_raw_data_completion.py
cd ..

# Resource benchmark (CPU + whole-process RSS via /proc). ES must already be running.
python3 scripts/proc_monitor.py --selftest                       # verify PID resolution
bash scripts/run_resource_benchmark_all.sh                       # full: 10 runs x 10 epochs x 1M, plots (~4h)
python3 scripts/run_resource_benchmark.py \
  --runs 1 --epochs 2 --rows-per-epoch 30000 --min-repeats 5 --min-measure-seconds 0.5 \
  --warmup 1 --idle-seconds 1                                    # quick smoke
```

### Tests
```bash
# OR-Tools solver tests
cd solver_experimental && uv run pytest python_solver/tests/
```

## Architecture Notes

- The Rust server uses **KLL sketches** (from `asap_sketchlib`) for approximate quantile queries, providing O(1) query time vs ES's full scan
- Two solver implementations exist: **PuLP** (`scheduler/solver.py`) and **OR-Tools** (`python_solver/`). The OR-Tools version is more mature with migration penalties and reassignment limits. The OR-Tools solver supports configurable backends via `solver_backend` parameter: **CBC** (default), **SCIP**, and **GLPK**
- The telemetry emulator (`emulate_telemetry.py`) runs as a FastAPI sidecar, sending identical data to both ES and Sketch server for consistency comparison
- Benchmark scripts measure both **latency** (RTT) and **correctness** (metric value comparison between backends)
- **Resource benchmark methodology** (`run_resource_benchmark.py`): CPU and whole-process RSS are read externally from `/proc` (symmetric for both backends — no ES-internal instrumentation, which would distort latency), so latency + CPU + RSS are captured in the *same* run. CPU uses `utime+stime` deltas at window boundaries (process-wide, all threads); idle baseline is subtracted; client/server/ES are pinned to disjoint cores via `taskset`; ES `request_cache` is disabled and warmups discarded. The experiment mirrors the latency benchmark (N runs x M epochs at 1M rows/epoch); the sketch server is restarted per run (KLL state accumulates across that run's epochs) and ES is reset per run with each epoch's query filtered to that epoch. Note CPU time can exceed query latency: it is summed across all worker threads, so a parallelized query reports more CPU-time than wall-clock latency. Caveat: ES RSS is dominated by its fixed JVM heap (`-Xms=-Xmx≈31GB`), so RSS reflects provisioning, not per-query memory — the headline claim is reduced per-query **compute**, not provisioning (ES is not removed).

## Known Issues

- **Resource benchmark CPU resolution.** `/proc` CPU accounting is in jiffies (`CLK_TCK`, typically 100 → 10ms granularity). Per-query CPU is therefore only meaningful when accumulated over many queries; a single query may register 0 ticks. The benchmark uses ADAPTIVE repeats (`--min-measure-seconds`, default 1.0s; bounded by `--min-repeats`/`--max-repeats`) so the cheap sketch query runs many times and the expensive ES query fewer — both reach `cpu_busy_ticks` well above ~10. Inspect that column if results look quantized.
- **Resource benchmark requires exclusive port 10101.** The benchmark starts/stops its own pinned sketch server; kill any pre-existing server on 10101 first (as `evaluate_demo.sh` does). ES must be running under the same uid so `taskset` can set its affinity without root.
- **Ingested metric usage can exceed node capacity.** Synthetic metrics generated during benchmarks may produce cumulative usage values (CPU, memory) that exceed a node's declared capacity. The PuLP solver handles this gracefully (`max(capacity - used, 0.0)`), but the OR-Tools solver raises a `ValueError` on over-subscribed nodes. The OR-Tools sweep script (`run_rtt_sweep_epoch_full_ortools.py`) works around this by clamping `used_cpu`/`used_memory` to the node's capacity before solving. A proper fix would be to either cap the synthetic metric generation or add clamping inside the OR-Tools solver itself.

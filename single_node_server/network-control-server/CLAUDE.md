# CLAUDE.md — network-control-server (Rust)

## What This Is

An Axum HTTP server that ingests cluster metrics and serves aggregated queries using **KLL quantile sketches** (from `sketchlib-rust`). It acts as a fast alternative to Elasticsearch for percentile and cumulative aggregation queries.

Listens on `0.0.0.0:10101`. Unsupported aggregations are forwarded to an upstream Elasticsearch instance.

## Build & Run

```bash
cargo build
cargo run               # no timing
cargo run -- --timing   # enables per-request CSV timing log
```

Rust edition: 2024. No tests in this crate currently.

## Source Layout

```
src/
├── main.rs              # Entry point: loads configs, builds AppState, starts server
├── config.rs            # AggregationConfig (from YAML) + NodesConfig (from YAML)
├── ingest.rs            # Stub (startup CSV ingestion disabled, data arrives via POST)
├── metrics/
│   ├── mod.rs           # Exports NodeStore, MetricField (5 modules commented out)
│   ├── store.rs         # NodeStore: per-node KLL sketches + cumulative sums
│   └── util.rs          # MetricField enum (CpuCores, MemoryGb, NetworkMbps)
└── server/
    ├── mod.rs           # Exports + TimingSender type alias
    ├── handlers.rs      # Route definitions + all HTTP handlers (622 lines, largest file)
    ├── types.rs         # AppState, SearchRequest, AggregationRequest, Batch*, IngestRecord
    ├── query.rs         # handle_percentiles(), handle_cumulative(), parse_quantile_spec()
    ├── upstream.rs      # forward_to_upstream(), merge_aggregations()
    ├── timing.rs        # QueryTiming struct for per-step latency tracking
    └── logging.rs       # Request/response logging middleware via mpsc channel
```

### Disabled modules (files exist but are commented out in `metrics/mod.rs`)

- `cms_cumulative.rs` — Count-Min Sketch based cumulative tracking
- `hydra_labels.rs` — Label-aware metric handling
- `kll_quantiles.rs` — Standalone KLL quantile module
- `minute_window.rs` — Time-windowed metric aggregation
- `pre_aggregation.rs` — Pre-aggregation with `MetricStore` (605 lines, the old approach)

These represent previous iterations. The active approach uses `store.rs` (simple `NodeStore` with direct KLL sketches per node).

## HTTP API

| Method | Path | Description |
|---|---|---|
| `GET /` | Root | Returns usage examples |
| `POST /` | Ingest | Accepts `IngestRecord` JSON with parallel arrays |
| `GET /healthz` | Health | Returns `"ok"` |
| `POST /cluster-metrics/_search` | Search | ES-compatible aggs API; handles percentiles/cumulative locally, forwards rest upstream |
| `POST /cluster-metrics/_batch` | Batch | Batch queries: multiple keys x fields x agg types in one call |
| `POST /metrics/:field` | Direct | Query specific metric field with quantile specs and node_id |

Body limit: 50 MB.

### Ingest format (`POST /`)

```json
{
  "epoch": 1,                           // optional — triggers store clear on epoch change
  "task": ["T001", "T002"],
  "cluster": ["N001", "N002"],
  "cpu_cores": [2.5, 3.1],
  "memory_gb": [8.0, 16.0],
  "network_mbps": [100.0, 200.0]
}
```

All arrays must have equal length. `cluster` values must match node IDs in `nodes-config.yaml`.

### Search format (`POST /cluster-metrics/_search`)

```json
{
  "aggs": {
    "my_pct": { "percentiles": { "field": "cpu_cores", "percents": [50, 99], "key": "N001" } },
    "my_cum": { "cumulative": { "field": "memory_gb", "key": "N001" } },
    "other_agg": { "avg": { "field": "cpu_cores" } }
  }
}
```

- `percentiles` and `cumulative` are handled locally via KLL sketches
- Any other agg types (e.g. `avg`) are forwarded to upstream ES
- Results are merged into a single response under `aggregations`

### Batch format (`POST /cluster-metrics/_batch`)

```json
{
  "keys": ["N001", "N002"],
  "fields": ["cpu_cores", "memory_gb"],
  "aggs": ["percentiles", "cumulative"],
  "percents": [50.0, 99.0]
}
```

Runs queries concurrently across keys using `tokio::task::spawn_blocking`.

## Configuration Files

### `agg-config.yaml` (env: `AGG_CONFIG_PATH`)

Defines which metric fields support which aggregation types. Used for **query validation only** (not ingestion-time enforcement). Currently all three metrics (cpu_cores, memory_gb, network_mbps) support percentiles, cumulative, and top_entities with cluster/task labels.

### `nodes-config.yaml` (env: `NODES_CONFIG_PATH`)

Defines the set of valid node IDs:
```yaml
nodes:
  count: 30
  range:
    start: N001
    end: N030
```
Node IDs are generated from the range at startup. Ingest requests for unknown node IDs are rejected.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `UPSTREAM_URL` | `http://localhost:9200/cluster-metrics/_search` | ES forwarding endpoint |
| `AGG_CONFIG_PATH` | `agg-config.yaml` | Path to aggregation config |
| `NODES_CONFIG_PATH` | `nodes-config.yaml` | Path to nodes config |
| `SERVER_TIMING_CSV` | `server_request_timing.csv` | Output path for timing CSV (when `--timing` enabled) |

## Key Architecture Details

### Data Model

- `NodeStore` holds a `HashMap<String, NodeData>` keyed by node ID (e.g. "N001")
- Each `NodeData` has 3 KLL sketches (cpu, mem, net) + 3 cumulative f64 sums
- All fields are `RwLock`-wrapped for concurrent access
- KLL sketches provide approximate quantile queries; cumulative values are exact running sums

### Epoch Management

- Ingest payloads may include an `epoch` field
- When epoch changes, the entire `NodeStore` is cleared (all KLL sketches reset, cumulatives zeroed)
- This supports the benchmark workflow where data is ingested in discrete epochs

### Request Flow (search)

1. Parse JSON body into `SearchRequest`
2. Classify each agg as percentiles/cumulative (handled locally) or other (forwarded)
3. Execute local aggs against `NodeStore` sketches
4. If any aggs need ES, strip handled aggs from body and forward remainder to upstream
5. Merge local results + upstream response into single `aggregations` object
6. If `--timing`, attach `_timing` field and `X-Server-Timing` header

### Timing

When `--timing` is enabled:
- Each response includes `_timing` JSON with per-step breakdowns (parse_json, sketch_estimate, upstream, merge, serialize)
- `X-Server-Timing` header with total ms
- `x-request-id` and `x-request-type` headers from clients are logged
- Rows written to CSV via a dedicated writer thread (mpsc channel)

### External Dependency

`sketchlib-rust` is a **local path dependency** at `/users/yuanyc/sketchlib-rust`. It provides:
- `KLL` — KLL quantile sketch (approximate quantiles in bounded memory)
- `SketchInput` — Input wrapper for sketch updates

# CLAUDE.md — network-control-server

## What This Is

An Axum HTTP server for ingesting per-node metrics and serving keyed percentile/sum queries from an in-memory sketch-backed store. The `_search` endpoint parses incoming bodies as standard Elasticsearch DSL via the `elasticsearch-dsl-ast` crate.

The deploy contract is now driven by `server-config.yaml`.

## Current Contract

- Primary endpoints:
  - `POST /:index/_search`
  - `POST /:index/_batch`
- Compatibility endpoint:
  - `POST /metrics/:field`
  - `POST /:index/metrics/:field`
- Local aggregations:
  - `percentiles`
  - `sum`
- Local query subset:
  - `size: 0`
  - `query.bool.filter.term` on configured key fields and `epoch`

Unsupported features are either forwarded to upstream Elasticsearch when fallback is enabled, or rejected with a structured `400`.

## Key Modules

- `src/config.rs`
  - authoritative runtime config loader and validator
- `src/metrics/store.rs`
  - `MetricStore`, `KeyCatalog`, `RangeKeyCatalog`, `InMemoryNodeStore`
- `src/server/planner.rs`
  - request planning for local vs fallback execution
- `src/server/query.rs`
  - local aggregation engine registry and execution
- `src/server/upstream.rs`
  - upstream fallback client
- `src/server/handlers.rs`
  - HTTP handlers wired to the planner/engine/store abstractions

## Runtime Notes

- `--config <path>` selects the config file.
- Env overrides:
  - `NCS_CONFIG_PATH`
  - `NCS_SERVER_HOST`
  - `NCS_SERVER_PORT`
  - `NCS_UPSTREAM_SEARCH_URL`
  - `NCS_UPSTREAM_SEARCH_URL_TEMPLATE`
  - `NCS_TIMING_ENABLED`
  - `NCS_TIMING_CSV_PATH`
- `--timing` still forces timing on.

## Verification Note

`cargo fmt` was run locally.

`cargo check` could not be completed in this environment because crates.io access is blocked and dependency resolution for Rust crates fails without network access.

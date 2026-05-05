# CLAUDE.md — sketch-experiments

## Maintenance Rule

**Keep this file up to date.** When adding new experiments, dependencies, or changing the project structure, update this document.

## Overview

A standalone Rust sandbox for testing random ideas that could potentially be used by `network-control-server`. Each experiment is an independent binary — no library crate, no shared state between experiments.

**Key constraint:** This project must remain fully independent from `network-control-server`. They share the `sketchlib-rust` dependency but have no code-level coupling. Changes here must never affect `network-control-server`.

## Project Structure

```
sketch-experiments/
├── Cargo.toml
├── CLAUDE.md
└── src/
    ├── tumbling_window.rs   # Bin: tumbling-window
    └── raw_vs_kll.rs        # Bin: raw-vs-kll
```

## Dependencies

| Crate | Purpose |
|---|---|
| `sketchlib-rust` (local: `/users/yuanyc/sketchlib-rust`) | KLL sketches and other sketch data structures |
| `rand` | Random number generation for simulations |

## Adding a New Experiment

1. Create a new `.rs` file in `src/` (e.g. `src/my_idea.rs`)
2. Add a `[[bin]]` entry in `Cargo.toml`:
   ```toml
   [[bin]]
   name = "my-idea"
   path = "src/my_idea.rs"
   ```
3. Each experiment must have its own `fn main()` — they are standalone binaries
4. Add a new entry to the Experiments table below
5. Add any new dependencies to Cargo.toml if needed

## Build & Run

```bash
cd single_node_server/sketch-experiments

# Build all experiments
cargo build

# Run a specific experiment
cargo run --bin tumbling-window
```

## Experiments

### `tumbling-window`

**File:** `src/tumbling_window.rs`

**Idea:** Use a large tumbling window (e.g. 100 min) with a KLL sketch on timestamps to support time-range queries like "last 5 minutes" without requiring ordered timestamp arrival.

**How it works:**
- A KLL sketch tracks the distribution of ingested timestamps
- A second KLL sketch tracks the distribution of metric values
- On query ("last N minutes"), the timestamp KLL estimates the rank cutoff, then entries are filtered and a fresh value KLL computes the requested percentile
- Timestamps can arrive completely out of order

**Key types:**
- `KllTumblingWindow` — the window struct holding both KLL sketches and raw entries
- `QueryResult` — output containing the value percentile, estimated vs exact point counts, and cutoff rank

### `raw-vs-kll`

**File:** `src/raw_vs_kll.rs`

**Idea:** Benchmark four approaches to storing out-of-order timestamped metric values along three axes (memory, insert speed, query speed):

1. `RawVec` — append-only `Vec<TimestampedValue>`, scans on query.
2. `SortedBTree` — `BTreeMap<u64, Vec<f64>>` keyed by timestamp; sorted at insert time, range queries via `range()`, O(log n) min/max via `first_key_value`/`last_key_value`.
3. `KllOnly` — KLL sketch on timestamps + KLL sketch on values. Cheapest in memory, but cannot answer per-time-range percentiles — returns the global value percentile plus an estimated fraction-in-range.
4. `BucketedKll` — window split into N fixed-width time buckets, one value KLL per bucket. On query, `KLL.merge()` the relevant buckets and read the percentile. This is the form where KLL actually competes on the same query the raw approaches answer exactly. Time resolution = bucket width (boundary buckets are merged whole).

**What it reports:** insert throughput (ns/op + ops/s), approximate memory footprint, `earliest()/latest()` latency, and "last N minutes percentile" query latency, plus a spot check showing accuracy of each approximate method against the exact answer.

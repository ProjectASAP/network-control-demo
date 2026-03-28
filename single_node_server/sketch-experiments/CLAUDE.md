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
    └── tumbling_window.rs   # Bin: tumbling-window
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

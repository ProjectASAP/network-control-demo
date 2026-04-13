# visualization/

Live CLI dashboard for the ingest → query → solve loop against the Sketch
server. Self-contained: nothing here modifies the existing codebase. Logic
that's reused is vendored (copied) into `_vendored/`.

## What It Shows

A `rich.Live` dashboard that updates in real time with:
- current epoch + phase (ingest / query / solve)
- live phase bars and timings for the epoch in progress
- sparkline history of per-phase timings across epochs
- per-epoch results table (rows, ms per phase, solver assignments, objective)
- scrolling event log

Sketch server only. ES and the telemetry emulator are intentionally out of
scope for now — hooks can be added later without touching existing files.

## Requirements

- The Sketch server must already be running on `--server-url`
  (default `http://localhost:10101`). Start it however you usually do, e.g.:
  ```
  cd single_node_server/network-control-server
  ./docker-build.sh -t network-control-server:latest
  docker run --rm -p 10101:10101 network-control-server:latest
  ```
- Python deps: `rich`, `requests`, `pyyaml`, `jsonlines`, `cattrs`,
  `networkx`, `pandas`, `ortools` (all already present in
  `solver_experimental/.venv`). Easiest: run with that venv's python:
  ```
  /users/yuanyc/network-control-demo/solver_experimental/.venv/bin/python \
      visualization/demo.py --epochs 5
  ```

## Usage

```
python visualization/demo.py \
    --epochs 5 \
    --rows-per-epoch 200000 \
    --batch-size 1000 \
    --backend SCIP \
    --solver-node-count 30 \
    --query-node-count 30
```

Flags mirror `scripts/run_rtt_sweep_epoch_full_ortools.py`. Use
`--no-dashboard` for plain log output (CI / non-tty).

## Layout

```
visualization/
├── demo.py              # entry point: rich.Live dashboard driving the loop
├── README.md
└── _vendored/           # verbatim copies of the logic we reuse
    ├── rtt_sweep_common.py   # copied from scripts/
    ├── ort_solver.py         # copied from python_solver/src/network_controller/solver.py
    ├── load_info.py          # copied from solver_experimental/scheduler/
    └── entities.py           # copied from solver_experimental/scheduler/
```

Refresh vendored files manually when upstream changes.

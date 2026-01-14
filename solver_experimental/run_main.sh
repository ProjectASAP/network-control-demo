#!/bin/bash

set -euo pipefail

uv run emulate_telemetry.py &
EMULATOR_PID=$!
trap 'kill "$EMULATOR_PID"' EXIT

uv run main.py \
    --node-path "dummy_data/nodes.csv" \
    --edge-path "dummy_data/edges.csv" \
    --task-path "dummy_data/tasks.csv" \
    --interval 1.0 \
    --query-manager-config "configs/sample.yml" \
    --log-level "INFO"

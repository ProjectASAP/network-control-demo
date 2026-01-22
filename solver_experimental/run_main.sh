#!/bin/bash

set -euo pipefail

uv run emulate_telemetry.py &
EMULATOR_PID=$!
trap 'kill "$EMULATOR_PID"' EXIT

uv run main.py \
    --node-path "dummy_data/nodes.jsonl" \
    --edge-path "dummy_data/edges.jsonl" \
    --task-path "dummy_data/tasks.jsonl" \
    --interval 1.0 \
    --query-manager-config "configs/sample.yml" \
    --log-level "INFO"

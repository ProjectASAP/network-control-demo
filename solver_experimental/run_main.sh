#!/bin/bash

set -euo pipefail

INTERVAL=5.0
EPOCH_LENGTH_S=300.0

uv run emulate_telemetry.py --epoch-length-s "${EPOCH_LENGTH_S}" --interval "${INTERVAL}" --log-level "DEBUG" &
EMULATOR_PID=$!
trap 'kill "$EMULATOR_PID"' EXIT

CLUSTER_METRICS_CSV="${CLUSTER_METRICS_CSV:-$HOME/cluster-metrics.csv}" \
TIME_RANGE_MS="${TIME_RANGE_MS:-3000000}" \
NODE_QUERY_LIMIT="${NODE_QUERY_LIMIT:-}" \
uv run main.py \
    --node-path "dummy_data/nodes.jsonl" \
    --edge-path "dummy_data/edges.jsonl" \
    --task-path "dummy_data/tasks.jsonl" \
    --emulator-url "http://localhost:8000" \
    --interval "${INTERVAL}" \
    --epoch-length-s "${EPOCH_LENGTH_S}" \
    --query-manager-config "configs/sample.yml" \
    --log-level "INFO"

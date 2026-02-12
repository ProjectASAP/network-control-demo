#!/bin/bash

set -euo pipefail

EPOCH_LENGTH_S=150.0

LOG_DIR="logs"
mkdir -p "${LOG_DIR}"

uv run emulate_telemetry.py \
    --epoch-length-s "${EPOCH_LENGTH_S}" \
    --log-level "DEBUG" \
    --data-rate 200 \
    --sketch-ingest-log-path "${LOG_DIR}/sketch_ingest.csv" \
    --es-ingest-log-path "${LOG_DIR}/es_ingest.csv" \
    --no-es-ingest \
    &

EMULATOR_PID=$!
trap 'kill "$EMULATOR_PID"' EXIT

INTERVAL=1.0
uv run main.py \
    --node-path "dummy_data/nodes.jsonl" \
    --edge-path "dummy_data/edges.jsonl" \
    --task-path "dummy_data/tasks.jsonl" \
    --emulator-url "http://localhost:8000" \
    --interval "${INTERVAL}" \
    --epoch-length-s "${EPOCH_LENGTH_S}" \
    --query-manager-config "configs/sample.yml" \
    --log-level "INFO" \
    --query-rtt-log-path "${LOG_DIR}/query_rtt.csv" \
    --loop-rtt-log-path "${LOG_DIR}/loop_rtt.csv" 
    # --use-es 
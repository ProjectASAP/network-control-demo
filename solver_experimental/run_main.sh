#!/bin/bash

set -euo pipefail

EPOCH_LENGTH_S=150.0

LOG_DIR="logs/$(date +%Y%m%d-%H%M%S)"
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

sleep 3

INTERVAL=1.0
BATCH_SIZE=60
uv run main.py \
    --node-path "dummy_data/nodes.jsonl" \
    --edge-path "dummy_data/edges.jsonl" \
    --task-path "dummy_data/tasks.jsonl" \
    --emulator-url "http://localhost:8000" \
    --interval "${INTERVAL}" \
    --batch-size "${BATCH_SIZE}" \
    --epoch-length-s "${EPOCH_LENGTH_S}" \
    --log-level "INFO" \
    --query-rtt-log-path "${LOG_DIR}/query_rtt.csv" \
    --loop-rtt-log-path "${LOG_DIR}/loop_rtt.csv" \
    --assignments-log-path "${LOG_DIR}/assignments.csv" 
    # --use-es 
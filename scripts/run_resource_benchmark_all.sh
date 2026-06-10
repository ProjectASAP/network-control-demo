#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-${ROOT_DIR}/solver_experimental/.venv/bin/python}"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="python3"
fi

# The sketch server is built/started via `cargo run`; non-interactive shells
# skip .bashrc, so make the shared target dir explicit (see project memory).
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/mnt/yuanyc/cargo-target}"

mkdir -p "${ROOT_DIR}/data" "${ROOT_DIR}/plots" "${ROOT_DIR}/logs"

# Assumes Elasticsearch is already running (same uid as this script, so the
# benchmark can taskset-pin it). The Python benchmark starts/stops the sketch
# server itself; no emulator is needed (padding-only workload).
#
# Optional environment overrides:
#   RUNS=10  EPOCHS=10  WARMUP_EPOCHS=0  ROWS_PER_EPOCH=1000000
#   MIN_REPEATS=10  MAX_REPEATS=1000  MIN_MEASURE_SECONDS=1.0
#   WARMUP=3  IDLE_SECONDS=3
#   SHAPES="default"
#   SERVER_CORES="0-9"  ES_CORES="10-29"  CLIENT_CORES="30-39"
#   BATCH_SIZE=1000

RUNS="${RUNS:-10}"
EPOCHS="${EPOCHS:-10}"
WARMUP_EPOCHS="${WARMUP_EPOCHS:-0}"
ROWS_PER_EPOCH="${ROWS_PER_EPOCH:-1000000}"
MIN_REPEATS="${MIN_REPEATS:-10}"
MAX_REPEATS="${MAX_REPEATS:-1000}"
MIN_MEASURE_SECONDS="${MIN_MEASURE_SECONDS:-1.0}"
WARMUP="${WARMUP:-3}"
IDLE_SECONDS="${IDLE_SECONDS:-3}"
SHAPES="${SHAPES:-default}"
SERVER_CORES="${SERVER_CORES:-0-9}"
ES_CORES="${ES_CORES:-10-29}"
CLIENT_CORES="${CLIENT_CORES:-30-39}"
BATCH_SIZE="${BATCH_SIZE:-1000}"

echo "=== proc_monitor selftest ==="
"${PYTHON_BIN}" "${SCRIPT_DIR}/proc_monitor.py" --selftest

echo ""
echo "========================================================"
echo "  Resource benchmark: runs=${RUNS} epochs=${EPOCHS} rows/epoch=${ROWS_PER_EPOCH}"
echo "========================================================"
"${PYTHON_BIN}" "${SCRIPT_DIR}/run_resource_benchmark.py" \
  --runs "${RUNS}" \
  --epochs "${EPOCHS}" \
  --warmup-epochs "${WARMUP_EPOCHS}" \
  --rows-per-epoch "${ROWS_PER_EPOCH}" \
  --min-repeats "${MIN_REPEATS}" \
  --max-repeats "${MAX_REPEATS}" \
  --min-measure-seconds "${MIN_MEASURE_SECONDS}" \
  --warmup "${WARMUP}" \
  --idle-seconds "${IDLE_SECONDS}" \
  --shapes ${SHAPES} \
  --server-cores "${SERVER_CORES}" \
  --es-cores "${ES_CORES}" \
  --client-cores "${CLIENT_CORES}" \
  --batch-size "${BATCH_SIZE}" \
  --solver-data-dir "${ROOT_DIR}/solver_experimental/dummy_data" \
  --out-csv "${ROOT_DIR}/data/resource_benchmark.csv" \
  --ingestion-csv "${ROOT_DIR}/data/resource_ingestion.csv" \
  --raw-dir "${ROOT_DIR}/data/resource_benchmark_raw" \
  --server-log "${ROOT_DIR}/logs/server_resource_benchmark.log" \
  --truncate-csv \
  --truncate-server-log \
  2>&1 | tee -a "${ROOT_DIR}/logs/run_resource_benchmark.log"

echo ""
echo "=== Plotting ==="
for SHAPE in ${SHAPES}; do
  "${PYTHON_BIN}" "${SCRIPT_DIR}/plot_resource_benchmark.py" \
    --summary-csv "${ROOT_DIR}/data/resource_benchmark.csv" \
    --ingestion-csv "${ROOT_DIR}/data/resource_ingestion.csv" \
    --out-dir "${ROOT_DIR}/plots/resource" \
    --shape "${SHAPE}"
done

echo "Done. CSVs in data/, plots in plots/resource/, raw JSON in data/resource_benchmark_raw/."

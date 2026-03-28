#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-${ROOT_DIR}/.venv/bin/python}"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="python3"
fi

mkdir -p "${ROOT_DIR}/data" "${ROOT_DIR}/plots" "${ROOT_DIR}/logs"

# This script assumes Elasticsearch is already running and reachable.
# The Python benchmark will start/stop:
#   - sketch server
#   - emulator (unless --skip-emulator-start is passed)
#
# Optional environment overrides:
#   MAX_EPOCHS=50
#   ROWS_PER_EPOCH=1000000
#   BATCH_SIZE=1000
#   BACKENDS="CBC SCIP"

MAX_EPOCHS="${MAX_EPOCHS:-50}"
ROWS_PER_EPOCH="${ROWS_PER_EPOCH:-1000000}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
BACKENDS="${BACKENDS:-CBC SCIP}"

for BACKEND in ${BACKENDS}; do
  BACKEND_LOWER="$(echo "${BACKEND}" | tr '[:upper:]' '[:lower:]')"
  echo ""
  echo "========================================================"
  echo "  Running dynamic epoch benchmark with ${BACKEND} backend"
  echo "========================================================"
  "${PYTHON_BIN}" "${SCRIPT_DIR}/run_dynamic_epoch_benchmark.py" \
    --solver-backend "${BACKEND}" \
    --solver-data-dir "${ROOT_DIR}/solver_experimental/dummy_data" \
    --max-epochs "${MAX_EPOCHS}" \
    --rows-per-epoch "${ROWS_PER_EPOCH}" \
    --batch-size "${BATCH_SIZE}" \
    --out-csv "${ROOT_DIR}/data/dynamic_epoch_benchmark_${BACKEND_LOWER}.csv" \
    --out-plot "${ROOT_DIR}/plots/dynamic_epoch_benchmark_${BACKEND_LOWER}.png" \
    --server-log "${ROOT_DIR}/logs/server_dynamic_epoch_${BACKEND_LOWER}.log" \
    --emulator-log "${ROOT_DIR}/logs/emulator_dynamic_epoch_${BACKEND_LOWER}.log" \
    --truncate-csv \
    --truncate-server-log \
    2>&1 | tee -a "${ROOT_DIR}/logs/run_dynamic_epoch_benchmark.log"
done


#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-${ROOT_DIR}/.venv/bin/python}"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="python3"
fi

mkdir -p "${ROOT_DIR}/data" "${ROOT_DIR}/plots" "${ROOT_DIR}/logs"

"${PYTHON_BIN}" "${SCRIPT_DIR}/run_rtt_sweep.py" \
  --out-csv "${ROOT_DIR}/data/rtt_results.csv" \
  --out-plot "${ROOT_DIR}/plots/query_rtt_plot.png" \
  --server-log "${ROOT_DIR}/logs/server.log" \
  --truncate-csv \
  --truncate-server-log \
  --ingest-timeout 180 \
  2>&1 | tee "${ROOT_DIR}/logs/run_rtt_sweep.log"

"${PYTHON_BIN}" "${SCRIPT_DIR}/run_rtt_sweep_epoch.py" \
  --start-epoch 1 \
  --end-epoch 10 \
  --rows-per-epoch 1000000 \
  --batch-size 1000 \
  --out-csv "${ROOT_DIR}/data/rtt_results_epoch.csv" \
  --out-plot "${ROOT_DIR}/plots/query_rtt_plot_epoch.png" \
  --server-log "${ROOT_DIR}/logs/server_epoch.log" \
  --truncate-csv \
  --truncate-server-log \
  2>&1 | tee -a "${ROOT_DIR}/logs/run_rtt_sweep.log"

"${PYTHON_BIN}" "${SCRIPT_DIR}/run_rtt_sweep_epoch_with_solver.py" \
  --run-solver \
  --solver-data-dir "${ROOT_DIR}/solver_experimental/dummy_data" \
  --out-csv "${ROOT_DIR}/data/rtt_results_epoch_with_solver.csv" \
  --out-plot "${ROOT_DIR}/plots/query_rtt_plot_epoch_with_solver.png" \
  --server-log "${ROOT_DIR}/logs/server_epoch.log" \
  --truncate-csv \
  --truncate-server-log \
  2>&1 | tee -a "${ROOT_DIR}/logs/run_rtt_sweep.log"

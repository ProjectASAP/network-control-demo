#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-${ROOT_DIR}/solver_experimental/.venv/bin/python}"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="python3"
fi

mkdir -p "${ROOT_DIR}/data" "${ROOT_DIR}/plots" "${ROOT_DIR}/logs"

# "${PYTHON_BIN}" "${SCRIPT_DIR}/run_rtt_sweep.py" \
#   --out-csv "${ROOT_DIR}/data/rtt_results.csv" \
#   --out-plot "${ROOT_DIR}/plots/query_rtt_plot.png" \
#   --server-log "${ROOT_DIR}/logs/server.log" \
#   --truncate-csv \
#   --truncate-server-log \
#   --ingest-timeout 180 \
#   2>&1 | tee "${ROOT_DIR}/logs/run_rtt_sweep.log"

# "${PYTHON_BIN}" "${SCRIPT_DIR}/run_rtt_sweep_epoch.py" \
#   --start-epoch 1 \
#   --end-epoch 10 \
#   --rows-per-epoch 1000000 \
#   --batch-size 1000 \
#   --out-csv "${ROOT_DIR}/data/rtt_results_epoch.csv" \
#   --out-plot "${ROOT_DIR}/plots/query_rtt_plot_epoch.png" \
#   --server-log "${ROOT_DIR}/logs/server_epoch.log" \
#   --truncate-csv \
#   --truncate-server-log \
#   2>&1 | tee -a "${ROOT_DIR}/logs/run_rtt_sweep.log"

# run_rtt_sweep_epoch_with_solver.py is superseded by run_rtt_sweep_epoch_full.py
# (same query+solver timing but without ingest timing)
# "${PYTHON_BIN}" "${SCRIPT_DIR}/run_rtt_sweep_epoch_with_solver.py" \
#   --run-solver \
#   --solver-data-dir "${ROOT_DIR}/solver_experimental/dummy_data" \
#   --out-csv "${ROOT_DIR}/data/rtt_results_epoch_with_solver.csv" \
#   --out-plot "${ROOT_DIR}/plots/query_rtt_plot_epoch_with_solver.png" \
#   --server-log "${ROOT_DIR}/logs/server_epoch.log" \
#   --truncate-csv \
#   --truncate-server-log \
#   2>&1 | tee -a "${ROOT_DIR}/logs/run_rtt_sweep.log"

# # Full sweep: 30-node topology (matches metrics server node range)
# "${PYTHON_BIN}" "${SCRIPT_DIR}/run_rtt_sweep_epoch_full.py" \
#   --run-solver \
#   --solver-data-dir "${ROOT_DIR}/solver_experimental/dummy_data" \
#   --start-epoch 1 \
#   --end-epoch 10 \
#   --rows-per-epoch 1000000 \
#   --batch-size 1000 \
#   --solver-node-count 30 \
#   --query-node-count 30 \
#   --out-csv "${ROOT_DIR}/data/rtt_results_epoch_full_30nodes.csv" \
#   --out-plot "${ROOT_DIR}/plots/rtt_epoch_full_30nodes.png" \
#   --server-log "${ROOT_DIR}/logs/server_epoch_full_30nodes.log" \
#   --truncate-csv \
#   --truncate-server-log \
#   2>&1 | tee -a "${ROOT_DIR}/logs/run_rtt_sweep.log"

# # Full sweep: all nodes topology (300 solver nodes, all query nodes)
# "${PYTHON_BIN}" "${SCRIPT_DIR}/run_rtt_sweep_epoch_full.py" \
#   --run-solver \
#   --solver-data-dir "${ROOT_DIR}/solver_experimental/dummy_data" \
#   --start-epoch 1 \
#   --end-epoch 10 \
#   --rows-per-epoch 1000000 \
#   --batch-size 1000 \
#   --out-csv "${ROOT_DIR}/data/rtt_results_epoch_full_allnodes.csv" \
#   --out-plot "${ROOT_DIR}/plots/rtt_epoch_full_allnodes.png" \
#   --server-log "${ROOT_DIR}/logs/server_epoch_full_allnodes.log" \
#   --truncate-csv \
#   --truncate-server-log \
#   2>&1 | tee -a "${ROOT_DIR}/logs/run_rtt_sweep.log"

# Full sweep: 30-node topology (OR-Tools with CBC, SCIP, GLPK backends)
for BACKEND in CBC SCIP; do
  BACKEND_LOWER="$(echo "${BACKEND}" | tr '[:upper:]' '[:lower:]')"
  echo ""
  echo "=========================================="
  echo "  Running OR-Tools with ${BACKEND} backend"
  echo "=========================================="
  "${PYTHON_BIN}" "${SCRIPT_DIR}/run_rtt_sweep_epoch_full_ortools.py" \
    --run-solver \
    --solver-backend "${BACKEND}" \
    --solver-data-dir "${ROOT_DIR}/solver_experimental/dummy_data" \
    --start-epoch 1 \
    --end-epoch 10 \
    --rows-per-epoch 1000000 \
    --batch-size 1000 \
    --solver-node-count 30 \
    --query-node-count 30 \
    --out-csv "${ROOT_DIR}/data/rtt_results_epoch_full_ortools_${BACKEND_LOWER}_30nodes.csv" \
    --out-plot "${ROOT_DIR}/plots/rtt_epoch_full_ortools_${BACKEND_LOWER}_30nodes.png" \
    --server-log "${ROOT_DIR}/logs/server_epoch_full_ortools_${BACKEND_LOWER}_30nodes.log" \
    --truncate-csv \
    --truncate-server-log \
    2>&1 | tee -a "${ROOT_DIR}/logs/run_rtt_sweep.log"
done

# # Full sweep: all nodes (OR-Tools)
# "${PYTHON_BIN}" "${SCRIPT_DIR}/run_rtt_sweep_epoch_full_ortools.py" \
#   --run-solver \
#   --solver-data-dir "${ROOT_DIR}/solver_experimental/dummy_data" \
#   --start-epoch 1 \
#   --end-epoch 10 \
#   --rows-per-epoch 1000000 \
#   --batch-size 1000 \
#   --out-csv "${ROOT_DIR}/data/rtt_results_epoch_full_ortools_allnodes.csv" \
#   --out-plot "${ROOT_DIR}/plots/rtt_epoch_full_ortools_allnodes.png" \
#   --server-log "${ROOT_DIR}/logs/server_epoch_full_ortools_allnodes.log" \
#   --truncate-csv \
#   --truncate-server-log \
#   2>&1 | tee -a "${ROOT_DIR}/logs/run_rtt_sweep.log"

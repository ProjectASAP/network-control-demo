#!/usr/bin/env bash
set -euo pipefail

python3 run_rtt_sweep.py \
  --out-csv rtt_results.csv \
  --server-log logs/server.log \
  --truncate-csv \
  --truncate-server-log \
  --ingest-timeout 180 \
  > run_rtt_sweep.log 2>&1

python3 run_rtt_sweep_epoch.py \
  --start-epoch 1 \
  --end-epoch 10 \
  --rows-per-epoch 1000000 \
  --batch-size 1000 \
  --out-csv rtt_results_epoch.csv \
  --server-log logs/server_epoch.log \
  --truncate-csv \
  --truncate-server-log \
  2>&1 | tee run_rtt_sweep.log

python3 run_rtt_sweep_epoch_with_solver.py \
  --run-solver \
  --solver-data-dir solver_experimental/dummy_data \
  --out-csv rtt_results_epoch_with_solver.csv \
  --server-log logs/server_epoch.log \
  --truncate-csv \
  --truncate-server-log \
  2>&1 | tee run_rtt_sweep.log
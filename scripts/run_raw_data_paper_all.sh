#!/usr/bin/env bash
# Full re-run of the raw_data completion experiment, split the way the paper is:
#
#   Fig. 8 + Fig. 10   150 epochs x 1 run   (no Elasticsearch scenario, so no ES
#                                            ingest -- this run is sketch-only)
#   Fig. 9              21 epochs x 10 runs  (static vs sketch vs Elasticsearch)
#
# Epochs are 150 s, matching the paper. Background telemetry is ~1M rows per
# epoch, each epoch replaying a *different* 150 s slice of the real cpu_var.csv
# trace (scripts/raw_data_epoch_slices.py), so the per-node background actually
# moves between epochs instead of being one window replayed.
#
# Requires Elasticsearch on :9200. Each python run starts and stops its own
# sketch server on :10101.
set -uo pipefail

REPO=/users/yuanyc/network-control-demo
UV=/users/yuanyc/.local/bin/uv
cd "$REPO/solver_experimental" || exit 1

LOG_DIR="$REPO/logs"
mkdir -p "$LOG_DIR"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)

say() { printf '\n===== %s  [%s] =====\n' "$1" "$(date -u +%H:%M:%SZ)"; }

# Fig. 8's four arms plus Fig. 10's rule family. The paper's Fig. 10 plots
# no-rule / p50 / p50+1.2x / avg(p50,p75)+1.2x / avg(=window); p90 and the
# per-epoch mean are ours, carried as extras and labelled as such.
FIG810_SCENARIOS=(
  --scenario "static:static:request:0:0"
  --scenario "reassign:static:request:10:1"
  --scenario "dynamic:sketch:p50:0:0"
  --scenario "dynamic+reassign:sketch:p50:10:1"
  --scenario "p50-1.2xalloc:sketch:p50bump:10:1"
  --scenario "avg-p50p75-1.2xalloc:sketch:avgp50p75bump:10:1"
  --scenario "window-avg:sketch:window:10:1"
  --scenario "p90:sketch:p90:10:1"
  --scenario "avg-epoch:sketch:avg:10:1"
)

say "STEP 1/4  Fig 8 + Fig 10: 150 epochs x 1 run, 9 scenarios"
$UV run python ../scripts/run_raw_data_completion.py \
  "${FIG810_SCENARIOS[@]}" \
  --runs 1 --epochs 150 --epoch-length-s 150 \
  --rows-per-epoch 1000000 --task-samples 200 \
  --solver-time-limit-s 30 \
  --out-csv "data/raw_data_completion_fig810.csv" \
  --server-log "logs/server_fig810.log" \
  2>&1 | tee "$LOG_DIR/paper_fig810_$STAMP.log"
RC810=$?

say "STEP 2/4  Fig 9: 21 epochs x 10 runs, static vs sketch vs Elasticsearch"
$UV run python ../scripts/run_raw_data_completion.py \
  --figure 9 --gamma 10 --lam 1 \
  --runs 10 --epochs 21 --epoch-length-s 150 \
  --rows-per-epoch 1000000 --task-samples 200 \
  --solver-time-limit-s 30 \
  --out-csv "data/raw_data_completion_fig9.csv" \
  --server-log "logs/server_fig9.log" \
  2>&1 | tee "$LOG_DIR/paper_fig9_$STAMP.log"
RC9=$?

say "STEP 3/4  quantile accuracy vs ground truth (paper Fig 6)"
if [ -f ../scripts/run_raw_data_accuracy.py ]; then
  $UV run python ../scripts/run_raw_data_accuracy.py \
    2>&1 | tee "$LOG_DIR/paper_fig6_$STAMP.log"
else
  echo "scripts/run_raw_data_accuracy.py not present yet -- skipped"
fi

say "STEP 4/4  plots"
$UV run python ../scripts/plot_raw_data_completion.py \
  --csv data/raw_data_completion_fig810.csv \
  --summary-csv data/raw_data_completion_fig810_summary.csv \
  2>&1 | tee -a "$LOG_DIR/paper_plots_$STAMP.log"
$UV run python ../scripts/plot_raw_data_completion.py \
  --csv data/raw_data_completion_fig9.csv \
  --summary-csv data/raw_data_completion_fig9_summary.csv \
  2>&1 | tee -a "$LOG_DIR/paper_plots_$STAMP.log"

say "DONE  fig810 rc=$RC810  fig9 rc=$RC9"
echo "CSVs:  data/raw_data_completion_fig810.csv  data/raw_data_completion_fig9.csv"
echo "Logs:  $LOG_DIR/paper_*_$STAMP.log"

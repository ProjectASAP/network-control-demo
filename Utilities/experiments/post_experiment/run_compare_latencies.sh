#!/usr/bin/env bash

if [ -z "$1" ]; then
  echo "Usage: $0 <experiment_name> [--per_query]"
  exit 1
fi

EXP_NAME=$1
PER_QUERY_FLAG=$2

python3 compare_latencies.py --experiment_name $EXP_NAME --exact_experiment_mode sketchdb --exact_experiment_server_name prometheus --estimate_experiment_mode sketchdb ${PER_QUERY_FLAG}

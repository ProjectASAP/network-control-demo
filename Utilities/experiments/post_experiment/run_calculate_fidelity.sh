#!/usr/bin/env bash

if [ -z "$1" ]; then
  echo "Usage: $0 <experiment_name>"
  exit 1
fi

EXP_NAME=$1

python3 calculate_fidelity.py --experiment_name $EXP_NAME --exact_experiment_mode sketchdb --exact_experiment_server_name prometheus --estimate_experiment_mode sketchdb

#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <num_nodes> <cloudlab_username> <hostname_suffix>"
    exit 1
fi

NUM_NODES=$1
USERNAME=$2
HOSTNAME_SUFFIX=$3

THIS_DIR=$(dirname "$(readlink -f "$0")")

CMD="cd /scratch/sketch_db_for_prometheus/experiment_outputs; "
CMD=$CMD"rm -rf *"

echo "Running command: $CMD"

ssh -o StrictHostKeyChecking=no $USERNAME@"node0."$HOSTNAME_SUFFIX "$CMD" < /dev/null &

wait

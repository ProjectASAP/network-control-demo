#!/usr/bin/env bash

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <num_nodes> <cloudlab_username> <hostname_suffix>"
    exit 1
fi

NUM_NODES=$1
USERNAME=$2
HOSTNAME_SUFFIX=$3

THIS_DIR=$(dirname "$(readlink -f "$0")")

OPTIONS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
CD_CMD="cd /scratch/sketch_db_for_prometheus/code/Utilities/installation; "
EXTERNAL_CMD="./install_external_components.sh /scratch/sketch_db_for_prometheus/ all; "
INTERNAL_CMD="./setup_internal_components.sh all;"

echo "Running command: $EXTERNAL_CMD"
for i in $(seq 0 $(($NUM_NODES-1))); do
    ssh $OPTIONS $USERNAME@node"$i".$HOSTNAME_SUFFIX "$CD_CMD $EXTERNAL_CMD" &
done
wait

echo "External components installed on all nodes."

echo "Running command: $INTERNAL_CMD"
for i in $(seq 0 $(($NUM_NODES-1))); do
    ssh $OPTIONS $USERNAME@node"$i".$HOSTNAME_SUFFIX "$CD_CMD $INTERNAL_CMD" &
done
wait

echo "Internal components installed on all nodes."

#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <num_nodes> <cloudlab_username> <hostname_suffix>"
    exit 1
fi

NUM_NODES=$1
USERNAME=$2
HOSTNAME_SUFFIX=$3

THIS_DIR=$(dirname "$(readlink -f "$0")")
source $THIS_DIR"/constants.sh"
source $THIS_DIR"/utils.sh"

SINGLE_NODE_DIR="$THIS_DIR/../single_node"

setup_nodes $NUM_NODES $USERNAME $HOSTNAME_PREFIX $HOSTNAME_SUFFIX $SINGLE_NODE_DIR"/rsync.sh" true

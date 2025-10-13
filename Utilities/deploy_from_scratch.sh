#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <num_nodes> <cloudlab_username> <hostname_suffix>"
    exit 1
fi

NUM_NODES=$1
USERNAME=$2
HOSTNAME_SUFFIX=$3

THIS_DIR=$(dirname "$(readlink -f "$0")")
(cd "${THIS_DIR}/cloudlab_setup/multi_node"; "./oneshot_setup.sh" "$NUM_NODES" "$USERNAME" "$HOSTNAME_SUFFIX") || exit 1
(cd "${THIS_DIR}/installation"; "./oneshot_setup.sh" "$NUM_NODES" "$USERNAME" "$HOSTNAME_SUFFIX") || exit 1

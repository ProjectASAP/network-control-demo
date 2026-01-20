#!/bin/bash

if [ $# -ne 1 ]; then
  echo "Usage: $0 <path to kafka directory>"
  exit 1
fi

KAFKA_DIR=$1

KAFKA_CONFIG_FILE="./config/kraft/server.properties"

cd $KAFKA_DIR
./bin/kafka-server-start.sh $KAFKA_CONFIG_FILE
# check for error
if [ $? -ne 0 ]; then
    echo "Error starting Kafka server"
    echo "Trying reset"
    UUID=$(./bin/kafka-storage.sh random-uuid)
    ./bin/kafka-storage.sh format -t $UUID --config $KAFKA_CONFIG_FILE
    ./bin/kafka-server-start.sh $KAFKA_CONFIG_FILE
    if [ $? -ne 0 ]; then
        echo "Error starting Kafka server again"
        exit 1
    fi
fi

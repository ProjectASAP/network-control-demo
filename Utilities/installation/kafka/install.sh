#!/usr/bin/env bash

if [ -z "$1" ]; then
    echo "Usage: $0 <install_dir>"
    exit 1
fi

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
source "$THIS_DIR/../utils.sh"

INSTALL_DIR=$1

KAFKA_FILENAME="kafka_2.13-3.8.0.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/3.8.0/"$KAFKA_FILENAME
KAFKA_DIRNAME="kafka"

KAFKA_CONFIG_FILE="./config/kraft/server.properties"
KAFKA_LOG_DIR="/scratch/kraft-combined-logs"

mkdir -p $KAFKA_LOG_DIR

cd $INSTALL_DIR
wget $KAFKA_URL
untar $KAFKA_FILENAME $KAFKA_DIRNAME
cd $KAFKA_DIRNAME

#HOST_IP=$(ip r | grep 10.10 | awk '{print $9}')
HOST_IP=$(ip a | grep 10.10 | awk '{print $2}' | cut -d '/' -f1)

# Set up Kafka configuration
sed -i "s|log.dirs=.*|log.dirs=$KAFKA_LOG_DIR|g" $KAFKA_CONFIG_FILE
# if message.max.bytes is set, modify value to 4MB. Else set it to 4MB explicitly
set_property "$KAFKA_CONFIG_FILE" "message.max.bytes" "4194304"
set_property "$KAFKA_CONFIG_FILE" "log.retention.hours" "1"
set_property "$KAFKA_CONFIG_FILE" "advertised.listeners" "PLAINTEXT://"$HOST_IP":9092"

echo "Resetting Kafka storage"
UUID=$(./bin/kafka-storage.sh random-uuid)
./bin/kafka-storage.sh format -t $UUID --config $KAFKA_CONFIG_FILE
echo "Done resetting Kafka storage"

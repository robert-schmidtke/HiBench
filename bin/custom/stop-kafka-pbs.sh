#!/bin/bash

USAGE="Usage: ccmrun stop-kafka-pbs.sh"

if [ -z $PBS_JOBID ]; then
  echo "No PBS environment detected. $USAGE"
  exit 1
fi

source $(dirname $0)/env-ssd.sh

for kafka_node in ${KAFKA_NODES[@]}; do
  KAFKA_CONFIG_DIR=$KAFKA_HOME/config/$PBS_JOBID
  KAFKA_CONFIG=$KAFKA_CONFIG_DIR/$kafka_node.properties
  KAFKA_LOG_DIR=$KAFKA_HOME/log/$PBS_JOBID-$kafka_node

  # turns out you can't SIGINT a background process
  # the Kafka devs realized this from 0.8.2 onward
  ssh $kafka_node "sed -i 's/SIGINT/SIGTERM/g' $KAFKA_HOME/bin/kafka-server-stop.sh"

  ssh $kafka_node "$KAFKA_HOME/bin/kafka-server-stop.sh"
  sleep 5s

  ssh $kafka_node "rm $KAFKA_CONFIG"
  sleep 5s

  ssh $kafka_node "find $KAFKA_CONFIG_DIR -maxdepth 0 -empty -exec rm -r {} \;"
  ssh $kafka_node "rm -rf $KAFKA_LOG_DIR"
done

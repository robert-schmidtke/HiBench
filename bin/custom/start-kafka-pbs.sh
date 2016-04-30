#!/bin/bash

USAGE="Usage: ccmrun start-kafka-pbs.sh"

if [ -z $PBS_JOBID ]; then
  echo "No PBS environment detected. $USAGE"
  exit 1
fi

source $(dirname $0)/env-ssd.sh

broker_id=0
for kafka_node in ${KAFKA_NODES[@]}; do
  KAFKA_CONFIG_DIR=$KAFKA_HOME/config/$PBS_JOBID
  KAFKA_CONFIG=$KAFKA_CONFIG_DIR/$kafka_node.properties
  KAFKA_LOG_DIR=$KAFKA_HOME/log/$PBS_JOBID-$kafka_node

  ssh $kafka_node "mkdir -p $KAFKA_CONFIG_DIR"
  ssh $kafka_node "cp $KAFKA_HOME/config/server.properties $KAFKA_CONFIG"
  ssh $kafka_node "mkdir -p $KAFKA_LOG_DIR"
  ssh $kafka_node "sed -i \"/^broker\.id/c\broker.id=$broker_id\" $KAFKA_CONFIG"
  ssh $kafka_node "sed -i \"/^port/c\port=$KAFKA_PORT\" $KAFKA_CONFIG"
  ssh $kafka_node "sed -i \"/^log\.dirs/c\log.dirs=$KAFKA_LOG_DIR\" $KAFKA_CONFIG"
  ssh $kafka_node "sed -i \"/^num\.partitions/c\num.partitions=$KAFKA_DEFAULT_PARTITIONS\" $KAFKA_CONFIG"
  ssh $kafka_node "sed -i \"/^zookeeper\.connect=/c\zookeeper.connect=$ZOOKEEPER_NODE:$ZOOKEEPER_PORT\" $KAFKA_CONFIG"
  ssh $kafka_node "nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_CONFIG >> $KAFKA_LOG_DIR/$kafka_node.log 2>&1 &"
  broker_id=$(($broker_id + 1))
done

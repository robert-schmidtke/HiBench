#!/bin/bash

USAGE="Usage: srun --nodes=NUM_KAFKA_NODES --nodelist=<KAFKA_NODES> start-kafka-slurm.sh"

if [ -z $SLURM_JOB_ID ]; then
  echo "No Slurm environment detected. $USAGE"
  exit 1
fi

source $(dirname $0)/env-slurm.sh

broker_id=0
for kafka_node in ${KAFKA_NODES[@]}; do
  if [ "$kafka_node" = "$(hostname)" ]; then
    break
  fi
  broker_id=$(($broker_id + 1))
done

mkdir -p $KAFKA_HOME/config/$SLURM_JOB_ID
cp $KAFKA_HOME/config/server.properties $KAFKA_HOME/config/$SLURM_JOB_ID/$(hostname).properties

mkdir -p /local/$KAFKA_LOCAL_DIR
sed -i "/^broker\.id/c\broker.id=$broker_id" $KAFKA_HOME/config/$SLURM_JOB_ID/$(hostname).properties
sed -i "/^port/c\port=$KAFKA_PORT" $KAFKA_HOME/config/$SLURM_JOB_ID/$(hostname).properties
sed -i "/^log\.dirs/c\log.dirs=/local/$KAFKA_LOCAL_DIR" $KAFKA_HOME/config/$SLURM_JOB_ID/$(hostname).properties
sed -i "/^num\.partitions/c\num.partitions=$KAFKA_DEFAULT_PARTITIONS" $KAFKA_HOME/config/$SLURM_JOB_ID/$(hostname).properties
sed -i "/^zookeeper\.connect=/c\zookeeper.connect=$ZOOKEEPER_NODE:$ZOOKEEPER_PORT" $KAFKA_HOME/config/$SLURM_JOB_ID/$(hostname).properties

nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/$SLURM_JOB_ID/$(hostname).properties >> /local/$KAFKA_LOCAL_DIR/$(hostname).log 2>&1 &

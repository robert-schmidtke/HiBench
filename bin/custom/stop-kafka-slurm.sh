#!/bin/bash

USAGE="Usage: srun --nodes=NUM_KAFKA_NODES --nodelist=<KAFKA_NODES> stop-kafka-slurm.sh"

if [ -z $SLURM_JOB_ID ]; then
  echo "No Slurm environment detected. $USAGE"
  exit 1
fi

source $(dirname $0)/env-slurm.sh

sed -i 's/SIGINT/SIGTERM/g' $KAFKA_HOME/bin/kafka-server-stop.sh
$KAFKA_HOME/bin/kafka-server-stop.sh
sleep 5s

rm $KAFKA_HOME/config/$SLURM_JOB_ID/$(hostname).properties
sleep 5s
rm -d $KAFKA_HOME/config/$SLURM_JOB_ID
rm -rf /local/$KAFKA_LOCAL_DIR

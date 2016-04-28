#!/bin/bash

USAGE="Usage: srun --nodes=1-1 --nodelist=<ZOOKEEPER_NODE> start-zookeeper-slurm.sh"

if [ -z $SLURM_JOB_ID ]; then
  echo "No Slurm environment detected. $USAGE"
  exit 1
fi

source $(dirname $0)/env-slurm.sh

cp $ZOOKEEPER_HOME/conf/zoo_sample.cfg $ZOOKEEPER_HOME/conf/zoo.cfg
mkdir -p $ZOOKEEPER_HOME/data-$SLURM_JOB_ID
sed -i "/^dataDir/c\dataDir=$ZOOKEEPER_HOME/data-$SLURM_JOB_ID" $ZOOKEEPER_HOME/conf/zoo.cfg
sed -i "/^clientPort/c\clientPort=$ZOOKEEPER_PORT" $ZOOKEEPER_HOME/conf/zoo.cfg
$ZOOKEEPER_HOME/bin/zkServer.sh start

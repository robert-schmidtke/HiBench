#!/bin/bash

USAGE="Usage: ccmrun start-zookeeper-pbs.sh"

if [ -z $PBS_JOBID ]; then
  echo "No PBS environment detected. $USAGE"
  exit 1
fi

source $(dirname $0)/env-ssd.sh

ZOOKEEPER_CONFIG=$ZOOKEEPER_HOME/conf/zoo.cfg
ZOOKEEPER_DATA_DIR=$ZOOKEEPER_HOME/data-$PBS_JOBID

cp $ZOOKEEPER_HOME/conf/zoo_sample.cfg $ZOOKEEPER_CONFIG
mkdir -p $ZOOKEEPER_DATA_DIR
sed -i "/^dataDir/c\dataDir=$ZOOKEEPER_DATA_DIR" $ZOOKEEPER_CONFIG
sed -i "/^clientPort/c\clientPort=$ZOOKEEPER_PORT" $ZOOKEEPER_CONFIG
$ZOOKEEPER_HOME/bin/zkServer.sh start

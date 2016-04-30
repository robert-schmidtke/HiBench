#!/bin/bash

USAGE="Usage: ccmrun stop-zookeeper-pbs.sh"

if [ -z $PBS_JOBID ]; then
  echo "No PBS environment detected. $USAGE"
  exit 1
fi

source $(dirname $0)/env-ssd.sh

ZOOKEEPER_DATA_DIR=$ZOOKEEPER_HOME/data-$PBS_JOBID

$ZOOKEEPER_HOME/bin/zkServer.sh stop
sleep 5s
rm -rf $ZOOKEEPER_DATA_DIR/*
rm -r $ZOOKEEPER_DATA_DIR

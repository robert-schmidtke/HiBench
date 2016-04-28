#!/bin/bash

USAGE="Usage: srun --nodes=1-1 --nodelist=<ZOOKEEPER_NODE> stop-zookeeper-slurm.sh"

if [ -z $SLURM_JOB_ID ]; then
  echo "No Slurm environment detected. $USAGE"
  exit 1
fi

source $(dirname $0)/env-slurm.sh

$ZOOKEEPER_HOME/bin/zkServer.sh stop
sleep 5s
rm -rf $ZOOKEEPER_HOME/data-$SLURM_JOB_ID/*
rm -d $ZOOKEEPER_HOME/data-$SLURM_JOB_ID

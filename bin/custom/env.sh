#!/bin/bash

if [ -z $PBS_JOBID ]; then
  echo "No PBS environment detected."
  exit 1
fi

export WORK=/gfs1/work/$USER

export HOSTNAME=$(hostname)

export HADOOP_PREFIX="$HOME/hadoop-2.7.1"
export HADOOP_HOME=$HADOOP_PREFIX
export HADOOP_CONF_DIR="$HADOOP_PREFIX/conf/$PBS_JOBID"

export SPARK_HOME="$HOME/spark-1.6.0-bin-without-hadoop"

export HIBENCH_HOME=$HOME/workspace/HiBench

IFS=$'\n' read -d '' -r -a HADOOP_NODES < $PBS_NODEFILE
export HADOOP_NODES
export NUM_HADOOP_NODES=${#HADOOP_NODES[@]}
if [ $NUM_HADOOP_NODES -lt 2 ]; then
  echo "Please specify at least two nodes."
  exit 1
fi

export HADOOP_NAMENODE=${HADOOP_NODES[0]}
export HADOOP_DATANODES=(${HADOOP_NODES[@]:1})

export NUM_SSDS=8

# node-local directory for HDFS
export HDFS_LOCAL_DIR="$USER/hdfs/$PBS_JOBID"
export HDFS_LOCAL_LOG_DIR="$HDFS_LOCAL_DIR/log"

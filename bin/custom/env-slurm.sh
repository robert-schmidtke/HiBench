#!/bin/bash

if [ -z $SLURM_JOB_ID ]; then
  echo "No Slurm environment detected."
  exit 1
fi

if [ -f /etc/debian_version ]
  then
    function module { eval `/usr/bin/modulecmd bash $*`; }
    export MODULEPATH=/dassw/ubuntu/modules
fi

module load java/oracle-jdk1.8.0_45

export HOSTNAME=$(hostname)

export HADOOP_PREFIX="$HOME/hadoop-2.7.1"
#export HADOOP_PREFIX="$HOME/workspace/hadoop/hadoop-dist/target/hadoop-2.7.1"
export HADOOP_HOME=$HADOOP_PREFIX
export HADOOP_CONF_DIR="$HADOOP_PREFIX/conf/$SLURM_JOB_ID"

export SPARK_HOME="$HOME/spark-1.6.0-bin-without-hadoop"

export FLINK_HOME=/scratch/$USER/flink-0.10.2

export HIBENCH_HOME=$HOME/HiBench

export HADOOP_NODES=(`scontrol show hostnames`)
export NUM_HADOOP_NODES=${#HADOOP_NODES[@]}
if [ $NUM_HADOOP_NODES -lt 2 ]; then
  echo "Please specify at least two nodes."
  exit 1
fi

export HADOOP_NAMENODE=${HADOOP_NODES[0]}
export HADOOP_DATANODES=(${HADOOP_NODES[@]:1})
export NUM_HADOOP_DATANODES=${#HADOOP_DATANODES[@]}

# node-local directory for HDFS
export HDFS_LOCAL_DIR="$USER/hdfs/$SLURM_JOB_ID"
export HDFS_LOCAL_LOG_DIR="$HDFS_LOCAL_DIR/log"

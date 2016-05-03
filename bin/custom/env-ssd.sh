#!/bin/bash

if [ -z $PBS_JOBID ]; then
  echo "No PBS environment detected."
  exit 1
fi

export HOSTNAME=$(hostname)

#export HADOOP_PREFIX="$WORK/hadoop-2.7.1"
export HADOOP_PREFIX="$WORK/hadoop/hadoop-dist/target/hadoop-2.7.1"
export HADOOP_HOME=$HADOOP_PREFIX
export HADOOP_CONF_DIR="$HADOOP_PREFIX/conf/$PBS_JOBID"

export SPARK_HOME="$WORK/spark-1.6.0-bin-without-hadoop"
#export FLINK_HOME="$WORK/flink-0.10.2"
export FLINK_HOME="$WORK/flink/build-target"

export HIBENCH_HOME=$WORK/HiBench-ssd

export ZOOKEEPER_HOME=$WORK/zookeeper-3.3.6
export KAFKA_HOME=$WORK/kafka_2.10-0.8.1

IFS=$'\n' read -d '' -r -a NODES < $PBS_NODEFILE
export NODES
export NUM_KAFKA_NODES=${NUM_KAFKA_NODES:-0}
export NUM_HADOOP_NODES=$((${#NODES[@]} - $NUM_KAFKA_NODES))
if [ $NUM_HADOOP_NODES -lt 2 ]; then
  echo "Please specify at least two nodes."
  exit 1
fi

export HADOOP_NODES=(${NODES[@]:0:$NUM_HADOOP_NODES})

export HADOOP_NAMENODE=$(hostname) # ${HADOOP_NODES[0]}
hadoop_namenode=($HADOOP_NAMENODE) # tmp array var to remove from data nodes array
export HADOOP_DATANODES=(${HADOOP_NODES[@]/$hadoop_namenode})
export NUM_HADOOP_DATANODES=${#HADOOP_DATANODES[@]}

export KAFKA_NODES=(${NODES[@]:$NUM_HADOOP_NODES:$NUM_KAFKA_NODES})

export NUM_PRODUCER_NODES=${NUM_PRODUCER_NODES:-1}
export PRODUCER_NODES=(${NODES[@]:0:$NUM_PRODUCER_NODES})

export NUM_GFS=2
export SCRATCH=/flash/scratch5

# node-local directory for HDFS
export HDFS_LOCAL_DIR="$USER/hdfs/$PBS_JOBID"
export HDFS_LOCAL_LOG_DIR="$HDFS_LOCAL_DIR/log"

export KAFKA_DEFAULT_PARTITIONS=16
export KAFKA_PORT=9092

export ZOOKEEPER_PORT=2181
export ZOOKEEPER_NODE=$HADOOP_NAMENODE

export MAHOUT_HOME=$WORK/apache-mahout-distribution-0.11.1

# util functions
function join_array { local d=$1; shift; echo -n "$1"; shift; printf "%s" "${@/#/$d}"; }

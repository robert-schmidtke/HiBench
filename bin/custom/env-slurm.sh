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

#export HADOOP_PREFIX="$HOME/hadoop-2.7.1"
export HADOOP_PREFIX=/scratch/$USER/hadoop/hadoop-dist/target/hadoop-2.7.1
export HADOOP_HOME=$HADOOP_PREFIX
export HADOOP_CONF_DIR="$HADOOP_PREFIX/conf/$SLURM_JOB_ID"

#export SPARK_HOME=/scratch/$USER/spark-1.6.0-bin-without-hadoop
export SPARK_HOME=/scratch/$USER/spark-1.6.0-bin-custom-spark
#export FLINK_HOME=/scratch/$USER/flink-0.10.2
export FLINK_HOME=/scratch/$USER/flink/build-target

export HIBENCH_HOME=/scratch/$USER/HiBench

export ZOOKEEPER_HOME=/scratch/$USER/zookeeper-3.3.6
export KAFKA_HOME=/scratch/$USER/kafka_2.10-0.8.1

NODES=(`scontrol show hostnames`)
export NODES
export NUM_KAFKA_NODES=${NUM_KAFKA_NODES:-0}
export NUM_HADOOP_NODES=$((${#NODES[@]} - $NUM_KAFKA_NODES))
if [ $NUM_HADOOP_NODES -lt 2 ]; then
  echo "Please specify at least two nodes."
  exit 1
fi

export HADOOP_NODES=(${NODES[@]:0:$NUM_HADOOP_NODES})

export HADOOP_NAMENODE=${HADOOP_NODES[0]}
export HADOOP_DATANODES=(${HADOOP_NODES[@]:1})
export NUM_HADOOP_DATANODES=${#HADOOP_DATANODES[@]}

export KAFKA_NODES=(${NODES[@]:$NUM_HADOOP_NODES:$NUM_KAFKA_NODES})

export NUM_PRODUCER_NODES=${NUM_PRODUCER_NODES:-1}
export PRODUCER_NODES=(${NODES[@]:0:$NUM_PRODUCER_NODES})

# node-local directory for HDFS
export HDFS_LOCAL_DIR="$USER/hdfs/$SLURM_JOB_ID"
export HDFS_LOCAL_LOG_DIR="$HDFS_LOCAL_DIR/log"

# node-local directory for Kafka
export KAFKA_LOCAL_DIR="$USER/kafka/$SLURM_JOB_ID"
export KAFKA_DEFAULT_PARTITIONS=16
export KAFKA_PORT=9092

export ZOOKEEPER_PORT=2181
export ZOOKEEPER_NODE=$HADOOP_NAMENODE

export MAHOUT_HOME=/scratch/$USER/apache-mahout-distribution-0.11.1

# util functions
function join_array { local d=$1; shift; echo -n "$1"; shift; printf "%s" "${@/#/$d}"; }

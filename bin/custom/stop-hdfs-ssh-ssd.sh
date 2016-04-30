#!/bin/bash

USAGE="Usage: stop-hdfs-ssh-ssd.sh"

if [ -z $PBS_JOBID ]; then
  echo "No PBS environment detected. $USAGE"
  exit 1
fi

# HADOOP_NODES
# NUM_HADOOP_NODES
# HADOOP_NAMENODE
# HADOOP_DATANODES
# HADOOP_PREFIX
# HADOOP_CONF_DIR
source $(dirname $0)/env-ssd.sh

echo "Using Hadoop Distribution in '$HADOOP_PREFIX'."

echo "Stopping Hadoop NameNode on '$HADOOP_NAMENODE' and DataNode(s) on '${HADOOP_DATANODES[@]}'."

#mkdir -p $HADOOP_PREFIX/log-$PBS_JOBID
$HADOOP_PREFIX/sbin/stop-dfs.sh --config $HADOOP_CONF_DIR

$HADOOP_PREFIX/sbin/stop-yarn.sh --config $HADOOP_CONF_DIR

$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR stop historyserver

echo "Stopping Hadoop done."

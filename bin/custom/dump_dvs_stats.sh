#!/bin/bash

source $WORK/HiBench-ssd/bin/custom/env.sh

mkdir -p $HIBENCH_HOME/bin/custom/$PBS_JOBID.stats

for node in ${HADOOP_NODES[@]}; do
  dvs_file=$(ssh $node "mount | grep $SCRATCH | grep -o 'nodefile=[^,]*,' | cut -d= -f2 | sed -e's/,$//' -e's/nodenames/stats/'")
  ssh $node "cat $dvs_file" >> $HIBENCH_HOME/bin/custom/$PBS_JOBID.stats/$node-mount.stats
  cat /proc/fs/dvs/ipc/stats >> $HIBENCH_HOME/bin/custom/$PBS_JOBID.stats/$node-ipc.stats
done

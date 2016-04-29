#!/bin/bash

source $WORK2/HiBench/bin/custom/env.sh

mkdir -p $HIBENCH_HOME/bin/custom/$PBS_JOBID.stats

for node in ${HADOOP_NODES[@]}; do
  lustre_files=$(ssh $node "find /proc/fs/lustre/llite -name stats 2>/dev/null")
  for lustre_file in ${lustre_files[@]}; do
#    for stat_file in stats read_ahead_stats statahead_stats max_cached_mb; do
    for stat_file in stats; do
      ssh $node "cat $(dirname $lustre_file)/$stat_file" >> $HIBENCH_HOME/bin/custom/$PBS_JOBID.stats/$node-$(basename $(dirname $lustre_file))-$stat_file.stats
    done
  done
done

#!/bin/bash

source $WORK2/HiBench/bin/custom/env.sh

for node in ${HADOOP_NODES[@]}; do
  lustre_files=$(ssh $node "find /proc/fs/lustre/llite -name stats 2>/dev/null")
  for lustre_file in ${lustre_files[@]}; do
#    for stat_file in stats read_ahead_stats statahead_stats max_cached_mb; do
    for stat_file in stats; do
      ssh $node "sudo /opt/cray/lustre-cray_ari_s/2.5_3.0.101_0.46.1_1.0502.8871.17.1-1.0502.21371.9.1/bin/llstat -c $(dirname $lustre_file)/$stat_file"
    done
  done
done

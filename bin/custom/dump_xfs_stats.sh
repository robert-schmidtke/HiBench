#!/bin/bash

source /scratch/$USER/HiBench/bin/custom/env-slurm.sh

mkdir -p $HIBENCH_HOME/bin/custom/$SLURM_JOB_ID.stats

for hadoop_node in ${HADOOP_NODES[@]}; do
  srun -N1 -w$hadoop_node cat /proc/fs/xfs/stat >> $SLURM_JOB_ID.stats/$hadoop_node.stat
done

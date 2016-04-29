#!/bin/bash

source $WORK/HiBench-ssd/bin/custom/env.sh

for node in ${HADOOP_NODES[@]}; do
  dvs_file=$(ssh $node "mount | grep $SCRATCH | grep -o 'nodefile=[^,]*,' | cut -d= -f2 | sed -e's/,$//' -e's/nodenames/stats/'")
  ssh $node "echo 2 | sudo /usr/bin/tee $dvs_file"
done

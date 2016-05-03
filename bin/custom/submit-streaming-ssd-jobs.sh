#!/bin/bash

ldepend=""
for node_count in 7 11 15; do
  for engine in flink spark; do
    for batch_interval in 1 10 100 1000 10000; do
      if [ "$engine" == "flink" ] && [ "$batch_interval" == "1" ]; then
        batch_interval=0
      fi
      echo "$node_count nodes, on ssd, using $engine, with interval of $batch_interval, depending on $ldepend"
      jobid=$(msub $ldepend -lnodes=$node_count:ppn=1 -lwalltime=24:00:00 -F "\"$batch_interval\""  hibench-streaming-ssd-$engine.sh)
      jobid=($jobid)
      jobid=${jobid[0]}
      ldepend="-ldepend=$jobid"
      echo $jobid
    done
  done
done

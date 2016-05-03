#!/bin/bash

for node_count in 7 11 15; do
  for engine in flink spark; do
    for batch_interval in 1 10 100 1000 10000; do
      if [ "$engine" == "flink" ] && [ "$batch_interval" == "1" ]; then
        batch_interval=0
      fi
      echo "$node_count nodes, on lustre, using $engine, with interval of $batch_interval"
      msub -lnodes=$node_count:ppn=1 -lwalltime=24:00:00 -F "\"$batch_interval\""  hibench-streaming-lustre-$engine.sh
    done
  done
done

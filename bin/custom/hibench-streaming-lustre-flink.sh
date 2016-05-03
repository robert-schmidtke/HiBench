#!/bin/bash

#PBS -N hibench-streaming
#PBS -j oe
#PBS -l gres=ccm

source /opt/modules/default/init/bash
module load ccm java/jdk1.8.0_51
module unload atp # abnormal termination processing
cd $PBS_O_WORKDIR

batch_interval=$1

cat > detect-skew-$PBS_JOBID.sh << EOF
#!/bin/bash
echo "hibench.custom.nodes.\$(hostname).time \$(date +%s%3N)"
EOF

cat > launch-$PBS_JOBID.sh << EOF
#!/bin/bash

skew_file=\$1
batch_interval=\$2

module load java/jdk1.8.0_51

echo "Starting HiBench Streaming"
date

export NUM_KAFKA_NODES=4
export NUM_PRODUCER_NODES=4
source $WORK2/HiBench/bin/custom/env.sh

cp \$HIBENCH_HOME/conf/99-user_defined_properties.conf.template \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hadoop\.home/c\hibench.hadoop.home \$HADOOP_HOME" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hdfs\.master/c\hibench.hdfs.master hdfs://\$HADOOP_NAMENODE:8020" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^#hibench\.hadoop\.configure\.dir/c\hibench.hadoop.configure.dir \$HADOOP_CONF_DIR" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.streamingbench\.kafka\.home/c\hibench.streamingbench.kafka.home \$KAFKA_HOME" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.streamingbench\.zookeeper\.host/c\hibench.streamingbench.zookeeper.host \$ZOOKEEPER_NODE:\$ZOOKEEPER_PORT" \$HIBENCH_HOME/conf/99-user_defined_properties.conf

cat >> \$HIBENCH_HOME/conf/99-user_defined_properties.conf << EOL
hibench.report.dir \\\${hibench.home}/report-streaming.$PBS_JOBID
EOL

\$HIBENCH_HOME/bin/custom/start-hdfs-ssh-lustre.sh 262144 1

cp \$FLINK_HOME/conf/flink-conf.yaml.template \$FLINK_HOME/conf/flink-conf.yaml
sed -i "/^jobmanager\.rpc\.address/c\jobmanager.rpc.address: \$HADOOP_NAMENODE" \$FLINK_HOME/conf/flink-conf.yaml
sed -i "/^# fs\.hdfs\.hadoopconf/c\fs.hdfs.hadoopconf: \$HADOOP_CONF_DIR" \$FLINK_HOME/conf/flink-conf.yaml
sed -i "/^# taskmanager\.tmp\.dirs/c\taskmanager.tmp.dirs: /tmp/$USER/hadoop-tmp/nm-local-dir" \$FLINK_HOME/conf/flink-conf.yaml

echo "Starting Zookeeper \$(date)"
\$HIBENCH_HOME/bin/custom/start-zookeeper-pbs.sh
echo "Starting Zookeeper done \$(date)"

sleep 10s

echo "Starting Kafka on \${KAFKA_NODES[@]} \$(date)"
\$HIBENCH_HOME/bin/custom/start-kafka-pbs.sh
echo "Starting Kafka on \${KAFKA_NODES[@]} done \$(date)"

sleep 60s

broker_list=\$(join_array ":\${KAFKA_PORT}," \${KAFKA_NODES[@]}):\$KAFKA_PORT
node_list=\$(join_array "," \${NODES[@]})

cores=4

cp \$HIBENCH_HOME/workloads/streamingbench/conf/10-streamingbench-userdefine.conf.template \$HIBENCH_HOME/workloads/streamingbench/conf/10-streamingbench-userdefine.conf
cat >> \$HIBENCH_HOME/workloads/streamingbench/conf/10-streamingbench-userdefine.conf << EOL
hibench.streamingbench.benchname statistics
hibench.streamingbench.partitions \$KAFKA_DEFAULT_PARTITIONS
hibench.streamingbench.scale.profile larger
hibench.streamingbench.batch_interval \$batch_interval
hibench.streamingbench.batch_timeunit ms
hibench.streamingbench.copies 1
hibench.streamingbench.testWAL false
hibench.streamingbench.direct_mode true
hibench.streamingbench.prepare.mode push
hibench.streamingbench.prepare.push.records \\\${hibench.kmeans.num_of_samples}
hibench.streamingbench.record_count \\\${hibench.kmeans.num_of_samples}
hibench.streamingbench.num_producers \$NUM_PRODUCER_NODES
hibench.streamingbench.brokerList \$broker_list

dfs.replication 1
mapred.submit.replication 1
mapreduce.client.submit.file.replication 1
hibench.default.map.parallelism \$((\$NUM_HADOOP_DATANODES * \$cores))
hibench.default.shuffle.parallelism \$((\$NUM_HADOOP_DATANODES * \$cores))
hibench.yarn.taskmanager.num \$NUM_HADOOP_DATANODES
hibench.yarn.taskmanager.memory 20480
hibench.yarn.taskmanager.slots \$cores
hibench.yarn.jobmanager.memory 1792
flink.taskmanager.memory 20480
flink.jobmanager.memory 1792

# for our time measurements to be able to take clock skew into account
hibench.custom.nodes \$node_list
EOL

head -n\${#NODES[@]} \$skew_file >> \$HIBENCH_HOME/workloads/streamingbench/conf/10-streamingbench-userdefine.conf

\$HIBENCH_HOME/bin/custom/reset_dvs_stats.sh
\$HIBENCH_HOME/workloads/streamingbench/prepare/initTopic.sh
NO_DATA1=true \$HIBENCH_HOME/workloads/streamingbench/prepare/genSeedDataset.sh
\$HIBENCH_HOME/bin/custom/dump_dvs_stats.sh
\$HIBENCH_HOME/bin/custom/reset_dvs_stats.sh
echo "\$(date): Submitting Flink Job"
\$HIBENCH_HOME/workloads/streamingbench/flink/bin/run.sh 2>&1 &
FLINK_PID=\$!
echo "Flink Job running as PID \${FLINK_PID}"
sleep 30s

echo "\$(date): Starting data generation on \${PRODUCER_NODES[@]}"
\$HIBENCH_HOME/workloads/streamingbench/prepare/gendata.sh "\${PRODUCER_NODES[@]}"
echo "\$(date): Data generation done"
sleep 120s
\$HIBENCH_HOME/bin/custom/dump_dvs_stats.sh

# this should kill Flink too
echo "\$(date): Stopping Kafka"
\$HIBENCH_HOME/bin/custom/stop-kafka-pbs.sh
echo "\$(date): Stopping Kafka done"

sleep 30s

echo "\$(date): Stopping Zookeeper"
\$HIBENCH_HOME/bin/custom/stop-zookeeper-pbs.sh
echo "\$(date): Stopping Zookeeper done"

sleep 45s

# job history files are moved to the done folder every 180s
sleep 240s
\$HADOOP_PREFIX/bin/hadoop fs -copyToLocal hdfs://\$HADOOP_NAMENODE:8020/tmp/hadoop-yarn/staging/history/done \$HIBENCH_HOME/bin/custom/hibench-streaming.\$PBS_JOBID-history

\$HIBENCH_HOME/bin/custom/stop-hdfs-ssh-lustre.sh

echo "Stopping HiBench Streaming"
date

EOF

IFS=$'\n' read -d '' -r -a NODES < $PBS_NODEFILE
chmod +x detect-skew-$PBS_JOBID.sh
aprun -n${#NODES[@]} -N1 detect-skew-$PBS_JOBID.sh > $PBS_JOBID.skew
rm detect-skew-$PBS_JOBID.sh

chmod +x launch-$PBS_JOBID.sh
ccmrun ./launch-$PBS_JOBID.sh "$(pwd $(dirname $PBS_JOBID.skew))/$PBS_JOBID.skew" $batch_interval
rm launch-$PBS_JOBID.sh

rm -rf $WORK2/hadoop/hadoop-dist/target/hadoop-2.7.1/conf
rm -rf $WORK2/hadoop/hadoop-dist/target/hadoop-2.7.1/logs
rm -rf $WORK/hdfs
rm -rf $WORK2/hdfs
rm -rf $WORK2/kafka

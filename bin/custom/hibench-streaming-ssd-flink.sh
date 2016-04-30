#!/bin/bash

#PBS -N hibench-streaming
#PBS -l walltime=24:00:00
#PBS -j oe
#PBS -l gres=ccm

source /opt/modules/default/init/bash
module load ccm java/jdk1.8.0_51
module unload atp # abnormal termination processing
cd $PBS_O_WORKDIR

cat > launch-$PBS_JOBID.sh << EOF
#!/bin/bash

module load java/jdk1.8.0_51

echo "Starting HiBench Streaming"
date

export NUM_KAFKA_NODES=2
source $WORK/HiBench-ssd/bin/custom/env-ssd.sh

cp \$HIBENCH_HOME/conf/99-user_defined_properties.conf.template \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hadoop\.home/c\hibench.hadoop.home \$HADOOP_HOME" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hdfs\.master/c\hibench.hdfs.master hdfs://\$HADOOP_NAMENODE:8020" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^#hibench\.hadoop\.configure\.dir/c\hibench.hadoop.configure.dir \$HADOOP_CONF_DIR" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.streamingbench\.kafka\.home/c\hibench.streamingbench.kafka.home \$KAFKA_HOME" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.streamingbench\.zookeeper\.host/c\hibench.streamingbench.zookeeper.host \$ZOOKEEPER_NODE:\$ZOOKEEPER_PORT" \$HIBENCH_HOME/conf/99-user_defined_properties.conf

cat >> \$HIBENCH_HOME/conf/99-user_defined_properties.conf << EOL
hibench.report.dir \\\${hibench.home}/report-streaming.$PBS_JOBID
EOL

\$HIBENCH_HOME/bin/custom/start-hdfs-ssh-ssd.sh 262144 1

cp \$FLINK_HOME/conf/flink-conf.yaml.template \$FLINK_HOME/conf/flink-conf.yaml
sed -i "/^jobmanager\.rpc\.address/c\jobmanager.rpc.address: \$HADOOP_NAMENODE" \$FLINK_HOME/conf/flink-conf.yaml
sed -i "/^# fs\.hdfs\.hadoopconf/c\fs.hdfs.hadoopconf: \$HADOOP_CONF_DIR" \$FLINK_HOME/conf/flink-conf.yaml
sed -i "/^# taskmanager\.tmp\.dirs/c\taskmanager.tmp.dirs: /tmp/$USER/hadoop-tmp/nm-local-dir" \$FLINK_HOME/conf/flink-conf.yaml
#sed -i "/^# taskmanager\.network\.numberOfBuffers/c\taskmanager.network.numberOfBuffers: 131072" \$FLINK_HOME/conf/flink-conf.yaml

echo "Starting Zookeeper \$(date)"
\$HIBENCH_HOME/bin/custom/start-zookeeper-pbs.sh
echo "Starting Zookeeper done \$(date)"

sleep 10s

echo "Starting Kafka on \${KAFKA_NODES[@]} \$(date)"
\$HIBENCH_HOME/bin/custom/start-kafka-pbs.sh
echo "Starting Kafka on \${KAFKA_NODES[@]} done \$(date)"

sleep 60s

broker_list=\$(join_array ":\${KAFKA_PORT}," \${KAFKA_NODES[@]}):\$KAFKA_PORT

cores=4
#parallelism=770

cp \$HIBENCH_HOME/workloads/streamingbench/conf/10-streamingbench-userdefine.conf.template \$HIBENCH_HOME/workloads/streamingbench/conf/10-streamingbench-userdefine.conf
cat >> \$HIBENCH_HOME/workloads/streamingbench/conf/10-streamingbench-userdefine.conf << EOL
hibench.streamingbench.benchname statistics
hibench.streamingbench.partitions \$KAFKA_DEFAULT_PARTITIONS
hibench.streamingbench.scale.profile tiny
hibench.streamingbench.batch_interval 10
hibench.streamingbench.copies 1
hibench.streamingbench.testWAL false
hibench.streamingbench.direct_mode true
hibench.streamingbench.prepare.mode push
hibench.streamingbench.prepare.push.records \\\${hibench.kmeans.num_of_samples}
hibench.streamingbench.record_count \\\${hibench.kmeans.num_of_samples}
hibench.streamingbench.brokerList \$broker_list

dfs.replication 1
mapred.submit.replication 1
mapreduce.client.submit.file.replication 1
hibench.default.map.parallelism \$((\$NUM_HADOOP_DATANODES * \$cores))
hibench.default.shuffle.parallelism \$((\$NUM_HADOOP_DATANODES * \$cores))
#hibench.yarn.executor.num \$NUM_HADOOP_DATANODES
#hibench.yarn.executor.memory 20G
#hibench.yarn.executor.cores \$cores
#hibench.yarn.driver.memory 8G
hibench.yarn.taskmanager.num \$NUM_HADOOP_DATANODES
hibench.yarn.taskmanager.memory 20480
hibench.yarn.taskmanager.slots \$cores
hibench.yarn.jobmanager.memory 1792
flink.taskmanager.memory 20480
flink.jobmanager.memory 1792
EOL

#\$HIBENCH_HOME/bin/custom/reset_dvs_stats.sh
\$HIBENCH_HOME/workloads/streamingbench/prepare/initTopic.sh
NO_DATA1=true \$HIBENCH_HOME/workloads/streamingbench/prepare/genSeedDataset.sh
#\$HIBENCH_HOME/bin/custom/dump_dvs_stats.sh
#\$HIBENCH_HOME/bin/custom/reset_dvs_stats.sh
echo "\$(date): Submitting Flink Job"
\$HIBENCH_HOME/workloads/streamingbench/flink/bin/run.sh 2>&1 &
FLINK_PID=\$!
echo "Flink Job running as PID \${FLINK_PID}"
sleep 30s
echo "\$(date): Starting data generation"
\$HIBENCH_HOME/workloads/streamingbench/prepare/gendata.sh
echo "\$(date): Data generation done"
sleep 30s
#\$HIBENCH_HOME/bin/custom/dump_dvs_stats.sh

# this should kill Flink too
echo "\$(date): Stopping Kafka"
\$HIBENCH_HOME/bin/custom/stop-kafka-pbs.sh
echo "\$(date): Stopping Kafka done"

sleep 30s

#echo "\$(date): Killing Flink"
#kill -9 \$FLINK_PID
#echo "\$(date): Killing Flink done"

#sleep 30s

echo "\$(date): Stopping Zookeeper"
\$HIBENCH_HOME/bin/custom/stop-zookeeper-pbs.sh
echo "\$(date): Stopping Zookeeper done"

sleep 45s

# job history files are moved to the done folder every 180s
#sleep 240s
#\$HADOOP_PREFIX/bin/hadoop fs -copyToLocal hdfs://\$HADOOP_NAMENODE:8020/tmp/hadoop-yarn/staging/history/done \$HIBENCH_HOME/bin/custom/hibench-streaming-ssd.\$PBS_JOBID-history

\$HIBENCH_HOME/bin/custom/stop-hdfs-ssh-ssd.sh

echo "Stopping HiBench Streaming"
date

EOF

chmod +x launch-$PBS_JOBID.sh
ccmrun ./launch-$PBS_JOBID.sh
rm launch-$PBS_JOBID.sh

rm -rf $WORK/hadoop/hadoop-dist/target/hadoop-2.7.1/conf
rm -rf $WORK/hadoop/hadoop-dist/target/hadoop-2.7.1/logs
rm -rf /flash/scratch5/$USER/hdfs

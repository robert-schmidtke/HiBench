#!/bin/bash

#SBATCH -J hibench-streaming
#SBATCH --exclusive
#SBATCH --open-mode=append

export NUM_KAFKA_NODES=4
export NUM_PRODUCER_NODES=4
source /scratch/$USER/HiBench/bin/custom/env-slurm.sh

cat > detect-skew-$SLURM_JOB_ID.sh << EOL
#!/bin/bash
echo "hibench.custom.nodes.\$(hostname).time \$(date +%s%3N)"
EOL
chmod +x detect-skew-$SLURM_JOB_ID.sh

skew_file=$SLURM_JOB_ID.skew
srun --nodes=${#NODES[@]} detect-skew-$SLURM_JOBID.sh > $skew_file
rm detect-skew-$SLURM_JOB_ID.sh

cp $HIBENCH_HOME/conf/99-user_defined_properties.conf.template $HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hadoop\.home/c\hibench.hadoop.home $HADOOP_HOME" $HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hdfs\.master/c\hibench.hdfs.master hdfs://$HADOOP_NAMENODE:8020" $HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^#hibench\.hadoop\.configure\.dir/c\hibench.hadoop.configure.dir $HADOOP_CONF_DIR" $HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.streamingbench\.kafka\.home/c\hibench.streamingbench.kafka.home $KAFKA_HOME" $HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.streamingbench\.zookeeper\.host/c\hibench.streamingbench.zookeeper.host $HADOOP_NAMENODE:2181" $HIBENCH_HOME/conf/99-user_defined_properties.conf

cat >> $HIBENCH_HOME/conf/99-user_defined_properties.conf << EOL
hibench.report.dir \${hibench.home}/report-streaming.$SLURM_JOB_ID
EOL

echo "Cleaning Java processes ..."
srun -N$SLURM_JOB_NUM_NODES killall -sSIGKILL java
echo "Cleaning Java processes done"

echo "Cleaning local directories ..."
srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER/hdfs
echo "Cleaning local directories done"

echo "Starting Hadoop $(date)"
srun --nodes=1-1 --nodelist=$HADOOP_NAMENODE $HIBENCH_HOME/bin/custom/start-hdfs-slurm.sh 262144 1
echo "Starting Hadoop done $(date)"

echo "Creating local Flink folders $(date)"
srun -N$SLURM_JOB_NUM_NODES mkdir -p /local/$USER/flink/$SLURM_JOB_ID
echo "Creating local Flink folders done $(date)"

cp $FLINK_HOME/conf/flink-conf.yaml.template $FLINK_HOME/conf/flink-conf.yaml
sed -i "/^jobmanager\.rpc\.address/c\jobmanager.rpc.address: $HADOOP_NAMENODE" $FLINK_HOME/conf/flink-conf.yaml
sed -i "/^# fs\.hdfs\.hadoopconf/c\fs.hdfs.hadoopconf: $HADOOP_CONF_DIR" $FLINK_HOME/conf/flink-conf.yaml
sed -i "/^# taskmanager\.tmp\.dirs/c\taskmanager.tmp.dirs: /local/$USER/flink/$SLURM_JOB_ID" $FLINK_HOME/conf/flink-conf.yaml

echo "Starting Zookeeper $(date)"
srun --nodes=1-1 --nodelist=$ZOOKEEPER_NODE $HIBENCH_HOME/bin/custom/start-zookeeper-slurm.sh
echo "Starting Zookeeper done $(date)"

sleep 10s

echo "Starting Kafka on ${KAFKA_NODES[@]} $(date)"
srun -N$NUM_KAFKA_NODES --nodelist=$(join_array , ${KAFKA_NODES[@]}) $HIBENCH_HOME/bin/custom/start-kafka-slurm.sh
echo "Starting Kafka done $(date)"

sleep 60s

broker_list=$(join_array ":${KAFKA_PORT}," ${KAFKA_NODES[@]}):$KAFKA_PORT
node_list=$(join_array "," ${NODES[@]})

cores=4

cp $HIBENCH_HOME/workloads/streamingbench/conf/10-streamingbench-userdefine.conf.template $HIBENCH_HOME/workloads/streamingbench/conf/10-streamingbench-userdefine.conf
cat >> $HIBENCH_HOME/workloads/streamingbench/conf/10-streamingbench-userdefine.conf << EOL
hibench.streamingbench.benchname statistics
hibench.streamingbench.partitions $KAFKA_DEFAULT_PARTITIONS
hibench.streamingbench.scale.profile larger
hibench.streamingbench.batch_interval 10000
hibench.streamingbench.batch_timeunit ms
hibench.streamingbench.copies 1
hibench.streamingbench.testWAL false
hibench.streamingbench.direct_mode true
hibench.streamingbench.prepare.mode push
hibench.streamingbench.prepare.push.records \${hibench.kmeans.num_of_samples}
hibench.streamingbench.record_count \${hibench.kmeans.num_of_samples}
hibench.streamingbench.num_producers $NUM_PRODUCER_NODES
hibench.streamingbench.brokerList $broker_list

dfs.replication 1
mapred.submit.replication 1
mapreduce.client.submit.file.replication 1
hibench.default.map.parallelism $(($NUM_HADOOP_DATANODES * $cores))
hibench.default.shuffle.parallelism $(($NUM_HADOOP_DATANODES * $cores))
hibench.yarn.taskmanager.num $NUM_HADOOP_DATANODES
hibench.yarn.taskmanager.memory 39936
hibench.yarn.taskmanager.slots $cores
hibench.yarn.jobmanager.memory 2048
flink.taskmanager.memory 39936
flink.jobmanager.memory 2048

hibench.custom.nodes $node_list
EOL

head -n${#NODES[@]} $skew_file >> $HIBENCH_HOME/workloads/streamingbench/conf/10-streamingbench-userdefine.conf
rm $skew_file

$HIBENCH_HOME/bin/custom/dump_xfs_stats.sh
$HIBENCH_HOME/workloads/streamingbench/prepare/initTopic.sh
NO_DATA1=true $HIBENCH_HOME/workloads/streamingbench/prepare/genSeedDataset.sh
$HIBENCH_HOME/bin/custom/dump_xfs_stats.sh
echo "$(date): Submitting Flink Job"
$HIBENCH_HOME/workloads/streamingbench/flink/bin/run.sh 2>&1 &
FLINK_PID=$!
echo "Flink Job running as PID ${FLINK_PID}"
sleep 30s
echo "$(date): Starting data generation on ${PRODUCER_NODES[@]}"
$HIBENCH_HOME/workloads/streamingbench/prepare/gendata-slurm.sh "${PRODUCER_NODES[@]}"
echo "$(date): Data generation done"
sleep 120s
$HIBENCH_HOME/bin/custom/dump_xfs_stats.sh

# job history files are moved to the done folder every 180s
sleep 240s
$HADOOP_PREFIX/bin/hadoop fs -copyToLocal hdfs://$HADOOP_NAMENODE:8020/tmp/hadoop-yarn/staging/history/done $HIBENCH_HOME/bin/custom/hibench-terasort.$SLURM_JOB_ID-history

echo "Stopping Kafka on ${KAFKA_NODES[@]} $(date)"
srun -N$NUM_KAFKA_NODES --nodelist=$(join_array , ${KAFKA_NODES[@]}) $HIBENCH_HOME/bin/custom/stop-kafka-slurm.sh
echo "Stopping Kafka done $(date)"

sleep 30s

echo "Deleting local Flink folders $(date)"
srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER/flink/$SLURM_JOB_ID
echo "Deleting local Flink folders done $(date)"

echo "Stopping Zookeeper $(date)"
srun --nodes=1-1 --nodelist=$HADOOP_NAMENODE $HIBENCH_HOME/bin/custom/stop-zookeeper-slurm.sh
echo "Stopping Zookeeper done $(date)"

sleep 45s

echo "Stopping Hadoop $(date)"
srun --nodes=1-1 --nodelist=$HADOOP_NAMENODE $HIBENCH_HOME/bin/custom/stop-hdfs-slurm.sh
echo "Stopping Hadoop done $(date)"

sleep 60s

echo "Cleaning Java processes ..."
srun -N$SLURM_JOB_NUM_NODES killall -sSIGKILL java
echo "Cleaning Java processes done"

echo "Cleaning local directories ..."
srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER/hdfs
srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER/kafka
echo "Cleaning local directories done"

rm -rf $HADOOP_PREFIX/conf*
rm -rf $HADOOP_PREFIX/log*

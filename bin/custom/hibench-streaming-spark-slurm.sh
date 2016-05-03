#!/bin/bash

#SBATCH -J hibench-streaming
#SBATCH --exclusive
#SBATCH --open-mode=append

export NUM_KAFKA_NODES=4
source /scratch/$USER/HiBench/bin/custom/env-slurm.sh

cp $HIBENCH_HOME/conf/99-user_defined_properties.conf.template $HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hadoop\.home/c\hibench.hadoop.home $HADOOP_HOME" $HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.spark\.home/c\hibench.spark.home $SPARK_HOME" $HIBENCH_HOME/conf/99-user_defined_properties.conf
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

# add Hadoop classpath to Spark after Hadoop is running
cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
cat >> $SPARK_HOME/conf/spark-env.sh << EOL
export SPARK_DIST_CLASSPATH=$($HADOOP_PREFIX/bin/hadoop --config $HADOOP_CONF_DIR classpath)
EOL

echo "Starting Zookeeper $(date)"
srun --nodes=1-1 --nodelist=$ZOOKEEPER_NODE $HIBENCH_HOME/bin/custom/start-zookeeper-slurm.sh
echo "Starting Zookeeper done $(date)"

sleep 10s

echo "Starting Kafka on ${KAFKA_NODES[@]} $(date)"
srun -N$NUM_KAFKA_NODES --nodelist=$(join , ${KAFKA_NODES[@]}) $HIBENCH_HOME/bin/custom/start-kafka-slurm.sh
echo "Starting Kafka done $(date)"

sleep 60s

$HADOOP_PREFIX/bin/hadoop fs -mkdir -p hdfs://$HADOOP_NAMENODE:8020/tmp/spark-events

broker_list=$(join ":${KAFKA_PORT}," ${KAFKA_NODES[@]}):$KAFKA_PORT

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
hibench.streamingbench.brokerList $broker_list

dfs.replication 1
mapred.submit.replication 1
mapreduce.client.submit.file.replication 1
hibench.default.map.parallelism $(($NUM_HADOOP_DATANODES * $cores))
hibench.default.shuffle.parallelism $(($NUM_HADOOP_DATANODES * $cores))
hibench.yarn.executor.num $NUM_HADOOP_DATANODES
hibench.yarn.executor.memory 38G
hibench.yarn.executor.cores $cores
hibench.yarn.driver.memory 15G

spark.driver.memory 15G
spark.executor.cores $cores
spark.executor.memory 38G
spark.eventLog.enabled true
spark.eventLog.dir hdfs://$HADOOP_NAMENODE:8020/tmp/spark-events
EOL

$HIBENCH_HOME/bin/custom/dump_xfs_stats.sh
echo "$(date): Initializing topics and generating seed data set"
$HIBENCH_HOME/workloads/streamingbench/prepare/initTopic.sh
NO_DATA1=true $HIBENCH_HOME/workloads/streamingbench/prepare/genSeedDataset.sh
echo "$(date): Initializing topics and generating seed data set done"
$HIBENCH_HOME/bin/custom/dump_xfs_stats.sh
echo "$(date): Submitting Spark Job"
$HIBENCH_HOME/workloads/streamingbench/spark/bin/run.sh 2>&1 &
SPARK_PID=$!
echo "$(date): Spark Job running as PID ${SPARK_PID}"
sleep 30s
echo "$(date): Starting data generation"
$HIBENCH_HOME/workloads/streamingbench/prepare/gendata.sh
echo "$(date): Data generation done"
sleep 30s
$HIBENCH_HOME/bin/custom/dump_xfs_stats.sh

# job history files are moved to the done folder every 180s
sleep 240s
$HADOOP_PREFIX/bin/hadoop fs -copyToLocal hdfs://$HADOOP_NAMENODE:8020/tmp/hadoop-yarn/staging/history/done $HIBENCH_HOME/bin/custom/hibench-streaming.$SLURM_JOB_ID-history
$HADOOP_PREFIX/bin/hadoop fs -copyToLocal hdfs://$HADOOP_NAMENODE:8020/tmp/spark-events $HIBENCH_HOME/bin/custom/hibench-streaming.$SLURM_JOB_ID-sparkhistory

echo "Stopping Kafka on ${KAFKA_NODES[@]} $(date)"
srun -N$NUM_KAFKA_NODES --nodelist=$(join , ${KAFKA_NODES[@]}) $HIBENCH_HOME/bin/custom/stop-kafka-slurm.sh
echo "Stopping Kafka done $(date)"

sleep 10s

echo "Stopping Zookeeper $(date)"
srun --nodes=1-1 --nodelist=$HADOOP_NAMENODE $HIBENCH_HOME/bin/custom/stop-zookeeper-slurm.sh
echo "Stopping Zookeeper done $(date)"

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

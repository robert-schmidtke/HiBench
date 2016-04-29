#!/bin/bash

#SBATCH -J hibench-terasort
#SBATCH --exclusive
#SBATCH --open-mode=append

source /scratch/$USER/HiBench/bin/custom/env-slurm.sh

#$HOME/collectl-slurm/collectl_slurm.sh start

cp $HIBENCH_HOME/conf/99-user_defined_properties.conf.template $HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hadoop\.home/c\hibench.hadoop.home $HADOOP_HOME" $HIBENCH_HOME/conf/99-user_defined_properties.conf
#sed -i "/^hibench\.spark\.home/c\hibench.spark.home $SPARK_HOME" $HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hdfs\.master/c\hibench.hdfs.master hdfs://$HADOOP_NAMENODE:8020" $HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^#hibench\.hadoop\.configure\.dir/c\hibench.hadoop.configure.dir $HADOOP_CONF_DIR" $HIBENCH_HOME/conf/99-user_defined_properties.conf

cat >> $HIBENCH_HOME/conf/99-user_defined_properties.conf << EOL
hibench.report.dir \${hibench.home}/report-terasort.$SLURM_JOB_ID
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
#cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
#cat >> $SPARK_HOME/conf/spark-env.sh << EOL
#export SPARK_DIST_CLASSPATH=$($HADOOP_PREFIX/bin/hadoop --config $HADOOP_CONF_DIR classpath)
#EOL

echo "Creating local Flink folders $(date)"
srun -N$SLURM_JOB_NUM_NODES mkdir -p /local/$USER/flink/$SLURM_JOB_ID
echo "Creating local Flink folders done $(date)"

cp $FLINK_HOME/conf/flink-conf.yaml.template $FLINK_HOME/conf/flink-conf.yaml
sed -i "/^jobmanager\.rpc\.address/c\jobmanager.rpc.address: $HADOOP_NAMENODE" $FLINK_HOME/conf/flink-conf.yaml
sed -i "/^# fs\.hdfs\.hadoopconf/c\fs.hdfs.hadoopconf: $HADOOP_CONF_DIR" $FLINK_HOME/conf/flink-conf.yaml
sed -i "/^# taskmanager\.tmp\.dirs/c\taskmanager.tmp.dirs: /local/$USER/flink/$SLURM_JOB_ID" $FLINK_HOME/conf/flink-conf.yaml
sed -i "/^# taskmanager\.network\.numberOfBuffers/c\taskmanager.network.numberOfBuffers: 262144" $FLINK_HOME/conf/flink-conf.yaml

sleep 60s

#$HADOOP_PREFIX/bin/hadoop fs -mkdir -p hdfs://$HADOOP_NAMENODE:8020/tmp/spark-events

cores=32
parallelism=$(($NUM_HADOOP_DATANODES * $cores))

cp $HIBENCH_HOME/workloads/terasort/conf/10-terasort-userdefine.conf.template $HIBENCH_HOME/workloads/terasort/conf/10-terasort-userdefine.conf
cat >> $HIBENCH_HOME/workloads/terasort/conf/10-terasort-userdefine.conf << EOL
hibench.scale.profile tera
dfs.replication 1
mapred.submit.replication 1
mapreduce.client.submit.file.replication 1
hibench.default.map.parallelism $(($NUM_HADOOP_DATANODES * $cores))
hibench.default.shuffle.parallelism $(($NUM_HADOOP_DATANODES * $cores))
hibench.yarn.executor.num $NUM_HADOOP_DATANODES
hibench.yarn.executor.memory 39G
hibench.yarn.executor.cores $cores
hibench.yarn.driver.memory 16G

#spark.driver.memory 10G
#spark.executor.cores 4
#spark.executor.memory 2G
#spark.eventLog.enabled true
#spark.eventLog.dir hdfs://$HADOOP_NAMENODE:8020/tmp/spark-events
EOL

$HIBENCH_HOME/bin/custom/dump_xfs_stats.sh
$HIBENCH_HOME/workloads/terasort/prepare/prepare.sh
$HIBENCH_HOME/bin/custom/dump_xfs_stats.sh
$HADOOP_PREFIX/bin/hadoop fs -mkdir -p hdfs://$HADOOP_NAMENODE:8020/HiBench/Terasort/Output
$FLINK_HOME/bin/flink run \
  -m yarn-cluster \
  -yn $NUM_HADOOP_DATANODES \
  -ys $cores \
  -p $parallelism \
  -yjm 2048 \
  -ytm 39936 \
  -c eastcircle.terasort.FlinkTeraSort \
  /scratch/$USER/terasort/target/scala-2.10/terasort_2.10-0.0.1.jar \
  hdfs://$HADOOP_NAMENODE:8020 /HiBench/Terasort/Input /HiBench/Terasort/Output \
  $parallelism
$HIBENCH_HOME/bin/custom/dump_xfs_stats.sh

#$HIBENCH_HOME/workloads/terasort/mapreduce/bin/run.sh
#$HIBENCH_HOME/workloads/terasort/spark/scala/bin/run.sh

# job history files are moved to the done folder every 180s
sleep 240s
$HADOOP_PREFIX/bin/hadoop fs -copyToLocal hdfs://$HADOOP_NAMENODE:8020/tmp/hadoop-yarn/staging/history/done $HIBENCH_HOME/bin/custom/hibench-terasort.$SLURM_JOB_ID-history
#$HADOOP_PREFIX/bin/hadoop fs -copyToLocal hdfs://$HADOOP_NAMENODE:8020/tmp/spark-events $HIBENCH_HOME/bin/custom/hibench-terasort.$SLURM_JOB_ID-sparkhistory

echo "Deleting local Flink folders $(date)"
srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER/flink/$SLURM_JOB_ID
echo "Deleting local Flink folders done $(date)"

echo "Stopping Hadoop $(date)"
srun --nodes=1-1 --nodelist=$HADOOP_NAMENODE $HIBENCH_HOME/bin/custom/stop-hdfs-slurm.sh
echo "Stopping Hadoop done $(date)"

sleep 60s

echo "Cleaning Java processes ..."
srun -N$SLURM_JOB_NUM_NODES killall -sSIGKILL java
echo "Cleaning Java processes done"

echo "Cleaning local directories ..."
srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER/hdfs
echo "Cleaning local directories done"

rm -rf $HADOOP_PREFIX/conf*
rm -rf $HADOOP_PREFIX/log*

#$HOME/collectl-slurm/collectl_slurm.sh stop -savelogs

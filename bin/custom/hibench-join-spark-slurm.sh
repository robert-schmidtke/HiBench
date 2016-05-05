#!/bin/bash

#SBATCH -J hibench-join
#SBATCH --exclusive
#SBATCH --open-mode=append

source /scratch/$USER/HiBench/bin/custom/env-slurm.sh

cp $HIBENCH_HOME/conf/99-user_defined_properties.conf.template $HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hadoop\.home/c\hibench.hadoop.home $HADOOP_HOME" $HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.spark\.home/c\hibench.spark.home $SPARK_HOME" $HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hdfs\.master/c\hibench.hdfs.master hdfs://$HADOOP_NAMENODE:8020" $HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^#hibench\.hadoop\.configure\.dir/c\hibench.hadoop.configure.dir $HADOOP_CONF_DIR" $HIBENCH_HOME/conf/99-user_defined_properties.conf

cat >> $HIBENCH_HOME/conf/99-user_defined_properties.conf << EOL
hibench.report.dir \${hibench.home}/report-join.$SLURM_JOB_ID
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

sleep 60s

$HADOOP_PREFIX/bin/hadoop fs -mkdir -p hdfs://$HADOOP_NAMENODE:8020/tmp/spark-events

cores=4

cp $HIBENCH_HOME/workloads/join/conf/10-join-userdefine.conf.template $HIBENCH_HOME/workloads/join/conf/10-join-userdefine.conf
cat >> $HIBENCH_HOME/workloads/join/conf/10-join-userdefine.conf << EOL
hibench.scale.profile tiny
dfs.replication 1
mapred.submit.replication 1
mapreduce.client.submit.file.replication 1
hibench.default.map.parallelism $(($NUM_HADOOP_DATANODES * $cores))
hibench.default.shuffle.parallelism $(($NUM_HADOOP_DATANODES * $cores))
hibench.yarn.executor.num $NUM_HADOOP_DATANODES
hibench.yarn.executor.memory 38G
hibench.yarn.executor.cores $cores
hibench.yarn.driver.memory 16G

spark.driver.memory 15G
spark.executor.cores $cores
spark.executor.memory 38G
spark.eventLog.enabled true
spark.eventLog.dir hdfs://$HADOOP_NAMENODE:8020/tmp/spark-events
EOL

$HIBENCH_HOME/bin/custom/dump_xfs_stats.sh
$HIBENCH_HOME/workloads/join/prepare/prepare.sh
$HIBENCH_HOME/bin/custom/dump_xfs_stats.sh
$HIBENCH_HOME/workloads/join/spark/scala/bin/run.sh
$HIBENCH_HOME/bin/custom/dump_xfs_stats.sh

#echo "Printing result file list"
#result_files=$($HADOOP_PREFIX/bin/hadoop fs -ls -R hdfs://$HADOOP_NAMENODE:8020/HiBench/Join/Output/rankings_uservisits_join 2> /dev/null)
#echo "$result_files"
#result_files=($result_files)

#i=7
#while [ $i -lt ${#result_files[@]} ]; do
#  echo "Printing top 10 results for ${result_files[$i]}"
#  $HADOOP_PREFIX/bin/hadoop fs -cat ${result_files[$i]} | head -n10
#  i=$(($i + 8))
#done

# job history files are moved to the done folder every 180s
sleep 240s
$HADOOP_PREFIX/bin/hadoop fs -copyToLocal hdfs://$HADOOP_NAMENODE:8020/tmp/hadoop-yarn/staging/history/done $HIBENCH_HOME/bin/custom/hibench-join.$SLURM_JOB_ID-history
$HADOOP_PREFIX/bin/hadoop fs -copyToLocal hdfs://$HADOOP_NAMENODE:8020/tmp/spark-events $HIBENCH_HOME/bin/custom/hibench-join.$SLURM_JOB_ID-sparkhistory

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

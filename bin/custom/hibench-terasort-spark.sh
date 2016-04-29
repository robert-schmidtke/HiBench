#!/bin/bash

#PBS -N hibench-terasort
#PBS -l walltime=48:00:00
#PBS -j oe
#PBS -l gres=ccm

source /opt/modules/default/init/bash
module load ccm java/jdk1.8.0_51
module unload atp # abnormal termination processing
cd $PBS_O_WORKDIR

cat > launch-$PBS_JOBID.sh << EOF
#!/bin/bash

module load java/jdk1.8.0_51

echo "Starting HiBench TeraSort"
date

source $WORK2/HiBench/bin/custom/env.sh

cp \$HIBENCH_HOME/conf/99-user_defined_properties.conf.template \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hadoop\.home/c\hibench.hadoop.home \$HADOOP_HOME" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.spark\.home/c\hibench.spark.home \$SPARK_HOME" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hdfs\.master/c\hibench.hdfs.master hdfs://\$HADOOP_NAMENODE:8020" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^#hibench\.hadoop\.configure\.dir/c\hibench.hadoop.configure.dir \$HADOOP_CONF_DIR" \$HIBENCH_HOME/conf/99-user_defined_properties.conf

cat >> \$HIBENCH_HOME/conf/99-user_defined_properties.conf << EOL
hibench.report.dir \\\${hibench.home}/report-terasort.$PBS_JOBID
EOL

$WORK2/HiBench/bin/custom/start-hdfs-ssh-lustre.sh 262144 1

# add Hadoop classpath to Spark after Hadoop is running
cp \$SPARK_HOME/conf/spark-env.sh.template \$SPARK_HOME/conf/spark-env.sh
cat >> \$SPARK_HOME/conf/spark-env.sh << EOL
export SPARK_DIST_CLASSPATH=\$(\$HADOOP_PREFIX/bin/hadoop --config \$HADOOP_CONF_DIR classpath)
EOL

sleep 60s

\$HADOOP_PREFIX/bin/hadoop fs -mkdir -p hdfs://\$HADOOP_NAMENODE:8020/tmp/spark-events

cores=4

cp \$HIBENCH_HOME/workloads/terasort/conf/10-terasort-userdefine.conf.template \$HIBENCH_HOME/workloads/terasort/conf/10-terasort-userdefine.conf
cat >> \$HIBENCH_HOME/workloads/terasort/conf/10-terasort-userdefine.conf << EOL
hibench.scale.profile tera
dfs.replication 1
mapred.submit.replication 1
mapreduce.client.submit.file.replication 1
hibench.default.map.parallelism \$((\$NUM_HADOOP_DATANODES * \$cores))
hibench.default.shuffle.parallelism \$((\$NUM_HADOOP_DATANODES * \$cores))
hibench.yarn.executor.num \$NUM_HADOOP_DATANODES
hibench.yarn.executor.memory 20G
hibench.yarn.executor.cores \$cores
hibench.yarn.driver.memory 8G

spark.driver.memory 8G
spark.executor.cores \$cores
spark.executor.memory 20G
spark.eventLog.enabled true
spark.eventLog.dir hdfs://\$HADOOP_NAMENODE:8020/tmp/spark-events
EOL

\$HIBENCH_HOME/bin/custom/reset_lustre_stats.sh
\$HIBENCH_HOME/workloads/terasort/prepare/prepare.sh
\$HIBENCH_HOME/bin/custom/dump_lustre_stats.sh
\$HIBENCH_HOME/bin/custom/reset_lustre_stats.sh
\$HADOOP_PREFIX/bin/hadoop fs -mkdir -p hdfs://\$HADOOP_NAMENODE:8020/HiBench/Terasort/Output
\$HIBENCH_HOME/workloads/terasort/spark/scala/bin/run-eastcircle.sh \
  hdfs://\$HADOOP_NAMENODE:8020 /HiBench/Terasort/Input /HiBench/Terasort/Output \
  768
#  \$((\$NUM_HADOOP_DATANODES * \$cores * 8))
#  768
\$HIBENCH_HOME/bin/custom/dump_lustre_stats.sh

# job history files are moved to the done folder every 180s
sleep 240s
\$HADOOP_PREFIX/bin/hadoop fs -copyToLocal hdfs://\$HADOOP_NAMENODE:8020/tmp/hadoop-yarn/staging/history/done \$HIBENCH_HOME/bin/custom/hibench-terasort.\$PBS_JOBID-history
\$HADOOP_PREFIX/bin/hadoop fs -copyToLocal hdfs://\$HADOOP_NAMENODE:8020/tmp/spark-events \$HIBENCH_HOME/bin/custom/hibench-terasort.\$PBS_JOBID-sparkhistory

$WORK2/HiBench/bin/custom/stop-hdfs-ssh.sh

echo "Stopping HiBench TeraSort"
date

EOF

chmod +x launch-$PBS_JOBID.sh
ccmrun ./launch-$PBS_JOBID.sh
rm launch-$PBS_JOBID.sh

rm -rf $WORK2/hadoop/hadoop-dist/target/hadoop-2.7.1/conf
rm -rf $WORK2/hadoop/hadoop-dist/target/hadoop-2.7.1/logs
rm -rf $WORK/hdfs
rm -rf $WORK2/hdfs

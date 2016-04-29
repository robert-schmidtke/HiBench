#!/bin/bash

#PBS -N hibench-terasort-ssd
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

echo "Starting HiBench TeraSort"
date

# $HOME/workspace/collectl-slurm/collectl_pbs.sh start

source $WORK/HiBench-ssd/bin/custom/env.sh

cp \$HIBENCH_HOME/conf/99-user_defined_properties.conf.template \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hadoop\.home/c\hibench.hadoop.home \$HADOOP_HOME" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
#sed -i "/^hibench\.spark\.home/c\hibench.spark.home \$SPARK_HOME" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hdfs\.master/c\hibench.hdfs.master hdfs://\$HADOOP_NAMENODE:8020" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^#hibench\.hadoop\.configure\.dir/c\hibench.hadoop.configure.dir \$HADOOP_CONF_DIR" \$HIBENCH_HOME/conf/99-user_defined_properties.conf

cat >> \$HIBENCH_HOME/conf/99-user_defined_properties.conf << EOL
hibench.report.dir \\\${hibench.home}/report-terasort-ssd.$PBS_JOBID
EOL

$WORK/HiBench-ssd/bin/custom/start-hdfs-ssh-ssd.sh 262144 1

# add Hadoop classpath to Spark after Hadoop is running
#cp \$SPARK_HOME/conf/spark-env.sh.template \$SPARK_HOME/conf/spark-env.sh
#cat >> \$SPARK_HOME/conf/spark-env.sh << EOL
#export SPARK_DIST_CLASSPATH=\$(\$HADOOP_PREFIX/bin/hadoop --config \$HADOOP_CONF_DIR classpath)
#EOL

sleep 60s

#\$HADOOP_PREFIX/bin/hadoop fs -mkdir -p hdfs://\$HADOOP_NAMENODE:8020/tmp/spark-events

cp \$HIBENCH_HOME/workloads/terasort/conf/10-terasort-userdefine.conf.template \$HIBENCH_HOME/workloads/terasort/conf/10-terasort-userdefine.conf
cat >> \$HIBENCH_HOME/workloads/terasort/conf/10-terasort-userdefine.conf << EOL
hibench.scale.profile fiveseventhtera
dfs.replication 1
mapred.submit.replication 1
mapreduce.client.submit.file.replication 1
#hibench.default.map.parallelism \$((\$NUM_HADOOP_DATANODES * 8))
hibench.default.map.parallelism \$((\$NUM_HADOOP_DATANODES * 60))
#hibench.default.shuffle.parallelism \$((\$NUM_HADOOP_DATANODES * 8))
hibench.default.shuffle.parallelism \$((\$NUM_HADOOP_DATANODES * 60))
hibench.yarn.executor.num \$NUM_HADOOP_DATANODES
hibench.yarn.executor.memory 16G
#hibench.yarn.executor.cores 8
hibench.yarn.executor.cores 2
hibench.yarn.driver.memory 8G

#spark.driver.memory 10G
#spark.executor.cores 4
#spark.executor.memory 2G
#spark.eventLog.enabled true
#spark.eventLog.dir hdfs://\$HADOOP_NAMENODE:8020/tmp/spark-events
EOL

\$HIBENCH_HOME/bin/custom/dump_dvs_stats.sh
\$HIBENCH_HOME/workloads/terasort/prepare/prepare.sh
\$HIBENCH_HOME/bin/custom/dump_dvs_stats.sh
\$HIBENCH_HOME/workloads/terasort/mapreduce/bin/run.sh
\$HIBENCH_HOME/bin/custom/dump_dvs_stats.sh
#\$HIBENCH_HOME/workloads/terasort/spark/scala/bin/run.sh
#\$HIBENCH_HOME/bin/custom/dump_dvs_stats.sh

# job history files are moved to the done folder every 180s
sleep 240s
\$HADOOP_PREFIX/bin/hadoop fs -copyToLocal hdfs://\$HADOOP_NAMENODE:8020/tmp/hadoop-yarn/staging/history/done \$HIBENCH_HOME/bin/custom/hibench-terasort-ssd.\$PBS_JOBID-history
#\$HADOOP_PREFIX/bin/hadoop fs -copyToLocal hdfs://\$HADOOP_NAMENODE:8020/tmp/spark-events \$HIBENCH_HOME/bin/custom/hibench-terasort-ssd.\$PBS_JOBID-sparkhistory

$WORK/HiBench-ssd/bin/custom/stop-hdfs-ssh.sh

# $HOME/workspace/collectl-slurm/collectl_pbs.sh stop -savelogs

echo "Stopping HiBench TeraSort"
date

EOF

chmod +x launch-$PBS_JOBID.sh
ccmrun ./launch-$PBS_JOBID.sh
rm launch-$PBS_JOBID.sh

rm -rf $WORK/hadoop/hadoop-dist/target/hadoop-2.7.1/conf
rm -rf $WORK/hadoop/hadoop-dist/target/hadoop-2.7.1/logs
rm -rf /flash/scratch4/$USER/hdfs

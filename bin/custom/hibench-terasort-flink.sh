#!/bin/bash

#PBS -N hibench-terasort
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

source $WORK2/HiBench/bin/custom/env.sh

cp \$HIBENCH_HOME/conf/99-user_defined_properties.conf.template \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hadoop\.home/c\hibench.hadoop.home \$HADOOP_HOME" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hdfs\.master/c\hibench.hdfs.master hdfs://\$HADOOP_NAMENODE:8020" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^#hibench\.hadoop\.configure\.dir/c\hibench.hadoop.configure.dir \$HADOOP_CONF_DIR" \$HIBENCH_HOME/conf/99-user_defined_properties.conf

cat >> \$HIBENCH_HOME/conf/99-user_defined_properties.conf << EOL
hibench.report.dir \\\${hibench.home}/report-terasort.$PBS_JOBID
EOL

$WORK2/HiBench/bin/custom/start-hdfs-ssh-lustre.sh 262144 1

cores=4
#parallelism=770

cp \$FLINK_HOME/conf/flink-conf.yaml.template \$FLINK_HOME/conf/flink-conf.yaml
sed -i "/^jobmanager\.rpc\.address/c\jobmanager.rpc.address: \$HADOOP_NAMENODE" \$FLINK_HOME/conf/flink-conf.yaml
sed -i "/^# fs\.hdfs\.hadoopconf/c\fs.hdfs.hadoopconf: \$HADOOP_CONF_DIR" \$FLINK_HOME/conf/flink-conf.yaml
sed -i "/^# taskmanager\.tmp\.dirs/c\taskmanager.tmp.dirs: /tmp/$USER/hadoop-tmp/nm-local-dir" \$FLINK_HOME/conf/flink-conf.yaml
#sed -i "/^# taskmanager\.network\.numberOfBuffers/c\taskmanager.network.numberOfBuffers: 131072" \$FLINK_HOME/conf/flink-conf.yaml

sleep 60s

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
#flink.taskmanager.num \$NUM_HADOOP_DATANODES
#flink.taskmanager.memory 20480
#flink.taskmanager.cores 4
#flink.jobmanager.memory 1792
EOL

\$HIBENCH_HOME/bin/custom/reset_lustre_stats.sh
\$HIBENCH_HOME/workloads/terasort/prepare/prepare.sh
\$HIBENCH_HOME/bin/custom/dump_lustre_stats.sh
\$HIBENCH_HOME/bin/custom/reset_lustre_stats.sh
\$HADOOP_PREFIX/bin/hadoop fs -mkdir -p hdfs://\$HADOOP_NAMENODE:8020/HiBench/Terasort/Output
\$FLINK_HOME/bin/flink run \
  -m yarn-cluster \
  -yn \$NUM_HADOOP_DATANODES \
  -ys \$cores \
  -p \$((\$NUM_HADOOP_DATANODES * \$cores)) \
  -yjm 1792 \
  -ytm 20480 \
  -c eastcircle.terasort.FlinkTeraSort \
  $WORK2/terasort/target/scala-2.10/terasort_2.10-0.0.1.jar \
  hdfs://\$HADOOP_NAMENODE:8020 /HiBench/Terasort/Input /HiBench/Terasort/Output \
  \$((\$NUM_HADOOP_DATANODES * \$cores))
#  -p \$((\$NUM_HADOOP_DATANODES * \$cores)) \
\$HIBENCH_HOME/bin/custom/dump_lustre_stats.sh

# job history files are moved to the done folder every 180s
sleep 240s
\$HADOOP_PREFIX/bin/hadoop fs -copyToLocal hdfs://\$HADOOP_NAMENODE:8020/tmp/hadoop-yarn/staging/history/done \$HIBENCH_HOME/bin/custom/hibench-terasort.\$PBS_JOBID-history

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

#!/bin/bash

#PBS -N hibench-join
#PBS -j oe
#PBS -l gres=ccm

source /opt/modules/default/init/bash
module load ccm java/jdk1.8.0_51
module unload atp # abnormal termination processing
cd $PBS_O_WORKDIR

cat > launch-$PBS_JOBID.sh << EOF
#!/bin/bash

module load java/jdk1.8.0_51

echo "Starting HiBench Join"
date

export NUM_KAFKA_NODES=0
export NUM_PRODUCER_NODES=0
source $WORK2/HiBench/bin/custom/env.sh

cp \$HIBENCH_HOME/conf/99-user_defined_properties.conf.template \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hadoop\.home/c\hibench.hadoop.home \$HADOOP_HOME" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hdfs\.master/c\hibench.hdfs.master hdfs://\$HADOOP_NAMENODE:8020" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^#hibench\.hadoop\.configure\.dir/c\hibench.hadoop.configure.dir \$HADOOP_CONF_DIR" \$HIBENCH_HOME/conf/99-user_defined_properties.conf

cat >> \$HIBENCH_HOME/conf/99-user_defined_properties.conf << EOL
hibench.report.dir \\\${hibench.home}/report-join.$PBS_JOBID
EOL

\$HIBENCH_HOME/bin/custom/start-hdfs-ssh-lustre.sh 262144 1

sleep 60s

cores=4

cp \$HIBENCH_HOME/workloads/join/conf/10-join-userdefine.conf.template \$HIBENCH_HOME/workloads/join/conf/10-join-userdefine.conf
cat >> \$HIBENCH_HOME/workloads/join/conf/10-join-userdefine.conf << EOL
hibench.scale.profile bigdata
dfs.replication 1
mapred.submit.replication 1
mapreduce.client.submit.file.replication 1
hibench.default.map.parallelism \$((\$NUM_HADOOP_DATANODES * \$cores))
hibench.default.shuffle.parallelism \$((\$NUM_HADOOP_DATANODES * \$cores))
hibench.yarn.executor.num \$NUM_HADOOP_DATANODES
hibench.yarn.executor.memory 20G
hibench.yarn.executor.cores \$cores
hibench.yarn.driver.memory 8G
EOL

\$HIBENCH_HOME/bin/custom/reset_lustre_stats.sh
\$HIBENCH_HOME/workloads/join/prepare/prepare.sh
\$HIBENCH_HOME/bin/custom/dump_lustre_stats.sh
\$HIBENCH_HOME/bin/custom/reset_lustre_stats.sh
\$HIBENCH_HOME/workloads/join/mapreduce/bin/run.sh
\$HIBENCH_HOME/bin/custom/dump_lustre_stats.sh

#echo "Printing result file list"
#result_files=\$(\$HADOOP_PREFIX/bin/hadoop fs -ls -R hdfs://\$HADOOP_NAMENODE:8020/HiBench/Join/Output/rankings_uservisits_join 2> /dev/null)
#echo "\$result_files"
#result_files=(\$result_files)

#i=7
#while [ \$i -lt \${#result_files[@]} ]; do
#  echo "Printing top 10 results for \${result_files[\$i]}"
#  \$HADOOP_PREFIX/bin/hadoop fs -cat \${result_files[\$i]} | head -n10
#  i=\$((\$i + 8))
#done

# job history files are moved to the done folder every 180s
sleep 240s
\$HADOOP_PREFIX/bin/hadoop fs -copyToLocal hdfs://\$HADOOP_NAMENODE:8020/tmp/hadoop-yarn/staging/history/done \$HIBENCH_HOME/bin/custom/hibench-join.\$PBS_JOBID-history

\$HIBENCH_HOME/bin/custom/stop-hdfs-ssh-lustre.sh

echo "Stopping HiBench Join"
date

EOF

chmod +x launch-$PBS_JOBID.sh
ccmrun ./launch-$PBS_JOBID.sh
rm launch-$PBS_JOBID.sh

rm -rf $WORK2/hadoop/hadoop-dist/target/hadoop-2.7.1/conf
rm -rf $WORK2/hadoop/hadoop-dist/target/hadoop-2.7.1/logs
rm -rf $WORK/hdfs
rm -rf $WORK2/hdfs

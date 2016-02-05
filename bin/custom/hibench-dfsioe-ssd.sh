#!/bin/bash

#PBS -N hibench-dfsioe-ssd
#PBS -l walltime=05:00:00
#PBS -l nodes=16:ppn=1
#PBS -j oe
#PBS -l gres=ccm

source /opt/modules/default/init/bash
module load ccm #java/jdk1.8.0_51
module unload atp # abnormal termination processing
cd $PBS_O_WORKDIR

cat > launch.sh << EOF
#!/bin/bash

source $HOME/workspace/HiBench/bin/custom/env.sh

cp \$HIBENCH_HOME/conf/99-user_defined_properties.conf.template \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hadoop\.home/c\hibench.hadoop.home \$HADOOP_HOME" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.spark\.home/c\hibench.spark.home \$SPARK_HOME" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^hibench\.hdfs\.master/c\hibench.hdfs.master hdfs://\$HADOOP_NAMENODE:8020" \$HIBENCH_HOME/conf/99-user_defined_properties.conf
sed -i "/^#hibench\.hadoop\.configure\.dir/c\hibench.hadoop.configure.dir \$HADOOP_CONF_DIR" \$HIBENCH_HOME/conf/99-user_defined_properties.conf

$HOME/workspace/HiBench/bin/custom/start-hdfs-ssh-ssd.sh 524288 1

# add Hadoop classpath to Spark after Hadoop is running
cp \$SPARK_HOME/conf/spark-env.sh.template \$SPARK_HOME/conf/spark-env.sh
cat >> \$SPARK_HOME/conf/spark-env.sh << EOL
export SPARK_DIST_CLASSPATH=\$(\$HADOOP_PREFIX/bin/hadoop --config \$HADOOP_CONF_DIR classpath)
EOL

sleep 60s

cp \$HIBENCH_HOME/workloads/dfsioe/conf/10-dfsioe-userdefine.conf.template \$HIBENCH_HOME/workloads/dfsioe/conf/10-dfsioe-userdefine.conf
cat >> \$HIBENCH_HOME/workloads/dfsioe/conf/10-dfsioe-userdefine.conf << EOL
hibench.scale.profile huge
dfs.replication 1
mapred.submit.replication 1
mapreduce.client.submit.file.replication 1
hibench.default.map.parallelism \$((\$NUM_HADOOP_DATANODES * 2))
hibench.default.shuffle.parallelism \$((\$NUM_HADOOP_DATANODES * 2))
EOL

\$HIBENCH_HOME/workloads/dfsioe/prepare/prepare.sh
\$HIBENCH_HOME/workloads/dfsioe/mapreduce/bin/run.sh

$HOME/workspace/HiBench/bin/custom/stop-hdfs-ssh.sh
EOF

chmod +x launch.sh
ccmrun ./launch.sh
rm launch.sh
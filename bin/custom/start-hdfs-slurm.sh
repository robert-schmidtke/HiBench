#!/bin/bash

USAGE="Usage: srun --nodes=1-1 --nodelist=<NAMENODE> start-hdfs-slurm.sh [HDFS_BLOCKSIZE] [HDFS_REPLICATION]"

if [ -z $SLURM_JOB_ID ]; then
  echo "No Slurm environment detected. $USAGE"
  exit 1
fi

export HDFS_BLOCKSIZE="$1k"
if [ "k" == "$HDFS_BLOCKSIZE" ]; then
  echo "No HDFS block size given, defaulting to 131072k."
  export HDFS_BLOCKSIZE="131072k"
fi

export HDFS_REPLICATION=$2
if [ -z $HDFS_REPLICATION ]; then
  echo "No HDFS replication given, defaulting to 3."
  export HDFS_REPLICATION=3
fi

# HADOOP_NODES
# NUM_HADOOP_NODES
# HADOOP_NAMENODE
# HADOOP_DATANODES
# HADOOP_PREFIX
# HADOOP_CONF_DIR
source $(dirname $0)/env-slurm.sh

echo "Using Hadoop Distribution in '$HADOOP_PREFIX'."

echo "Starting Hadoop NameNode on '$HADOOP_NAMENODE' and DataNode(s) on '${HADOOP_DATANODES[@]}'."

mkdir -p $HADOOP_CONF_DIR
cp $HADOOP_PREFIX/etc/hadoop/* $HADOOP_CONF_DIR
echo "Creating configuration in '$HADOOP_CONF_DIR'."

printf "%s\n" "${HADOOP_DATANODES[@]}" > "${HADOOP_CONF_DIR}/slaves"

# core configuration
cat > $HADOOP_CONF_DIR/core-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://$HADOOP_NAMENODE:8020</value>
  </property>
  <property>
    <name>io.file.buffer.size</name>
    <value>65536</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/local/${HDFS_LOCAL_DIR}-tmp</value>
  </property>
</configuration>
EOF

# name node configuration
mkdir -p $HADOOP_CONF_DIR/$HADOOP_NAMENODE
export HADOOP_NAMENODE_HDFS_SITE=$HADOOP_CONF_DIR/$HADOOP_NAMENODE/hdfs-site.xml
cp $HADOOP_CONF_DIR/hdfs-site.xml $HADOOP_NAMENODE_HDFS_SITE

# cut off closing configuration tag
line_number=`grep -nr "</configuration>" "$HADOOP_NAMENODE_HDFS_SITE" | cut -d : -f 1`
printf '%s\n' "${line_number}s#.*##" w  | ed -s "$HADOOP_NAMENODE_HDFS_SITE"

cat >> $HADOOP_NAMENODE_HDFS_SITE << EOF
  <property>
    <name>dfs.namenode.rpc-address</name>
    <value>$HADOOP_NAMENODE:8020</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///local/$HDFS_LOCAL_DIR/name</value>
  </property>
  <property>
    <name>dfs.blocksize</name>
    <value>$HDFS_BLOCKSIZE</value>
  </property>
  <property>
    <name>dfs.replication</name>
    <value>$HDFS_REPLICATION</value>
  </property>
</configuration>
EOF

cp $HADOOP_NAMENODE_HDFS_SITE $HADOOP_CONF_DIR/hdfs-site.xml

# data node configurations
for datanode in ${HADOOP_DATANODES[@]}; do
  mkdir -p $HADOOP_CONF_DIR/$datanode
  hadoop_datanode_hdfs_site=$HADOOP_CONF_DIR/$datanode/hdfs-site.xml
  cp $HADOOP_CONF_DIR/hdfs-site.xml $hadoop_datanode_hdfs_site

  line_number=`grep -nr "</configuration>" "$hadoop_datanode_hdfs_site" | cut -d : -f 1`
  printf '%s\n' "${line_number}s#.*##" w  | ed -s "$hadoop_datanode_hdfs_site"

  cat >> $hadoop_datanode_hdfs_site << EOF
  <property>
    <name>dfs.datanode.address</name>
    <value>$datanode:50010</value>
  </property>
  <property>
    <name>dfs.datanode.ipc.address</name>
    <value>$datanode:50020</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///local/$HDFS_LOCAL_DIR/data</value>
  </property>
</configuration>
EOF
done

# yarn configuration
cat > $HADOOP_CONF_DIR/mapred-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>mapreduce.jobhistory.address</name>
    <value>$HADOOP_NAMENODE:10020</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>$HADOOP_NAMENODE:19888</value>
  </property>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.client.submit.file.replication</name>
    <value>$HDFS_REPLICATION</value>
  </property>
  <property>
    <name>mapreduce.map.memory.mb</name>
    <value>3072</value>
  </property>
  <property>
    <name>mapreduce.map.java.opts</name>
    <value>-Xmx2048M</value>
  </property>
  <property>
    <name>mapreduce.reduce.memory.mb</name>
    <value>4096</value>
  </property>
  <property>
    <name>mapreduce.reduce.java.opts</name>
    <value>-Xmx3072M</value>
  </property>
  <property>
    <name>mapreduce.task.io.sort.mb</name>
    <value>1024</value>
  </property>
  <property>
    <name>mapreduce.task.io.sort.factor</name>
    <value>32</value>
  </property>
</configuration>
EOF

NODE_MEMORY=$(srun --nodes=1-1 --nodelist=$HADOOP_NAMENODE awk '/MemTotal/ {printf( "%.2d\n", ($2 / 1024) * 0.7 )}' /proc/meminfo)
cat > $HADOOP_CONF_DIR/yarn-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>$NODE_MEMORY</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>$NODE_MEMORY</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>$HADOOP_NAMENODE</value>
  </property>
</configuration>
EOF

cp $HADOOP_CONF_DIR/yarn-site.xml $HADOOP_CONF_DIR/$HADOOP_NAMENODE/yarn-site.xml

for datanode in ${HADOOP_DATANODES[@]}; do
  datanode_yarn_site=$HADOOP_CONF_DIR/$datanode/yarn-site.xml
  cp $HADOOP_CONF_DIR/yarn-site.xml $datanode_yarn_site

  line_number=`grep -nr "</configuration>" "$datanode_yarn_site" | cut -d : -f 1`
  printf '%s\n' "${line_number}s#.*##" w | ed -s "$datanode_yarn_site"

  cat >> $datanode_yarn_site << EOF
  <property>
    <name>yarn.nodemanager.hostname</name>
    <value>$datanode</value>
  </property>
</configuration>
EOF

done

# start name node
mkdir -p /local/$HDFS_LOCAL_DIR
mkdir -p /local/${HDFS_LOCAL_DIR}-tmp
mkdir -p /local/$HDFS_LOCAL_LOG_DIR

export HADOOP_USER_CLASSPATH_FIRST="YES"
export HADOOP_CLASSPATH="$HADOOP_CONF_DIR/$HADOOP_NAMENODE:$HADOOP_CLASSPATH"

export HDFS_NAMENODE_LOG=/local/$HDFS_LOCAL_LOG_DIR/namenode-$(hostname).log

echo "Formatting NameNode."
ulimit -c unlimited
# the scripts asks for confirmation
$HADOOP_PREFIX/bin/hdfs --config $HADOOP_CONF_DIR namenode -format >> $HDFS_NAMENODE_LOG 2>&1 << EOF
Y
EOF
echo "Formatting NameNode done."

export HADOOP_HEAPSIZE=$(srun --nodes=1-1 --nodelist=$HADOOP_NAMENODE awk '/MemTotal/ {printf( "%.2d\n", ($2 / 1024) * 0.20 )}' /proc/meminfo)

echo "Starting NameNode on $(hostname)."
nohup $HADOOP_PREFIX/bin/hdfs --config $HADOOP_CONF_DIR namenode >> $HDFS_NAMENODE_LOG 2>&1 &
echo $! > /local/$HDFS_LOCAL_DIR/namenode-$(hostname).pid
echo "Starting NameNode done (PID file: /local/$HDFS_LOCAL_DIR/namenode-$(hostname).pid)."

for datanode in ${HADOOP_DATANODES[@]}; do
  datanode_script=$(dirname $0)/${SLURM_JOB_ID}-${datanode}-start-datanode.sh
  cat > $datanode_script << EOF
#!/bin/bash
source \$(dirname \$0)/env-slurm.sh

# same as for namenode
mkdir -p /local/$HDFS_LOCAL_DIR
mkdir -p /local/${HDFS_LOCAL_DIR}-tmp
mkdir -p /local/$HDFS_LOCAL_LOG_DIR

export HADOOP_USER_CLASSPATH_FIRST="YES"
export HADOOP_CLASSPATH="$HADOOP_CONF_DIR/$datanode:$HADOOP_CLASSPATH"
export HDFS_DATANODE_LOG=/local/$HDFS_LOCAL_LOG_DIR/datanode-$datanode.log
#export JSTAT_LOG=/local/$HDFS_LOCAL_LOG_DIR/datanode-jstat-$datanode.log

nohup $HADOOP_PREFIX/bin/hdfs --config $HADOOP_CONF_DIR datanode >> \$HDFS_DATANODE_LOG 2>&1 &
pid=\$!
echo \$pid > /local/$HDFS_LOCAL_DIR/datanode-$datanode.pid
#nohup jstat -gcutil \$pid 5000 >> \$JSTAT_LOG 2>&1 &
#echo \$! > /local/$HDFS_LOCAL_DIR/datanode-jstat-$datanode.pid
EOF
  chmod +x $datanode_script
  echo "Starting DataNode on $datanode."
  srun --nodes=1-1 --nodelist=$datanode $datanode_script
  echo "Starting DataNode on $datanode done."
  rm $datanode_script
done

# start resource manager
export YARN_USER_CLASSPATH="$YARN_USER_CLASSPATH:$HADOOP_CONF_DIR/$(hostname)"

export YARN_RESOURCEMANAGER_LOG=/local/$HDFS_LOCAL_LOG_DIR/resourcemanager-$(hostname).log

export YARN_HEAPSIZE=$HADOOP_HEAPSIZE

echo "Starting ResourceManager on $(hostname)."
nohup $HADOOP_PREFIX/bin/yarn --config $HADOOP_CONF_DIR resourcemanager >> $YARN_RESOURCEMANAGER_LOG 2>&1 &
echo $! > /local/$HDFS_LOCAL_DIR/resourcemanager-$(hostname).pid
echo "Starting ResourceManager done (PID file /local/$HDFS_LOCAL_DIR/resourcemanager-$(hostname).pid)."

for datanode in ${HADOOP_DATANODES[@]}; do
nodemanager_script=$(dirname $0)/${SLURM_JOB_ID}-${datanode}-start-nodemanager.sh
  cat > $nodemanager_script << EOF
#!/bin/bash
source \$(dirname \$0)/env-slurm.sh

# same as for resource manager
export YARN_USER_CLASSPATH="$YARN_USER_CLASSPATH:$HADOOP_CONF_DIR/$datanode"
export YARN_NODEMANAGER_LOG=/local/$HDFS_LOCAL_LOG_DIR/nodemanager-$datanode.log
#export JSTAT_LOG=/local/$HDFS_LOCAL_LOG_DIR/nodemanager-jstat-$datanode.log

nohup $HADOOP_PREFIX/bin/yarn --config $HADOOP_CONF_DIR nodemanager >> \$YARN_NODEMANAGER_LOG 2>&1 &
pid=\$!
echo \$pid > /local/$HDFS_LOCAL_DIR/nodemanager-$datanode.pid
#nohup jstat -gcutil \$pid 5000 >> \$JSTAT_LOG 2>&1 &
#echo \$! > /local/$HDFS_LOCAL_DIR/nodemanager-jstat-$datanode.pid
EOF
  chmod +x $nodemanager_script
  echo "Starting NodeManager on $datanode."
  srun --nodes=1-1 --nodelist=$datanode $nodemanager_script
  echo "Starting NodeManager on $datanode done."
  rm $nodemanager_script
done

export JOBHISTORY_SERVER_LOG=/local/$HDFS_LOCAL_LOG_DIR/jobhistory_server-$(hostname).log

echo "Starting JobHistory Server on $(hostname)."
nohup $HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR start historyserver >> $JOBHISTORY_SERVER_LOG 2>&1 &
echo $! > /local/$HDFS_LOCAL_DIR/jobhistory_server-$(hostname).pid
echo "Starting JobHistory Server done (PID file /local/$HDFS_LOCAL_DIR/jobhistory_server-$(hostname).pid)."

echo "Starting Hadoop done."

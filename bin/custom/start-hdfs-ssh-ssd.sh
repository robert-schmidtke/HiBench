#!/bin/bash

USAGE="Usage: start-hdfs-ssh.sh [HDFS_BLOCKSIZE] [HDFS_REPLICATION]"

if [ -z $PBS_JOBID ]; then
  echo "No PBS environment detected. $USAGE"
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
source $(dirname $0)/env.sh

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
    <value>/tmp/$USER/hadoop-tmp</value>
  </property>
</configuration>
EOF

# name node configuration
mkdir -p $HADOOP_CONF_DIR/$HADOOP_NAMENODE
mkdir -p $SCRATCH/$HDFS_LOCAL_DIR/name-$HADOOP_NAMENODE
mkdir -p $SCRATCH/$HDFS_LOCAL_DIR/name-$HADOOP_NAMENODE-tmp
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
    <value>file://$SCRATCH/$HDFS_LOCAL_DIR/name-$HADOOP_NAMENODE</value>
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
  mkdir -p $SCRATCH/$HDFS_LOCAL_DIR/data-$datanode
  mkdir -p $SCRATCH/$HDFS_LOCAL_DIR/data-$datanode-tmp
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
    <value>file://$SCRATCH/$HDFS_LOCAL_DIR/data-$datanode</value>
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
<!--  <property>
    <name>mapreduce.task.profile</name>
    <value>true</value>
  </property> -->
</configuration>
EOF

NODE_MEMORY=$(awk '/MemTotal/ {printf( "%.2d\n", ($2 / 1024) * 0.7 )}' /proc/meminfo)
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
<!--  <property>
    <name>yarn.nodemanager.sleep-delay-before-sigkill.ms</name>
    <value>30000</value>
  </property>
  <property>
    <name>mapreduce.task.profile.params</name>
    <value>-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,verbose=y,file=%s</value>
  </property> -->
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
mkdir -p $SCRATCH/$HDFS_LOCAL_DIR
mkdir -p $SCRATCH/$HDFS_LOCAL_LOG_DIR

mkdir -p /tmp/$USER
rm -rf /tmp/$USER/hadoop-tmp
ln -s $SCRATCH/$HDFS_LOCAL_DIR/name-$HADOOP_NAMENODE-tmp /tmp/$USER/hadoop-tmp

for datanode in ${HADOOP_DATANODES[@]}; do
  echo -n "Creating symlink on $datanode ..."
  ssh $datanode "mkdir -p /tmp/$USER"
  ssh $datanode "rm -rf /tmp/$USER/hadoop-tmp"
  ssh $datanode "ln -s $SCRATCH/$HDFS_LOCAL_DIR/data-$datanode-tmp /tmp/$USER/hadoop-tmp"
  echo "done"
done

export HADOOP_USER_CLASSPATH_FIRST="YES"
export HADOOP_CLASSPATH="$HADOOP_CONF_DIR/$HADOOP_NAMENODE:$HADOOP_CLASSPATH"

export HDFS_NAMENODE_LOG=$SCRATCH/$HDFS_LOCAL_LOG_DIR/namenode-$(hostname).log

echo "Formatting NameNode."
ulimit -c unlimited
# the scripts asks for confirmation
$HADOOP_PREFIX/bin/hdfs --config $HADOOP_CONF_DIR namenode -format >> $HDFS_NAMENODE_LOG 2>&1 << EOF
Y
EOF
echo "Formatting NameNode done."

export HADOOP_HEAPSIZE=$(awk '/MemTotal/ {printf( "%.2d\n", ($2 / 1024) * 0.20 )}' /proc/meminfo)

$HADOOP_PREFIX/sbin/start-dfs.sh --config $HADOOP_CONF_DIR

# start resource manager
export YARN_USER_CLASSPATH="$YARN_USER_CLASSPATH:$HADOOP_CONF_DIR/$(hostname)"

export YARN_RESOURCEMANAGER_LOG=$SCRATCH/$HDFS_LOCAL_LOG_DIR/resourcemanager-$(hostname).log

export YARN_HEAPSIZE=$HADOOP_HEAPSIZE

$HADOOP_PREFIX/sbin/start-yarn.sh --config $HADOOP_CONF_DIR

$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR start historyserver

echo "Starting Hadoop done."

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
    <value>1048576</value>
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
    <value>file:///flash/scratch1/$HDFS_LOCAL_DIR/name-$HADOOP_NAMENODE</value>
  </property>
  <property>
    <name>dfs.blocksize</name>
    <value>$HDFS_BLOCKSIZE</value>
  </property>
  <property>
    <name>dfs.replication</name>
    <value>$HDFS_REPLICATION</value>
  </property>
  <property>
    <name>dfs.replication.max</name>
    <value>$HDFS_REPLICATION</value>
  </property>
</configuration>
EOF

cp $HADOOP_NAMENODE_HDFS_SITE $HADOOP_CONF_DIR/hdfs-site.xml

# data node configurations
for datanode in ${HADOOP_DATANODES[@]}; do
 # find out which node this is in the list of all nodes
  HADOOP_NODE_INDEX=-1
  for (( i=0;i<$NUM_HADOOP_NODES;i++ )); do
    if [ "$datanode" = "${HADOOP_NODES[i]}" ]; then
      export HADOOP_NODE_INDEX=$i
      break
    fi
  done
  if [ "-1" -eq "$HADOOP_NODE_INDEX" ]; then
    echo "Could not determine index of current node ($datanode) in the list of nodes (${HADOOP_NODES[@]})."
    echo "---"
    cat $PBS_NODEFILE
    echo "---"
  fi
  # index between 1 and 8 because there are 8 SSDs
  let "ssd_index=${HADOOP_NODE_INDEX}%${NUM_SSDS}+1" 

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
    <value>file:///flash/scratch${ssd_index}/$HDFS_LOCAL_DIR/data-$datanode</value>
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
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.client.submit.file.replication</name>
    <value>$HDFS_REPLICATION</value>
  </property>
</configuration>
EOF

NODE_MEMORY=$(awk '/MemTotal/ {printf( "%.2d\n", ($2 / 1024) * 0.4 )}' /proc/meminfo)
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
mkdir -p /flash/scratch1/$HDFS_LOCAL_DIR
mkdir -p /flash/scratch1/$HDFS_LOCAL_LOG_DIR

export HADOOP_USER_CLASSPATH_FIRST="YES"
export HADOOP_CLASSPATH="$HADOOP_CONF_DIR/$HADOOP_NAMENODE:$HADOOP_CLASSPATH"

export HDFS_NAMENODE_LOG=/flash/scratch1/$HDFS_LOCAL_LOG_DIR/namenode-$(hostname).log

echo "Formatting NameNode."
ulimit -c unlimited
# the scripts asks for confirmation
$HADOOP_PREFIX/bin/hdfs --config $HADOOP_CONF_DIR namenode -format >> $HDFS_NAMENODE_LOG 2>&1 << EOF
Y
EOF
echo "Formatting NameNode done."

export HADOOP_HEAPSIZE=$(awk '/MemTotal/ {printf( "%.2d\n", ($2 / 1024) * 0.05 )}' /proc/meminfo)

$HADOOP_PREFIX/sbin/start-dfs.sh --config $HADOOP_CONF_DIR

# start resource manager
export YARN_USER_CLASSPATH="$YARN_USER_CLASSPATH:$HADOOP_CONF_DIR/$(hostname)"

export YARN_RESOURCEMANAGER_LOG=/flash/scratch1/$HDFS_LOCAL_LOG_DIR/resourcemanager-$(hostname).log

export YARN_HEAPSIZE=$HADOOP_HEAPSIZE

$HADOOP_PREFIX/sbin/start-yarn.sh --config $HADOOP_CONF_DIR

echo "Starting Hadoop done."

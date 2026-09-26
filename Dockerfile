# ============================================================================
# Hadoop + Spark + PySpark Base Image
# Supports: NameNode, DataNode, ResourceManager, NodeManager, Spark Master/Worker
# ============================================================================
FROM ubuntu:22.04

LABEL maintainer="BDATLProject" \
      description="Hadoop 3.3.6 + Spark 3.5.1 + PySpark distributed node"

# ---------- Environment Variables ----------
ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_VERSION=3.3.6
ENV SPARK_VERSION=3.5.1
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HBASE_VERSION=2.6.3
ENV HBASE_HOME=/opt/hbase
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV YARN_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${HBASE_HOME}/bin
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

# ---------- System Dependencies ----------
RUN echo 'Acquire::ForceIPv4 "true";' > /etc/apt/apt.conf.d/99force-ipv4 \
    && apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless \
    python3 python3-pip python3-venv \
    curl wget ssh rsync procps net-tools \
    && rm -rf /var/lib/apt/lists/*

# Alias python
RUN ln -sf /usr/bin/python3 /usr/bin/python

# ---------- SSH (passwordless for Hadoop daemons) ----------
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys \
    && chmod 600 ~/.ssh/authorized_keys
RUN echo "Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null" > ~/.ssh/config

# ---------- Download & Install Hadoop ----------
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ \
    && mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

# ---------- Download & Install Spark ----------
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# ---------- Install HBase ----------
RUN wget -q https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz \
    && tar -xzf hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz -C /opt/ \
    && mv /opt/hbase-${HBASE_VERSION}-hadoop3 ${HBASE_HOME} \
    && rm hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz

RUN cat > ${HBASE_HOME}/conf/hbase-site.xml <<'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <value>hdfs://master:9000/hbase</value>
  </property>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
  </property>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>master</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.clientPort</name>
    <value>2181</value>
  </property>
</configuration>
EOF

# ---------- Python Dependencies ----------
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# ---------- Hadoop Configuration ----------
COPY config/core-site.xml     ${HADOOP_CONF_DIR}/core-site.xml
COPY config/hdfs-site.xml     ${HADOOP_CONF_DIR}/hdfs-site.xml
COPY config/yarn-site.xml     ${HADOOP_CONF_DIR}/yarn-site.xml
COPY config/mapred-site.xml   ${HADOOP_CONF_DIR}/mapred-site.xml

# ---------- Spark Configuration ----------
COPY config/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

# ---------- Project Code ----------
RUN mkdir -p /app
COPY src/ /app/src/
COPY run_pipeline.sh /app/run_pipeline.sh
RUN sed -i 's/\r$//' /app/run_pipeline.sh && chmod +x /app/run_pipeline.sh

# ---------- Entrypoint ----------
COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

# HDFS ports: 9870 (NameNode UI), 9000 (HDFS IPC)
# YARN ports: 8088 (ResourceManager UI)
# Spark ports: 8080 (Master UI), 7077 (Master), 4040 (App UI)
# Streamlit: 8501
EXPOSE 9870 9000 8088 8080 7077 4040 8501

ENTRYPOINT ["/entrypoint.sh"]

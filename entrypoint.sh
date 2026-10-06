#!/bin/bash
# ============================================================================
# Entrypoint for Hadoop/Spark Docker containers
# Role is determined by the NODE_ROLE environment variable: master | slave
# ============================================================================

echo "============================================"
echo " Node Role : ${NODE_ROLE:-master}"
echo " Hostname  : $(hostname)"
echo "============================================"

# Start SSH daemon (required for Hadoop inter-node communication)
service ssh start || true

if [ "${NODE_ROLE}" = "master" ]; then
    echo "[MASTER] Formatting HDFS NameNode (if not already formatted)..."
    if [ ! -d "/hadoop/dfs/name/current" ]; then
        hdfs namenode -format -force -nonInteractive || true
    fi

    echo "[MASTER] Starting HDFS NameNode..."
    hdfs --daemon start namenode || true

    echo "[MASTER] Starting Spark Master..."
    ${SPARK_HOME}/sbin/start-master.sh || true

    echo "[MASTER] Starting MapReduce JobHistory Server..."
    mapred --daemon start historyserver || true

    echo "[MASTER] All NameNode/Spark services started."

    # Leave Safe Mode immediately (do not block)
    echo "[MASTER] Ensuring HDFS leaves Safe Mode..."
    sleep 5
    hdfs dfsadmin -safemode leave || true

    # Prepare HDFS directories
    echo "[MASTER] Ensuring HDFS directory structure exists..."
    hdfs dfs -mkdir -p /spark-logs /data/raw/prices /data/raw/news /data/processed/sentiment /data/processed/anomalies /data/processed/momentum /tmp/hadoop-yarn/staging || true
    hdfs dfs -chmod -R 777 /spark-logs /data /tmp/hadoop-yarn 2>/dev/null || true

    # Start YARN ResourceManager
    echo "[MASTER] Starting YARN ResourceManager..."
    mkdir -p /tmp/hadoop-yarn-nodeattr || true
    yarn --daemon start resourcemanager || true
    echo "[MASTER] ResourceManager started."

    echo "[MASTER] Starting Streamlit Dashboard on port 8501..."
    cd /app
    pkill -f streamlit || true
    nohup streamlit run src/dashboard/dashboard.py \
        --server.port=8501 \
        --server.address=0.0.0.0 \
        --server.headless=true \
        > /tmp/streamlit.log 2>&1 &

    echo "[MASTER] Streamlit Dashboard launched in background."

    # Only run pipeline if processed data is completely missing
    if ! hdfs dfs -test -e /data/processed/momentum/_SUCCESS 2>/dev/null; then
        echo "[MASTER] Launching data pipeline in background..."
        : > /tmp/pipeline.log
        nohup bash /app/run_pipeline.sh >> /tmp/pipeline.log 2>&1 &
    else
        echo "[MASTER] Processed data already exists in HDFS. Skipping auto-pipeline."
    fi

else
    echo "[SLAVE] Starting HDFS DataNode..."
    hdfs --daemon start datanode || true

    echo "[SLAVE] Starting YARN NodeManager..."
    yarn --daemon start nodemanager || true

    echo "[SLAVE] Starting Spark Worker..."
    ${SPARK_HOME}/sbin/start-worker.sh spark://master:7077 || true

    echo "[SLAVE] All services started."
fi

echo "============================================"
echo " Container ready. Staying alive..."
echo "============================================"

# Eternal keepalive - never exits
exec tail -f /dev/null

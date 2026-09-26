#!/bin/bash
# ============================================================================
# Entrypoint for Hadoop/Spark Docker containers
# Role is determined by the NODE_ROLE environment variable: master | slave
# ============================================================================
set -e

echo "============================================"
echo " Node Role : ${NODE_ROLE:-master}"
echo " Hostname  : $(hostname)"
echo "============================================"

# Start SSH daemon (required for Hadoop inter-node communication)
service ssh start

if [ "${NODE_ROLE}" = "master" ]; then
    echo "[MASTER] Formatting HDFS NameNode (if not already formatted)..."
    if [ ! -d "/hadoop/dfs/name/current" ]; then
        hdfs namenode -format -force -nonInteractive
    fi

    echo "[MASTER] Starting HDFS NameNode..."
    hdfs --daemon start namenode

    echo "[MASTER] Starting Spark Master..."
    ${SPARK_HOME}/sbin/start-master.sh

    echo "[MASTER] Starting MapReduce JobHistory Server..."
    mapred --daemon start historyserver

    echo "[MASTER] All NameNode/Spark services started."

    # Wait for DataNodes to register and leave Safe Mode before any write operations
    echo "[MASTER] Waiting for DataNodes to register and HDFS to exit Safe Mode..."
    sleep 5
    hdfs dfsadmin -safemode wait || true
    hdfs dfsadmin -safemode leave || true
    hdfs dfsadmin -report || true

    # Prepare HDFS directories
    echo "[MASTER] Ensuring HDFS directory structure exists..."
    hdfs dfs -mkdir -p /spark-logs /data/raw/prices /data/raw/news /data/processed/sentiment /data/processed/anomalies /data/processed/momentum /tmp/hadoop-yarn/staging || true
    # Retry chmod in case HDFS is still leaving Safe Mode
    for _i in 1 2 3 4 5; do
        hdfs dfs -chmod -R 777 /spark-logs /data /tmp/hadoop-yarn 2>/dev/null && break || echo "[MASTER] chmod attempt ${_i} failed (Safe Mode?), retrying in 5s..."
        sleep 5
    done || true

    # Now start YARN ResourceManager (after HDFS is writable)
    echo "[MASTER] Starting YARN ResourceManager..."
    mkdir -p /tmp/hadoop-yarn-nodeattr
    yarn --daemon start resourcemanager
    echo "[MASTER] ResourceManager started."

    # Upload local seed data to HDFS if available and not already loaded
    if [ -d "/app/data/raw/prices" ] && [ $(ls -1 /app/data/raw/prices/*.csv 2>/dev/null | wc -l) -gt 0 ]; then
        PRICE_COUNT=$(hdfs dfs -ls /data/raw/prices 2>/dev/null | grep -c "\.csv" || true)
        if [ "$PRICE_COUNT" -eq 0 ]; then
            echo "[MASTER] Loading seed price CSVs into HDFS..."
            hdfs dfs -put -f /app/data/raw/prices/*.csv /data/raw/prices/ || true
        else
            echo "[MASTER] Price data already present in HDFS ($PRICE_COUNT files), skipping seed upload."
        fi
    fi
    if [ -d "/app/data/raw/news" ] && [ $(ls -1 /app/data/raw/news/*.csv 2>/dev/null | wc -l) -gt 0 ]; then
        NEWS_COUNT=$(hdfs dfs -ls /data/raw/news 2>/dev/null | grep -c "\.csv" || true)
        if [ "$NEWS_COUNT" -eq 0 ]; then
            echo "[MASTER] Loading seed news CSVs into HDFS..."
            hdfs dfs -put -f /app/data/raw/news/*.csv /data/raw/news/ || true
        else
            echo "[MASTER] News data already present in HDFS ($NEWS_COUNT files), skipping seed upload."
        fi
    fi

    echo "[MASTER] Starting Streamlit Dashboard on port 8501..."
    cd /app
    nohup streamlit run src/dashboard/dashboard.py \
        --server.port=8501 \
        --server.address=0.0.0.0 \
        --server.headless=true \
        > /tmp/streamlit.log 2>&1 &

    # Auto-run the data pipeline in background
    # pipeline.log is created/appended by run_pipeline.sh itself via tee -a
    echo "[MASTER] Launching data pipeline in background..."
    : > /tmp/pipeline.log  # truncate/create the log file cleanly
    nohup bash /app/run_pipeline.sh >> /tmp/pipeline.log 2>&1 &

else
    echo "[SLAVE] Starting HDFS DataNode..."
    hdfs --daemon start datanode

    echo "[SLAVE] Starting YARN NodeManager..."
    yarn --daemon start nodemanager

    echo "[SLAVE] Starting Spark Worker..."
    ${SPARK_HOME}/sbin/start-worker.sh spark://master:7077

    echo "[SLAVE] All services started."
fi

echo "============================================"
echo " Container ready. Staying alive..."
echo "============================================"

# Keep container alive indefinitely
while true; do
    sleep 3600
done

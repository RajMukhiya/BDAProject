#!/bin/bash
# ============================================================================
# run_pipeline.sh — Master Pipeline Orchestrator with Modular Modes
# ============================================================================
# Modes:
#   bash /app/run_pipeline.sh --big-data    (Generates 25M+ records, 150k news, ~8-11 GB in HDFS + Spark ML)
#   bash /app/run_pipeline.sh --spark-only   (Runs Spark ML on existing HDFS data immediately)
#   bash /app/run_pipeline.sh --quick        (Ingests top 30 stocks + news + Spark ML ~2 min)
#   bash /app/run_pipeline.sh --full         (Full 500-stock ingestion + Spark ML ~25 min)
#   bash /app/run_pipeline.sh --nlp-only     (Spark NLP sentiment job only)
#   bash /app/run_pipeline.sh --anomaly-only (Spark Anomaly detection job only)
#   bash /app/run_pipeline.sh --momentum-only(Spark Momentum MapReduce job only)
#   bash /app/run_pipeline.sh --status       (Print HDFS data counts & exit — no processing)
# ============================================================================
set -e

MODE="${1:---quick}"

# ── Status mode: print HDFS data counts / job markers and exit (no processing) ──
if [ "${MODE}" = "--status" ]; then
    echo "📊 HDFS Data Status Report — $(date)"
    echo "────────────────────────────────────────────────────────────"
    echo "  Columns: DIR_COUNT  FILE_COUNT  CONTENT_SIZE(bytes)  PATH"
    hdfs dfs -count /data/raw/prices /data/raw/news \
        /data/processed/sentiment /data/processed/anomalies \
        /data/processed/momentum 2>/dev/null \
        || echo "  ⚠️  HDFS not reachable (NameNode down or in Safe Mode)."
    echo "────────────────────────────────────────────────────────────"
    echo "  Spark ML job completion markers:"
    for d in /data/processed/sentiment /data/processed/anomalies /data/processed/momentum; do
        if hdfs dfs -test -e "${d}/_SUCCESS" 2>/dev/null; then
            echo "    ✅ ${d} — _SUCCESS present"
        else
            echo "    ⏳ ${d} — no _SUCCESS (pending or failed)"
        fi
    done
    echo "────────────────────────────────────────────────────────────"
    exit 0
fi

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║     BDATL — Pipeline Execution Engine                   ║"
echo "║     Mode: ${MODE}                                        "
echo "║     Time: $(date)                                        "
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

cd /app

run_spark_sentiment() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a /tmp/pipeline.log
    echo "🧠 Spark ML: NLP Sentiment Analysis (Cluster: spark://master:7077)" | tee -a /tmp/pipeline.log
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a /tmp/pipeline.log
    spark-submit --master spark://master:7077 \
        --conf spark.cores.max=4 \
        --driver-memory 768m --executor-memory 768m \
        /app/src/ml/nlp_sentiment.py 2>&1 | tee -a /tmp/pipeline.log || {
        echo "⚠️  Sentiment pipeline had warnings/errors." | tee -a /tmp/pipeline.log
    }
}

run_spark_anomaly() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a /tmp/pipeline.log
    echo "⚡ Spark ML: Price-Volume Anomaly Detection" | tee -a /tmp/pipeline.log
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a /tmp/pipeline.log
    spark-submit --master spark://master:7077 \
        --conf spark.cores.max=4 \
        --driver-memory 768m --executor-memory 768m \
        /app/src/ml/anomaly_detector.py 2>&1 | tee -a /tmp/pipeline.log || {
        echo "⚠️  Anomaly detection had warnings/errors." | tee -a /tmp/pipeline.log
    }
}

run_spark_momentum() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a /tmp/pipeline.log
    echo "📈 Spark ML: Sector Momentum MapReduce" | tee -a /tmp/pipeline.log
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a /tmp/pipeline.log
    spark-submit --master spark://master:7077 \
        --conf spark.cores.max=4 \
        --driver-memory 768m --executor-memory 768m \
        /app/src/ml/momentum_mapreduce.py 2>&1 | tee -a /tmp/pipeline.log || {
        echo "⚠️  Momentum pipeline had warnings/errors." | tee -a /tmp/pipeline.log
    }
}

case "${MODE}" in
    --big-data)
        echo "🚀 Stage 1/2: Generating Multi-Million High-Frequency Intraday & News Big Data (5–10 GB scale)..." | tee -a /tmp/pipeline.log
        python -m src.ingestion.generate_big_data --tickers-count 500 --records-per-ticker 50000 --news-count 150000 --upload-hdfs 2>&1 | tee -a /tmp/pipeline.log || echo "⚠️  Big Data generation had errors, continuing..." | tee -a /tmp/pipeline.log
        echo "🧠 Stage 2/2: Running Distributed Spark ML Cluster on Big Data..." | tee -a /tmp/pipeline.log
        run_spark_sentiment
        run_spark_anomaly
        run_spark_momentum
        ;;

    --spark-only)
        echo "⏩ Skipping ingestion — Running Spark ML on existing HDFS data..." | tee -a /tmp/pipeline.log
        run_spark_sentiment
        run_spark_anomaly
        run_spark_momentum
        ;;

    --nlp-only)
        run_spark_sentiment
        ;;

    --anomaly-only)
        run_spark_anomaly
        ;;

    --momentum-only)
        run_spark_momentum
        ;;

    --full)
        echo "📈 Stage 1/3: Ingesting Price Data for all 500 NIFTY Stocks (5y)..." | tee -a /tmp/pipeline.log
        python -m src.ingestion.fetch_price --period 5y 2>&1 | tee -a /tmp/pipeline.log || echo "⚠️  Price ingestion had errors, continuing..." | tee -a /tmp/pipeline.log
        echo "📰 Stage 2/3: Ingesting RSS News Data..." | tee -a /tmp/pipeline.log
        python -m src.ingestion.fetch_news 2>&1 | tee -a /tmp/pipeline.log || echo "⚠️  News ingestion had errors, continuing..." | tee -a /tmp/pipeline.log
        echo "🧠 Stage 3/3: Running Distributed Spark ML Cluster..." | tee -a /tmp/pipeline.log
        run_spark_sentiment
        run_spark_anomaly
        run_spark_momentum
        ;;

    --quick|*)
        echo "📈 Stage 1/3: Ingesting Price Data for Top 30 High-Volume Stocks (2y)..." | tee -a /tmp/pipeline.log
        python -m src.ingestion.fetch_price --period 2y --limit 30 2>&1 | tee -a /tmp/pipeline.log || echo "⚠️  Price ingestion had errors, continuing..." | tee -a /tmp/pipeline.log
        echo "📰 Stage 2/3: Ingesting RSS News Data..." | tee -a /tmp/pipeline.log
        python -m src.ingestion.fetch_news 2>&1 | tee -a /tmp/pipeline.log || echo "⚠️  News ingestion had errors, continuing..." | tee -a /tmp/pipeline.log
        echo "🧠 Stage 3/3: Running Distributed Spark ML Cluster..." | tee -a /tmp/pipeline.log
        run_spark_sentiment
        run_spark_anomaly
        run_spark_momentum
        ;;
esac

echo "" | tee -a /tmp/pipeline.log
echo "╔══════════════════════════════════════════════════════════╗" | tee -a /tmp/pipeline.log
echo "║  ✅ Execution Completed!                                 ║" | tee -a /tmp/pipeline.log
echo "║  Check Spark Master at: http://localhost:8080            ║" | tee -a /tmp/pipeline.log
echo "║  Dashboard at:         http://localhost:8501            ║" | tee -a /tmp/pipeline.log
echo "╚══════════════════════════════════════════════════════════╝" | tee -a /tmp/pipeline.log
echo "" | tee -a /tmp/pipeline.log

# Print HDFS data summary
echo "📊 HDFS Data Summary:" | tee -a /tmp/pipeline.log
hdfs dfs -count /data/raw/prices /data/raw/news /data/processed/sentiment /data/processed/anomalies /data/processed/momentum 2>/dev/null | tee -a /tmp/pipeline.log || true

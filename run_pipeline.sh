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
# ============================================================================
set -e

MODE="${1:---quick}"

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║     BDATL — Pipeline Execution Engine                   ║"
echo "║     Mode: ${MODE}                                        "
echo "║     Time: $(date)                                        "
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

cd /app

run_spark_sentiment() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🧠 Spark ML: NLP Sentiment Analysis (Cluster: spark://master:7077)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    spark-submit --master spark://master:7077 \
        --conf spark.cores.max=4 \
        --driver-memory 768m --executor-memory 768m \
        /app/src/ml/nlp_sentiment.py || {
        echo "⚠️  Sentiment pipeline had warnings/errors."
    }
}

run_spark_anomaly() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "⚡ Spark ML: Price-Volume Anomaly Detection"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    spark-submit --master spark://master:7077 \
        --conf spark.cores.max=4 \
        --driver-memory 768m --executor-memory 768m \
        /app/src/ml/anomaly_detector.py || {
        echo "⚠️  Anomaly detection had warnings/errors."
    }
}

run_spark_momentum() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📈 Spark ML: Sector Momentum MapReduce"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    spark-submit --master spark://master:7077 \
        --conf spark.cores.max=4 \
        --driver-memory 768m --executor-memory 768m \
        /app/src/ml/momentum_mapreduce.py || {
        echo "⚠️  Momentum pipeline had warnings/errors."
    }
}

case "${MODE}" in
    --big-data)
        echo "🚀 Stage 1/2: Generating Multi-Million High-Frequency Intraday & News Big Data (5–10 GB scale)..."
        python -m src.ingestion.generate_big_data --tickers-count 500 --records-per-ticker 50000 --news-count 150000 --upload-hdfs || echo "⚠️  Big Data generation had errors, continuing..."
        echo "🧠 Stage 2/2: Running Distributed Spark ML Cluster on Big Data..."
        run_spark_sentiment
        run_spark_anomaly
        run_spark_momentum
        ;;

    --spark-only)
        echo "⏩ Skipping ingestion — Running Spark ML on existing HDFS data..."
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
        echo "📈 Stage 1/3: Ingesting Price Data for all 500 NIFTY Stocks (5y)..."
        python -m src.ingestion.fetch_price --period 5y || echo "⚠️  Price ingestion had errors, continuing..."
        echo "📰 Stage 2/3: Ingesting RSS News Data..."
        python -m src.ingestion.fetch_news || echo "⚠️  News ingestion had errors, continuing..."
        echo "🧠 Stage 3/3: Running Distributed Spark ML Cluster..."
        run_spark_sentiment
        run_spark_anomaly
        run_spark_momentum
        ;;

    --quick|*)
        echo "📈 Stage 1/3: Ingesting Price Data for Top 30 High-Volume Stocks (2y)..."
        python -m src.ingestion.fetch_price --period 2y --limit 30 || echo "⚠️  Price ingestion had errors, continuing..."
        echo "📰 Stage 2/3: Ingesting RSS News Data..."
        python -m src.ingestion.fetch_news || echo "⚠️  News ingestion had errors, continuing..."
        echo "🧠 Stage 3/3: Running Distributed Spark ML Cluster..."
        run_spark_sentiment
        run_spark_anomaly
        run_spark_momentum
        ;;
esac

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  ✅ Execution Completed!                                 ║"
echo "║  Check Spark Master at: http://localhost:8080            ║"
echo "║  Dashboard at:         http://localhost:8501            ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

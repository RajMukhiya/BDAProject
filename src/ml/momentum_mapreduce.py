"""
momentum_mapreduce.py — Rolling Sentiment Momentum Index (MapReduce / Spark)
=============================================================================
Computes 30/60/90-day rolling sentiment momentum indices aggregated by
Indian market sector (IT, Banking, Pharma, Auto, Energy, FMCG, etc.).
Blends true sentiment with price-return based semantic fallbacks for robust
momentum indices suitable for production dashboards.

Submit to Spark cluster:
    spark-submit --master spark://master:7077 /app/src/ml/momentum_mapreduce.py
"""

import os
import sys
import datetime

# Ensure project root is in sys.path for spark-submit
sys.path.insert(0, "/app")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

from src.config.tickers import TICKER_MAP
SECTOR_MAP = {k.replace(".NS", ""): v for k, v in TICKER_MAP.items()}


ROLLING_WINDOWS = [30, 60, 90]

def create_spark_session():
    return (SparkSession.builder
            .appName("BDATL_Sentiment_Momentum")
            .config("spark.sql.shuffle.partitions", "12")
            .config("spark.network.timeout", "300s")
            .config("spark.executor.heartbeatInterval", "30s")
            .getOrCreate())

def load_price_data(spark):
    paths = ["hdfs://master:9000/data/raw/prices"]
    raw_schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Adj Close", DoubleType(), True),
        StructField("Volume", DoubleType(), True),
        StructField("Ticker", StringType(), True),
        StructField("Exchange", StringType(), True),
    ])
    for path in paths:
        try:
            df = spark.read.option("header", "true").schema(raw_schema).csv(path)
            rename_map = {"Date": "trade_date", "Close": "close_price", "Ticker": "ticker"}
            for old, new in rename_map.items():
                if old in df.columns and old != new:
                    df = df.withColumnRenamed(old, new)
            if "trade_date" in df.columns and "close_price" in df.columns:
                return df.select("trade_date", "close_price", "ticker").dropna()
        except Exception as e:
            print(f"⚠️  Could not load prices from {path}: {e}")
    return None

def load_sentiment_data(spark):
    schema = StructType([
        StructField("source_type", StringType(), True),
        StructField("source_id", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("sentiment_score", DoubleType(), True),
        StructField("sentiment_label", StringType(), True),
        StructField("confidence", DoubleType(), True),
        StructField("event_date", StringType(), True),
        StructField("processed_at", StringType(), True),
    ])
    paths = ["hdfs://master:9000/data/processed/sentiment"]
    for path in paths:
        try:
            df = spark.read.csv(path, header=True, schema=schema)
            if df.head(1):  # Cheap check — avoids full .count()
                print(f"✅ Loaded sentiment records from {path}")
                return df
        except Exception as e:
            print(f"⚠️  Could not load sentiment from {path}: {e}")
    return spark.createDataFrame([], schema)

def build_blended_data(spark, prices_df, sentiment_df):
    """Blends genuine sentiment with a fallback proxy generated from price action."""
    w = Window.partitionBy("ticker").orderBy("trade_date")
    prices_df = prices_df.withColumn("prev_close", F.lag("close_price", 1).over(w))
    prices_df = prices_df.withColumn("daily_return", (F.col("close_price") - F.col("prev_close")) / F.col("prev_close"))
    
    # Simple semantic proxy for sentiment via returns
    prices_df = prices_df.withColumn("price_sentiment",
        F.when(F.col("daily_return") > 0.01, 1.0)
         .when(F.col("daily_return") < -0.01, -1.0)
         .otherwise(0.0)
    ).withColumn("date", F.substring("trade_date", 1, 10))

    if not sentiment_df.isEmpty():
        sentiment_df = sentiment_df.withColumn("date", F.substring("event_date", 1, 10))
        daily_sent = sentiment_df.groupBy("ticker", "date").agg(F.avg("sentiment_score").alias("real_sentiment"))
        joined = prices_df.join(daily_sent, ["ticker", "date"], "left")
        joined = joined.withColumn("blended_sentiment", F.coalesce("real_sentiment", "price_sentiment"))
    else:
        joined = prices_df.withColumn("blended_sentiment", F.col("price_sentiment"))

    # Assign sectors
    sector_rows = [(ticker, sector) for ticker, sector in SECTOR_MAP.items()]
    sector_df = spark.createDataFrame(sector_rows, ["map_ticker", "sector"])
    enriched = joined.join(F.broadcast(sector_df), joined.ticker == sector_df.map_ticker, "left").drop("map_ticker")
    enriched = enriched.withColumn("sector", F.coalesce(F.col("sector"), F.lit("Other")))
    
    return enriched

def compute_daily_aggregates(df):
    """Compute daily aggregate sentiment per sector."""
    daily_sector = (df.groupBy("sector", "date")
                    .agg(
                        F.avg("blended_sentiment").alias("sector_avg_sentiment"),
                        F.count("*").alias("sector_article_count"),
                        F.countDistinct("ticker").alias("ticker_count")
                    ))
    return daily_sector

def compute_rolling_momentum(daily_sector_df, window_days: int):
    """Compute rolling momentum via z-scores over N days."""
    w = Window.partitionBy("sector").orderBy("date").rowsBetween(-(window_days - 1), 0)
    
    momentum = (daily_sector_df
                .withColumn(f"rolling_avg_{window_days}d", F.avg("sector_avg_sentiment").over(w))
                .withColumn(f"rolling_std_{window_days}d", F.stddev("sector_avg_sentiment").over(w))
                .withColumn(f"rolling_tickers_{window_days}d", F.avg("ticker_count").over(w))
               )
    
    momentum = momentum.withColumn(
        "momentum_score",
        F.when(
            (F.col(f"rolling_std_{window_days}d").isNotNull()) & (F.col(f"rolling_std_{window_days}d") > 0),
            (F.col("sector_avg_sentiment") - F.col(f"rolling_avg_{window_days}d")) / F.col(f"rolling_std_{window_days}d")
        ).otherwise(0.0)
    )

    output = momentum.select(
        F.col("sector"),
        F.lit(window_days).alias("window_days"),
        F.col("momentum_score"),
        F.col(f"rolling_avg_{window_days}d").alias("avg_sentiment"),
        F.col(f"rolling_tickers_{window_days}d").cast(IntegerType()).alias("ticker_count"),
        F.date_sub(F.col("date"), window_days).cast(StringType()).alias("start_date"),
        F.col("date").alias("end_date"),
        F.lit(datetime.datetime.now().isoformat()).alias("computed_at")
    )
    return output

def main():
    print(f"\n{'='*60}")
    print(f" BDATL Sector Sentiment Momentum Index")
    print(f" Windows: {ROLLING_WINDOWS}")
    print(f" Time: {datetime.datetime.now()}")
    print(f"{'='*60}\n")

    spark = create_spark_session()

    prices_df = load_price_data(spark)
    if prices_df is None:
        print("❌ No price data available. Exiting.")
        spark.stop()
        sys.exit(1)

    sentiment_df = load_sentiment_data(spark)

    print("📊 Blending metrics and attributing sectors...")
    enriched_df = build_blended_data(spark, prices_df, sentiment_df)

    print("📊 Computing daily aggregates...")
    daily_sector = compute_daily_aggregates(enriched_df)
    daily_sector.cache()  # Reused for each rolling window

    all_momentum = None
    for window in ROLLING_WINDOWS:
        print(f"\n📈 Computing {window}-day rolling momentum...")
        momentum_df = compute_rolling_momentum(daily_sector, window)

        if all_momentum is None:
            all_momentum = momentum_df
        else:
            all_momentum = all_momentum.unionByName(momentum_df)

    # Distributed Write purely to HDFS
    hdfs_path = "hdfs://master:9000/data/processed/momentum"
    try:
        all_momentum.write.mode("overwrite").csv(hdfs_path, header=True)
        print(f"✅ Momentum indices saved to HDFS.")
    except Exception as e:
        print(f"⚠️  HDFS write failed: {e}")

    # Read back from HDFS for summary to avoid recomputing the full DAG
    print(f"\n{'='*60}")
    print(" Momentum Summary by Sector (latest):")
    try:
        saved = spark.read.csv(hdfs_path, header=True, inferSchema=True)
        if saved.head(1):
            latest = saved.agg(F.max("end_date")).collect()[0][0]
            (saved
             .filter(F.col("end_date") == latest)
             .orderBy("sector", "window_days")
             .show(50, truncate=False))
    except Exception:
        print("  (summary skipped)")

    print(f"{'='*60}\n")
    daily_sector.unpersist()
    spark.stop()
    print("✅ Sentiment Momentum MapReduce job complete.")

if __name__ == "__main__":
    main()

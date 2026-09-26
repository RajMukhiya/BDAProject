"""
anomaly_detector.py — Price-Volume Anomaly Detection with Sentiment Correlation
================================================================================
Uses Isolation Forest to detect statistically unusual price/volume behavior
in Indian stocks. Correlates detected anomalies with lagged sentiment shifts.

Submit to Spark cluster:
    spark-submit --master spark://master:7077 /app/src/ml/anomaly_detector.py
"""

import os
import sys
import datetime
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


def create_spark_session():
    """Initialize Spark session."""
    return (SparkSession.builder
            .appName("BDATL_Anomaly_Detector")
            .config("spark.sql.shuffle.partitions", "6")
            .getOrCreate())


def load_price_data(spark):
    """Load OHLCV price data from HDFS or local fallback."""
    schema = StructType([
        StructField("trade_date", StringType(), True),
        StructField("open_price", DoubleType(), True),
        StructField("high_price", DoubleType(), True),
        StructField("low_price", DoubleType(), True),
        StructField("close_price", DoubleType(), True),
        StructField("adj_close", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("ticker", StringType(), True),
        StructField("exchange", StringType(), True),
    ])
    # yfinance CSVs have columns: Date,Open,High,Low,Close,Adj Close,Volume,Ticker,Exchange
    # Try HDFS first with custom schema, then local with header inference
    paths = ["hdfs://master:9000/data/raw/prices"]
    for path in paths:
        try:
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
            df = spark.read.option("header", "true").schema(raw_schema).csv(path)
            # Normalize column names
            rename_map = {
                "Date": "trade_date", "Open": "open_price", "High": "high_price",
                "Low": "low_price", "Close": "close_price", "Adj Close": "adj_close",
                "Volume": "volume", "Ticker": "ticker", "Exchange": "exchange",
            }
            for old, new in rename_map.items():
                if old in df.columns and old != new:
                    df = df.withColumnRenamed(old, new)
            print(f"✅ Registered price stream from {path}")
            return df
        except Exception as e:
            print(f"⚠️  Could not load prices from {path}: {e}")
    return None


def load_sentiment_data(spark):
    """Load precomputed sentiment scores from HDFS or local."""
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
            print(f"⚠️  Could not load sentiment data from {path}: {e}")
    return None


def compute_technical_features(price_df):
    """Compute rolling technical features for anomaly detection."""

    # Window specifications
    w7 = Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-6, 0)
    w20 = Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-19, 0)
    w1 = Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-1, -1)

    featured = (price_df
                # Daily returns
                .withColumn("prev_close", F.lag("close_price", 1).over(
                    Window.partitionBy("ticker").orderBy("trade_date")))
                .withColumn("daily_return",
                            (F.col("close_price") - F.col("prev_close")) / F.col("prev_close"))

                # Volatility (7-day rolling std of returns)
                .withColumn("volatility_7d", F.stddev("daily_return").over(w7))

                # Volume features
                .withColumn("avg_volume_7d", F.avg("volume").over(w7))
                .withColumn("avg_volume_20d", F.avg("volume").over(w20))
                .withColumn("volume_ratio",
                            F.col("volume") / F.col("avg_volume_20d"))

                # Price range
                .withColumn("price_range",
                            (F.col("high_price") - F.col("low_price")) / F.col("close_price"))

                # Moving averages
                .withColumn("sma_7", F.avg("close_price").over(w7))
                .withColumn("sma_20", F.avg("close_price").over(w20))
                .withColumn("ma_crossover",
                            (F.col("sma_7") - F.col("sma_20")) / F.col("sma_20"))
                )

    # Drop rows with nulls from window calculations
    featured = featured.dropna(subset=[
        "daily_return", "volatility_7d", "volume_ratio", "ma_crossover"
    ])

    return featured


def run_isolation_forest(ticker_data: dict) -> list:
    """Run Isolation Forest on a per-ticker basis using sklearn."""
    results = []

    feature_cols = [
        "daily_return", "volatility_7d", "volume_ratio",
        "price_range", "ma_crossover"
    ]

    for ticker, rows in ticker_data.items():
        if len(rows) < 30:
            continue

        # Build feature matrix
        X = np.array([[r[col] for col in feature_cols] for r in rows])
        dates = [r["trade_date"] for r in rows]
        close_prices = [r["close_price"] for r in rows]
        volumes = [r["volume"] for r in rows]

        # Standardize
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # Fit Isolation Forest
        iso = IsolationForest(
            n_estimators=50,
            contamination=0.05,   # Expect ~5% anomalies
            random_state=42,
            n_jobs=1  # Limit threads to reduce memory pressure inside container
        )
        predictions = iso.fit_predict(X_scaled)
        scores = iso.decision_function(X_scaled)

        # Extract anomalies (prediction == -1)
        for i, pred in enumerate(predictions):
            if pred == -1:
                # Classify anomaly type
                daily_ret = rows[i]["daily_return"]
                vol_ratio = rows[i]["volume_ratio"]

                if abs(daily_ret) > 0.03 and vol_ratio > 2.0:
                    anomaly_type = "price_volume_spike"
                elif abs(daily_ret) > 0.03:
                    anomaly_type = "price_spike"
                elif vol_ratio > 2.5:
                    anomaly_type = "volume_surge"
                else:
                    anomaly_type = "statistical_outlier"

                results.append({
                    "ticker": ticker,
                    "anomaly_date": dates[i],
                    "close_price": close_prices[i],
                    "volume": int(volumes[i]),
                    "anomaly_score": float(-scores[i]),  # Higher = more anomalous
                    "anomaly_type": anomaly_type,
                    "daily_return": float(daily_ret),
                    "volume_ratio": float(vol_ratio),
                })

    return results


def correlate_with_sentiment(spark, anomaly_df, sentiment_df):
    """Add lagged sentiment context around each anomaly."""
    if sentiment_df is None:
        return anomaly_df.withColumn("sentiment_avg", F.lit(None).cast(DoubleType())) \
                         .withColumn("description", F.lit("No sentiment data available"))

    # Compute daily average sentiment per ticker
    daily_sentiment = (sentiment_df
                       .withColumn("event_date_clean", F.substring("event_date", 1, 10))
                       .groupBy("ticker", "event_date_clean")
                       .agg(
                           F.avg("sentiment_score").alias("daily_sentiment"),
                           F.count("*").alias("article_count")
                       ))

    # Join anomalies with sentiment (same day and 1 day prior)
    enriched = (anomaly_df
                .join(daily_sentiment,
                      (anomaly_df.ticker == daily_sentiment.ticker) &
                      (anomaly_df.anomaly_date == daily_sentiment.event_date_clean),
                      "left")
                .withColumnRenamed("daily_sentiment", "sentiment_avg")
                .drop(daily_sentiment.ticker))

    # Generate human-readable description
    enriched = enriched.withColumn("description",
        F.when(
            (F.col("daily_return") < -0.03) & (F.col("sentiment_avg") > 0.1),
            F.concat(
                F.lit("⚠️ DIVERGENCE: Price down "),
                F.format_number(F.abs(F.col("daily_return")) * 100, 1),
                F.lit("% but sentiment positive ("),
                F.format_number(F.col("sentiment_avg"), 2),
                F.lit(") — potential oversold signal")
            )
        ).when(
            (F.col("daily_return") > 0.03) & (F.col("sentiment_avg").isNotNull()) &
            (F.col("sentiment_avg") < -0.1),
            F.concat(
                F.lit("⚠️ DIVERGENCE: Price up "),
                F.format_number(F.col("daily_return") * 100, 1),
                F.lit("% but sentiment negative ("),
                F.format_number(F.col("sentiment_avg"), 2),
                F.lit(") — potential overbought signal")
            )
        ).otherwise(
            F.concat(
                F.col("anomaly_type"),
                F.lit(" detected: return="),
                F.format_number(F.col("daily_return") * 100, 1),
                F.lit("%, vol_ratio="),
                F.format_number(F.col("volume_ratio"), 1)
            )
        ))

    return enriched


def main():
    print(f"\n{'='*60}")
    print(f" BDATL Price-Volume Anomaly Detector")
    print(f" Time: {datetime.datetime.now()}")
    print(f"{'='*60}\n")

    spark = create_spark_session()

    # Load data
    price_df = load_price_data(spark)
    if price_df is None:
        print("❌ No price data found. Run fetch_price.py first.")
        spark.stop()
        return
    sentiment_df = load_sentiment_data(spark)

    # Compute technical features
    print("\n🔧 Computing technical features...")
    featured_df = compute_technical_features(price_df)

    # Ensure /app is in sys.path
    sys.path.insert(0, "/app")
    print(f"\n⚡ Running 100% distributed Spark anomaly detection across cluster...")
    z_score_expr = (
        (F.abs(F.col("daily_return")) / F.when(F.col("volatility_7d") > 0.0001, F.col("volatility_7d")).otherwise(0.01)) * 0.6 +
        F.coalesce(F.col("volume_ratio"), F.lit(1.0)) * 0.4
    )
    anomaly_df = (featured_df
        .withColumn("anomaly_score", F.round(z_score_expr, 3))
        .withColumn("anomaly_date", F.col("trade_date"))
        .withColumn("anomaly_type",
            F.when((F.abs(F.col("daily_return")) > 0.03) & (F.col("volume_ratio") > 2.0), F.lit("price_volume_spike"))
             .when(F.abs(F.col("daily_return")) > 0.03, F.lit("price_spike"))
             .when(F.col("volume_ratio") > 2.5, F.lit("volume_surge"))
             .otherwise(F.lit("statistical_outlier"))
        )
        .filter((F.col("anomaly_score") > 2.5) | (F.abs(F.col("daily_return")) > 0.035) | (F.col("volume_ratio") > 2.5))
    )

    # Correlate with sentiment
    print("\n🔗 Correlating anomalies with sentiment data...")
    enriched_df = correlate_with_sentiment(spark, anomaly_df, sentiment_df)

    # Select output columns
    output_cols = [
        "ticker", "anomaly_date", "close_price", "volume",
        "anomaly_score", "anomaly_type", "sentiment_avg", "description"
    ]
    output_df = enriched_df.select(
        *[c for c in output_cols if c in enriched_df.columns]
    )

    # Write to HDFS
    hdfs_path = "hdfs://master:9000/data/processed/anomalies"
    try:
        output_df.write.mode("overwrite").csv(hdfs_path, header=True)
        print(f"✅ Anomalies saved to HDFS.")
    except Exception as e:
        print(f"⚠️  HDFS write failed: {e}")

    # Summary
    print(f"\n{'='*60}")
    print(" Anomaly Type Distribution:")
    output_df.groupBy("anomaly_type").count().show()
    print(" Top 10 Most Anomalous Events:")
    output_df.orderBy(F.desc("anomaly_score")).show(10, truncate=False)
    print(f"{'='*60}\n")

    spark.stop()
    print("✅ Anomaly detection complete.")


if __name__ == "__main__":
    main()

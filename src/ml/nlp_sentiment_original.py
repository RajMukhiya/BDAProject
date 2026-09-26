"""
nlp_sentiment.py — PySpark NLP Sentiment Pipeline
===================================================
Processes news headlines to extract sentiment scores
using TextBlob as fallback.

Submit to Spark cluster:
    spark-submit --master spark://master:7077 /app/src/ml/nlp_sentiment.py
"""

import os
import sys
import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, FloatType
)

from src.config.tickers import TICKER_MAP
SECTOR_MAP = {k.replace(".NS", ""): v for k, v in TICKER_MAP.items()}

# TextBlob for rule-based sentiment (fallback / baseline)
try:
    from textblob import TextBlob
    HAS_TEXTBLOB = True
except ImportError:
    HAS_TEXTBLOB = False


def create_spark_session():
    """Initialize Spark session with HDFS support."""
    return (SparkSession.builder
            .appName("BDATL_NLP_Sentiment")
            .config("spark.sql.shuffle.partitions", "6")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate())


def load_news_data(spark):
    """Load cleaned news data from HDFS or local fallback."""
    schema = StructType([
        StructField("source", StringType(), True),
        StructField("title", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("published_date", StringType(), True),
        StructField("link", StringType(), True),
        StructField("matched_tickers", StringType(), True),
        StructField("fetched_at", StringType(), True),
    ])
    paths = ["hdfs://master:9000/data/raw/news"]
    for path in paths:
        try:
            df = spark.read.csv(path, header=True, schema=schema)
            df = df.withColumn("source_type", F.lit("news"))
            df = df.withColumn("text", F.concat_ws(" ", F.col("title"), F.col("summary")))
            df = df.withColumn("source_id", F.col("link"))
            df = df.withColumn("event_date", F.col("published_date"))
            count = df.count()
            if count > 0:
                print(f"✅ Loaded {count} news articles from {path}")
                return df
        except Exception as e:
            print(f"⚠️  Could not load news from {path}: {e}")
    return None


def load_reddit_data(spark):
    """Load cleaned Reddit data from HDFS or local fallback."""
    schema = StructType([
        StructField("subreddit", StringType(), True),
        StructField("post_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("selftext", StringType(), True),
        StructField("score", StringType(), True),
        StructField("upvote_ratio", StringType(), True),
        StructField("num_comments", StringType(), True),
        StructField("created_utc", StringType(), True),
        StructField("author", StringType(), True),
        StructField("url", StringType(), True),
        StructField("matched_tickers", StringType(), True),
        StructField("flair", StringType(), True),
        StructField("fetched_at", StringType(), True),
    ])
    paths = ["hdfs://master:9000/data/raw/reddit"]
    for path in paths:
        try:
            df = spark.read.csv(path, header=True, schema=schema)
            df = df.withColumn("source_type", F.lit("reddit"))
            df = df.withColumn("text", F.concat_ws(" ", F.col("title"), F.col("selftext")))
            df = df.withColumn("source_id", F.col("post_id"))
            df = df.withColumn("event_date", F.col("created_utc"))
            count = df.count()
            if count > 0:
                print(f"✅ Loaded {count} Reddit posts from {path}")
                return df
        except Exception as e:
            print(f"⚠️  Could not load Reddit data from {path}: {e}")
    return None





def textblob_sentiment(text: str) -> tuple:
    """Compute sentiment using TextBlob (rule-based fallback)."""
    if not text or not HAS_TEXTBLOB:
        return (0.0, "neutral", 0.0)

    blob = TextBlob(str(text))
    polarity = blob.sentiment.polarity      # -1.0 to 1.0
    subjectivity = blob.sentiment.subjectivity  # 0.0 to 1.0

    if polarity > 0.1:
        label = "positive"
    elif polarity < -0.1:
        label = "negative"
    else:
        label = "neutral"

    return (float(polarity), label, float(subjectivity))


def compute_sentiment(spark, df):
    """Compute sentiment scores for all text using TextBlob UDF + TF-IDF features."""

    # Register TextBlob UDF
    @F.udf(StructType([
        StructField("sentiment_score", DoubleType()),
        StructField("sentiment_label", StringType()),
        StructField("confidence", DoubleType()),
    ]))
    def sentiment_udf(text):
        score, label, confidence = textblob_sentiment(text)
        return (score, label, confidence)

    # Apply sentiment
    df_with_sentiment = df.withColumn("_sentiment", sentiment_udf(F.col("text")))
    df_with_sentiment = (df_with_sentiment
                         .withColumn("sentiment_score", F.col("_sentiment.sentiment_score"))
                         .withColumn("sentiment_label", F.col("_sentiment.sentiment_label"))
                         .withColumn("confidence", F.col("_sentiment.confidence"))
                         .drop("_sentiment"))

    # Add processing timestamp
    df_with_sentiment = df_with_sentiment.withColumn(
        "processed_at", F.lit(datetime.datetime.now().isoformat())
    )

    return df_with_sentiment


def explode_tickers(df):
    """Explode comma-separated ticker matches into individual rows."""
    return (df
            .withColumn("ticker", F.explode(F.split(F.col("matched_tickers"), ",")))
            .withColumn("ticker", F.trim(F.col("ticker")))
            .filter(F.col("ticker") != ""))


def main():
    print(f"\n{'='*60}")
    print(f" BDATL NLP Sentiment Pipeline")
    print(f" Time: {datetime.datetime.now()}")
    print(f"{'='*60}\n")

    spark = create_spark_session()

    # Load data from HDFS
    news_df = load_news_data(spark)

    if news_df is None:
        print("❌ No news data available. Exiting.")
        spark.stop()
        sys.exit(1)

    combined_df = news_df.select("source_type", "source_id", "text", "matched_tickers", "event_date")

    # Filter nulls
    combined_df = combined_df.filter(F.col("text").isNotNull())
    total = combined_df.count()
    print(f"📊 Total documents to process: {total}")

    # --- Step 1: Compute sentiment scores ---
    print("\n🧠 Computing sentiment scores...")
    sentiment_df = compute_sentiment(spark, combined_df)

    # --- Step 3: Explode tickers for per-ticker output ---
    sentiment_by_ticker = explode_tickers(sentiment_df)

    # --- Step 4: Select output columns and write to HDFS ---
    output_df = sentiment_by_ticker.select(
        "source_type", "source_id", "ticker",
        "sentiment_score", "sentiment_label", "confidence",
        "event_date", "processed_at"
    )

    # Write to HDFS
    hdfs_path = "hdfs://master:9000/data/processed/sentiment"
    try:
        output_df.write.mode("overwrite").csv(hdfs_path, header=True)
        print(f"✅ Sentiment scores saved to HDFS ({output_df.count()} rows).")
    except Exception as e:
        print(f"⚠️  HDFS write failed: {e}")

    # --- Step 5: Summary statistics ---
    print(f"\n{'='*60}")
    print(" Sentiment Summary:")
    output_df.groupBy("sentiment_label").count().show()

    print(" Top 10 Tickers by article count:")
    (output_df
     .groupBy("ticker")
     .agg(
         F.count("*").alias("count"),
         F.avg("sentiment_score").alias("avg_sentiment")
     )
     .orderBy(F.desc("count"))
     .show(10))
    print(f"{'='*60}\n")

    spark.stop()
    print("✅ NLP Sentiment Pipeline complete.")


if __name__ == "__main__":
    main()

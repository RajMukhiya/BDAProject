"""
BDATL - Distributed NLP Sentiment Pipeline

Pipeline:
HDFS News -> Spark DataFrame -> TF-IDF -> TextBlob Sentiment
-> Ticker Explosion -> HDFS Sentiment Output
"""

import datetime
import sys

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF

from textblob import TextBlob


INPUT_PATH = "hdfs://master:9000/data/raw/news"
OUTPUT_PATH = "hdfs://master:9000/data/processed/sentiment"


def create_spark():
    return (
        SparkSession.builder
        .appName("BDATL_NLP_Sentiment")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def main():

    print("=" * 70)
    print(" BDATL DISTRIBUTED NLP SENTIMENT PIPELINE")
    print(" Started:", datetime.datetime.now())
    print("=" * 70)

    spark = create_spark()

    # ------------------------------------------------------------
    # 1. Read news from HDFS
    # ------------------------------------------------------------

    schema = StructType([
        StructField("source", StringType(), True),
        StructField("title", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("published_date", StringType(), True),
        StructField("link", StringType(), True),
        StructField("matched_tickers", StringType(), True),
        StructField("fetched_at", StringType(), True),
    ])

    print("\n[1/5] Reading news from HDFS...")

    news = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(INPUT_PATH)
    )

    # Keep only usable news records
    news = (
        news
        .withColumn(
            "text",
            F.trim(
                F.concat_ws(
                    " ",
                    F.coalesce(F.col("title"), F.lit("")),
                    F.coalesce(F.col("summary"), F.lit(""))
                )
            )
        )
        .filter(F.length(F.col("text")) > 0)
        .filter(F.col("matched_tickers").isNotNull())
        .filter(F.length(F.trim(F.col("matched_tickers"))) > 0)
    )

    # ------------------------------------------------------------
    # 2. Explode matched tickers
    # ------------------------------------------------------------

    ticker_news = (
        news
        .withColumn(
            "ticker",
            F.explode(
                F.split(F.col("matched_tickers"), ",")
            )
        )
        .withColumn("ticker", F.trim(F.col("ticker")))
        .filter(F.length(F.col("ticker")) > 0)
        .select(
            F.lit("news").alias("source_type"),
            F.col("link").alias("source_id"),
            F.col("text"),
            F.col("ticker"),
            F.col("published_date").alias("event_date")
        )
    )

    print("[2/5] Preparing ticker-level news records...")

    # ------------------------------------------------------------
    # 3. TF-IDF using Spark ML
    # ------------------------------------------------------------

    print("[3/5] Computing distributed TF-IDF features...")

    tokenizer = RegexTokenizer(
        inputCol="text",
        outputCol="words",
        pattern="\\W+",
        toLowercase=True
    )

    tokenized = tokenizer.transform(ticker_news)

    remover = StopWordsRemover(
        inputCol="words",
        outputCol="filtered_words"
    )

    cleaned = remover.transform(tokenized)

    hashing_tf = HashingTF(
        inputCol="filtered_words",
        outputCol="tf_features",
        numFeatures=4096
    )

    tf_data = hashing_tf.transform(cleaned)

    idf = IDF(
        inputCol="tf_features",
        outputCol="tfidf_features",
        minDocFreq=1
    )

    idf_model = idf.fit(tf_data)
    tfidf_data = idf_model.transform(tf_data)

    # ------------------------------------------------------------
    # 4. TextBlob sentiment using partition processing
    # ------------------------------------------------------------

    print("[4/5] Computing TextBlob sentiment...")

    sentiment_schema = StructType([
        StructField("source_type", StringType(), True),
        StructField("source_id", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("sentiment_score", DoubleType(), True),
        StructField("sentiment_label", StringType(), True),
        StructField("confidence", DoubleType(), True),
        StructField("event_date", StringType(), True),
        StructField("processed_at", StringType(), True),
    ])

    # Only send the columns required for sentiment to the Python workers.
    sentiment_input = tfidf_data.select(
        "source_type",
        "source_id",
        "ticker",
        "text",
        "event_date"
    )

    def sentiment_partition(rows):

        processed_time = datetime.datetime.now().isoformat()

        for row in rows:

            text = row["text"] or ""

            try:
                blob = TextBlob(text)

                polarity = float(blob.sentiment.polarity)
                subjectivity = float(blob.sentiment.subjectivity)

                if polarity > 0.1:
                    label = "positive"
                elif polarity < -0.1:
                    label = "negative"
                else:
                    label = "neutral"

                yield Row(
                    source_type=row["source_type"],
                    source_id=row["source_id"],
                    ticker=row["ticker"],
                    sentiment_score=polarity,
                    sentiment_label=label,
                    confidence=subjectivity,
                    event_date=row["event_date"],
                    processed_at=processed_time
                )

            except Exception:
                yield Row(
                    source_type=row["source_type"],
                    source_id=row["source_id"],
                    ticker=row["ticker"],
                    sentiment_score=0.0,
                    sentiment_label="neutral",
                    confidence=0.0,
                    event_date=row["event_date"],
                    processed_at=processed_time
                )

    result_rdd = sentiment_input.rdd.mapPartitions(sentiment_partition)

    result = spark.createDataFrame(
        result_rdd,
        schema=sentiment_schema
    )

    # ------------------------------------------------------------
    # 5. Write to HDFS
    # ------------------------------------------------------------

    print("[5/5] Writing sentiment results to HDFS...")

    (
        result
        .repartition(3)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(OUTPUT_PATH)
    )

    print("\n" + "=" * 70)
    print(" NLP SENTIMENT PIPELINE COMPLETED")
    print(" Output:", OUTPUT_PATH)
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()

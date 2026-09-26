-- ============================================================================
-- Hive Schema Definitions for Indian Stock Market Sentiment Engine
-- Run on master: hive -f /app/src/batch/hive_schema.hql
-- ============================================================================

-- Create database
CREATE DATABASE IF NOT EXISTS bdatl_warehouse;
USE bdatl_warehouse;

-- =========================================================================
-- 1. OHLCV Price Data (NSE/BSE)
-- =========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS stock_prices (
    trade_date      STRING,
    open_price      DOUBLE,
    high_price      DOUBLE,
    low_price       DOUBLE,
    close_price     DOUBLE,
    adj_close       DOUBLE,
    volume          BIGINT,
    ticker          STRING,
    exchange        STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/prices'
TBLPROPERTIES ('skip.header.line.count'='1');

-- =========================================================================
-- 2. Financial News Articles
-- =========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS news_articles (
    source          STRING,
    title           STRING,
    summary         STRING,
    published       STRING,
    link            STRING,
    matched_tickers STRING,
    fetched_at      STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/news'
TBLPROPERTIES ('skip.header.line.count'='1');

-- =========================================================================
-- 3. Reddit Posts
-- =========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS reddit_posts (
    subreddit       STRING,
    post_id         STRING,
    title           STRING,
    selftext        STRING,
    score           INT,
    upvote_ratio    DOUBLE,
    num_comments    INT,
    created_utc     STRING,
    author          STRING,
    url             STRING,
    matched_tickers STRING,
    flair           STRING,
    fetched_at      STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/reddit'
TBLPROPERTIES ('skip.header.line.count'='1');

-- =========================================================================
-- 4. Computed Sentiment Scores (written by NLP pipeline)
-- =========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS sentiment_scores (
    source_type     STRING COMMENT 'news or reddit',
    source_id       STRING,
    ticker          STRING,
    sentiment_score DOUBLE,
    sentiment_label STRING COMMENT 'positive, negative, neutral',
    confidence      DOUBLE,
    event_date      STRING,
    processed_at    STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/processed/sentiment';

-- =========================================================================
-- 5. Detected Anomalies (written by anomaly detector)
-- =========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS price_anomalies (
    ticker          STRING,
    anomaly_date    STRING,
    close_price     DOUBLE,
    volume          BIGINT,
    anomaly_score   DOUBLE,
    anomaly_type    STRING COMMENT 'price_spike, volume_surge, divergence',
    sentiment_avg   DOUBLE COMMENT 'average sentiment around anomaly window',
    description     STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/processed/anomalies';

-- =========================================================================
-- 6. Sector Momentum Index (written by MapReduce job)
-- =========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS sector_momentum (
    sector          STRING,
    window_days     INT COMMENT '30, 60, or 90',
    momentum_score  DOUBLE,
    avg_sentiment   DOUBLE,
    ticker_count    INT,
    start_date      STRING,
    end_date        STRING,
    computed_at     STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/processed/momentum';

-- =========================================================================
-- Useful Analytical Views
-- =========================================================================

-- Daily sentiment summary per ticker
CREATE VIEW IF NOT EXISTS daily_ticker_sentiment AS
SELECT
    ticker,
    SUBSTR(event_date, 1, 10) AS trade_date,
    AVG(sentiment_score) AS avg_sentiment,
    COUNT(*) AS article_count,
    SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) AS positive_count,
    SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) AS negative_count
FROM sentiment_scores
GROUP BY ticker, SUBSTR(event_date, 1, 10);

-- Price + Sentiment joined view (for divergence analysis)
CREATE VIEW IF NOT EXISTS price_sentiment_joined AS
SELECT
    p.ticker,
    p.trade_date,
    p.close_price,
    p.volume,
    s.avg_sentiment,
    s.article_count,
    ((p.close_price - LAG(p.close_price) OVER (
        PARTITION BY p.ticker ORDER BY p.trade_date
    )) / LAG(p.close_price) OVER (
        PARTITION BY p.ticker ORDER BY p.trade_date
    )) * 100 AS price_change_pct
FROM stock_prices p
LEFT JOIN daily_ticker_sentiment s
    ON p.ticker = s.ticker AND p.trade_date = s.trade_date;

-- ============================================================================
-- Pig ETL Script: Normalize, Deduplicate, and Join News ↔ Price Data
-- Run on master: pig -f /app/src/batch/etl_normalize_join.pig
-- ============================================================================

-- =========================================================================
-- STEP 1: Load raw price data from HDFS
-- =========================================================================
raw_prices = LOAD '/data/raw/prices/*.csv'
    USING PigStorage(',')
    AS (
        trade_date:chararray,
        open_price:double,
        high_price:double,
        low_price:double,
        close_price:double,
        adj_close:double,
        volume:long,
        ticker:chararray,
        exchange:chararray
    );

-- Skip CSV headers
prices_no_header = FILTER raw_prices BY trade_date != 'Date';

-- Normalize date format to YYYY-MM-DD
prices_normalized = FOREACH prices_no_header GENERATE
    SUBSTRING(trade_date, 0, 10) AS trade_date,
    open_price, high_price, low_price, close_price, adj_close, volume,
    UPPER(TRIM(ticker)) AS ticker,
    UPPER(TRIM(exchange)) AS exchange;

-- Deduplicate (same ticker + date)
prices_grouped = GROUP prices_normalized BY (ticker, trade_date);
prices_deduped = FOREACH prices_grouped {
    sorted = ORDER prices_normalized BY volume DESC;
    top = LIMIT sorted 1;
    GENERATE FLATTEN(top);
};

-- =========================================================================
-- STEP 2: Load raw news data from HDFS
-- =========================================================================
raw_news = LOAD '/data/raw/news/*.csv'
    USING PigStorage(',')
    AS (
        source:chararray,
        title:chararray,
        summary:chararray,
        published:chararray,
        link:chararray,
        matched_tickers:chararray,
        fetched_at:chararray
    );

-- Skip CSV headers
news_no_header = FILTER raw_news BY source != 'source';

-- Normalize published date to YYYY-MM-DD
news_normalized = FOREACH news_no_header GENERATE
    source,
    title,
    summary,
    SUBSTRING(published, 0, 10) AS published_date,
    link,
    UPPER(TRIM(matched_tickers)) AS matched_tickers,
    fetched_at;

-- Deduplicate by title + published_date
news_grouped = GROUP news_normalized BY (title, published_date);
news_deduped = FOREACH news_grouped {
    top = LIMIT news_normalized 1;
    GENERATE FLATTEN(top);
};

-- =========================================================================
-- STEP 3: Flatten news articles to per-ticker rows
-- Each article may mention multiple tickers (comma-separated)
-- =========================================================================
news_ticker_flat = FOREACH news_deduped GENERATE
    source,
    title,
    summary,
    published_date,
    link,
    FLATTEN(TOKENIZE(matched_tickers, ',')) AS ticker,
    fetched_at;

-- =========================================================================
-- STEP 4: Join news events with price windows by ticker and date
-- This aligns each news article with the stock price on the same day
-- =========================================================================
news_price_joined = JOIN
    news_ticker_flat BY (ticker, published_date),
    prices_deduped BY (prices_normalized::ticker, prices_normalized::trade_date);

-- Project the final joined output
joined_output = FOREACH news_price_joined GENERATE
    news_ticker_flat::ticker AS ticker,
    news_ticker_flat::published_date AS event_date,
    news_ticker_flat::source AS news_source,
    news_ticker_flat::title AS headline,
    news_ticker_flat::summary AS summary,
    prices_deduped::prices_normalized::close_price AS close_price,
    prices_deduped::prices_normalized::volume AS volume,
    prices_deduped::prices_normalized::high_price AS high_price,
    prices_deduped::prices_normalized::low_price AS low_price;

-- =========================================================================
-- STEP 5: Store processed outputs back to HDFS
-- =========================================================================
-- Store clean, deduplicated prices
STORE prices_deduped INTO '/data/processed/prices_clean'
    USING PigStorage(',');

-- Store clean, deduplicated news
STORE news_deduped INTO '/data/processed/news_clean'
    USING PigStorage(',');

-- Store the news ↔ price joined dataset
STORE joined_output INTO '/data/processed/news_price_aligned'
    USING PigStorage(',');

-- =========================================================================
-- STEP 6: Compute daily news volume per ticker (for momentum analysis)
-- =========================================================================
news_by_ticker_date = GROUP news_ticker_flat BY (ticker, published_date);
daily_news_volume = FOREACH news_by_ticker_date GENERATE
    FLATTEN(group) AS (ticker, event_date),
    COUNT(news_ticker_flat) AS article_count;

STORE daily_news_volume INTO '/data/processed/daily_news_volume'
    USING PigStorage(',');

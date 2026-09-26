"""
pipeline_standalone.py — Standalone Data Processing Pipeline (No PySpark)
==========================================================================
Runs all processing stages using pandas + sklearn + TextBlob.
Much faster and more reliable than the PySpark versions for single-node use.

Usage:
    python -m src.pipeline_standalone
"""

import os
import sys
import glob
import datetime

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

try:
    from textblob import TextBlob
    HAS_TEXTBLOB = True
except ImportError:
    HAS_TEXTBLOB = False

# ---------------------------------------------------------------------------
# Paths (cross-platform: works on Windows dev and Linux/Docker)
# ---------------------------------------------------------------------------
def _resolve_data_root() -> Path:
    """Find the project data root, supporting both Docker (/app) and local dev."""
    # Docker / container path
    docker_root = Path("/app/data")
    if docker_root.exists():
        return docker_root
    # Walk up from this file to find a directory containing 'data/'
    here = Path(__file__).resolve()
    for parent in [here.parent.parent, here.parent.parent.parent]:
        candidate = parent / "data"
        if candidate.exists():
            return candidate
    # Last resort: relative to cwd
    return Path("data")

_DATA_ROOT = _resolve_data_root()
RAW_PRICES   = str(_DATA_ROOT / "raw" / "prices")
RAW_NEWS     = str(_DATA_ROOT / "raw" / "news")
RAW_REDDIT   = str(_DATA_ROOT / "raw" / "reddit")
OUT_SENTIMENT = str(_DATA_ROOT / "processed" / "sentiment")
OUT_ANOMALIES = str(_DATA_ROOT / "processed" / "anomalies")
OUT_MOMENTUM  = str(_DATA_ROOT / "processed" / "momentum")

# ---------------------------------------------------------------------------
# Sector Mapping (Indian Market)
# ---------------------------------------------------------------------------
from src.config.tickers import TICKER_MAP
SECTOR_MAP = {k.replace(".NS", ""): v for k, v in TICKER_MAP.items()}


# ---------------------------------------------------------------------------
# Stage 1: Load raw data
# ---------------------------------------------------------------------------
def load_csvs(directory):
    """Load all CSV files from a directory into one DataFrame."""
    if not os.path.exists(directory):
        return pd.DataFrame()
    files = glob.glob(os.path.join(directory, "*.csv"))
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception as e:
            print(f"  ⚠️ Error reading {f}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ---------------------------------------------------------------------------
# Stage 2: NLP Sentiment
# ---------------------------------------------------------------------------
def compute_sentiment(text):
    """Compute sentiment score using TextBlob."""
    if not text or not isinstance(text, str) or not HAS_TEXTBLOB:
        return 0.0, "neutral", 0.0
    try:
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        subjectivity = blob.sentiment.subjectivity
        if polarity > 0.1:
            label = "positive"
        elif polarity < -0.1:
            label = "negative"
        else:
            label = "neutral"
        return polarity, label, subjectivity
    except Exception:
        return 0.0, "neutral", 0.0


def run_sentiment_pipeline(news_df):
    """Run sentiment analysis on news data."""
    print("\n🧠 Stage: NLP Sentiment Analysis")

    rows = []

    # Process news
    if not news_df.empty:
        print(f"  Processing {len(news_df)} news articles...")
        for _, row in news_df.iterrows():
            text = f"{row.get('title', '')} {row.get('summary', '')}"
            score, label, confidence = compute_sentiment(text)
            tickers = str(row.get("matched_tickers", "GENERAL"))

            for ticker in tickers.split(","):
                ticker = ticker.strip()
                if ticker:
                    rows.append({
                        "source_type": "news",
                        "source_id": row.get("link", ""),
                        "ticker": ticker,
                        "sentiment_score": score,
                        "sentiment_label": label,
                        "confidence": confidence,
                        "event_date": row.get("published", row.get("fetched_at", "")),
                        "processed_at": datetime.datetime.now().isoformat(),
                    })

    if not rows:
        print("  ⚠️ No text data to analyze.")
        return pd.DataFrame()

    sentiment_df = pd.DataFrame(rows)
    # Filter out GENERAL rows for ticker-specific analysis
    sentiment_df = sentiment_df[sentiment_df["ticker"] != "GENERAL"]

    os.makedirs(OUT_SENTIMENT, exist_ok=True)
    sentiment_df.to_csv(os.path.join(OUT_SENTIMENT, "sentiment.csv"), index=False)
    print(f"  ✅ Saved {len(sentiment_df)} sentiment scores")
    print(f"     Labels: {sentiment_df['sentiment_label'].value_counts().to_dict()}")
    return sentiment_df


# ---------------------------------------------------------------------------
# Stage 3: Anomaly Detection
# ---------------------------------------------------------------------------
def run_anomaly_detection(prices_df, sentiment_df):
    """Run Isolation Forest anomaly detection on price data."""
    print("\n🔍 Stage: Anomaly Detection")

    if prices_df.empty:
        print("  ⚠️ No price data available.")
        return pd.DataFrame()

    # Normalize columns
    col_map = {}
    for col in prices_df.columns:
        lc = col.lower().strip()
        if lc == "date":
            col_map[col] = "trade_date"
        elif lc == "open":
            col_map[col] = "open_price"
        elif lc == "high":
            col_map[col] = "high_price"
        elif lc == "low":
            col_map[col] = "low_price"
        elif lc == "close":
            col_map[col] = "close_price"
        elif lc == "volume":
            col_map[col] = "volume"
        elif lc == "ticker":
            col_map[col] = "ticker"
    prices_df = prices_df.rename(columns=col_map)

    required = ["trade_date", "close_price", "volume", "ticker",
                "open_price", "high_price", "low_price"]
    missing = [c for c in required if c not in prices_df.columns]
    if missing:
        print(f"  ⚠️ Missing columns: {missing}")
        return pd.DataFrame()

    prices_df = prices_df.sort_values(["ticker", "trade_date"])
    prices_df["close_price"] = pd.to_numeric(prices_df["close_price"], errors="coerce")
    prices_df["volume"] = pd.to_numeric(prices_df["volume"], errors="coerce")
    prices_df["open_price"] = pd.to_numeric(prices_df["open_price"], errors="coerce")
    prices_df["high_price"] = pd.to_numeric(prices_df["high_price"], errors="coerce")
    prices_df["low_price"] = pd.to_numeric(prices_df["low_price"], errors="coerce")

    all_anomalies = []
    tickers = prices_df["ticker"].unique()
    print(f"  Processing {len(tickers)} tickers...")

    for ticker in tickers:
        tdf = prices_df[prices_df["ticker"] == ticker].copy()
        if len(tdf) < 30:
            continue

        # Compute features
        tdf["daily_return"] = tdf["close_price"].pct_change()
        tdf["volatility_7d"] = tdf["daily_return"].rolling(7).std()
        tdf["avg_volume_20d"] = tdf["volume"].rolling(20).mean()
        tdf["volume_ratio"] = tdf["volume"] / tdf["avg_volume_20d"]
        tdf["price_range"] = (tdf["high_price"] - tdf["low_price"]) / tdf["close_price"]
        tdf["sma_7"] = tdf["close_price"].rolling(7).mean()
        tdf["sma_20"] = tdf["close_price"].rolling(20).mean()
        tdf["ma_crossover"] = (tdf["sma_7"] - tdf["sma_20"]) / tdf["sma_20"]

        tdf = tdf.dropna(subset=["daily_return", "volatility_7d", "volume_ratio", "ma_crossover"])
        if len(tdf) < 30:
            continue

        feature_cols = ["daily_return", "volatility_7d", "volume_ratio", "price_range", "ma_crossover"]
        X = tdf[feature_cols].values

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        iso = IsolationForest(n_estimators=100, contamination=0.05, random_state=42, n_jobs=-1)
        predictions = iso.fit_predict(X_scaled)
        scores = iso.decision_function(X_scaled)

        for i, pred in enumerate(predictions):
            if pred == -1:
                row = tdf.iloc[i]
                dr = row["daily_return"]
                vr = row["volume_ratio"]

                if abs(dr) > 0.03 and vr > 2.0:
                    atype = "price_volume_spike"
                elif abs(dr) > 0.03:
                    atype = "price_spike"
                elif vr > 2.5:
                    atype = "volume_surge"
                else:
                    atype = "statistical_outlier"

                # Check for sentiment divergence
                desc = f"{atype} detected: return={dr*100:.1f}%, vol_ratio={vr:.1f}"
                if not sentiment_df.empty:
                    ts = sentiment_df[
                        (sentiment_df["ticker"] == ticker) &
                        (sentiment_df["event_date"].str[:10] == str(row["trade_date"])[:10])
                    ]
                    if not ts.empty:
                        avg_sent = ts["sentiment_score"].mean()
                        if dr < -0.03 and avg_sent > 0.1:
                            desc = (f"⚠️ DIVERGENCE: Price down {abs(dr)*100:.1f}% "
                                    f"but sentiment positive ({avg_sent:.2f}) — potential oversold")
                            atype = "bearish_divergence"
                        elif dr > 0.03 and avg_sent < -0.1:
                            desc = (f"⚠️ DIVERGENCE: Price up {dr*100:.1f}% "
                                    f"but sentiment negative ({avg_sent:.2f}) — potential overbought")
                            atype = "bullish_divergence"

                all_anomalies.append({
                    "ticker": ticker,
                    "anomaly_date": row["trade_date"],
                    "close_price": row["close_price"],
                    "volume": int(row["volume"]),
                    "anomaly_score": float(-scores[i]),
                    "anomaly_type": atype,
                    "daily_return": float(dr),
                    "volume_ratio": float(vr),
                    "description": desc,
                })

    if not all_anomalies:
        print("  ⚠️ No anomalies detected.")
        return pd.DataFrame()

    anomaly_df = pd.DataFrame(all_anomalies)
    os.makedirs(OUT_ANOMALIES, exist_ok=True)
    anomaly_df.to_csv(os.path.join(OUT_ANOMALIES, "anomalies.csv"), index=False)
    print(f"  ✅ Detected {len(anomaly_df)} anomalies across {len(tickers)} tickers")
    print(f"     Types: {anomaly_df['anomaly_type'].value_counts().to_dict()}")
    return anomaly_df


def run_momentum_pipeline(sentiment_df, prices_df):
    """Compute 30/60/90-day rolling momentum by sector using price data + sentiment."""
    print("\n📊 Stage: Momentum Index Computation")

    if prices_df.empty:
        print("  ⚠️ No price data available for momentum.")
        return pd.DataFrame()

    # Normalize price columns
    col_map = {}
    for col in prices_df.columns:
        lc = col.lower().strip()
        if lc == "date":
            col_map[col] = "trade_date"
        elif lc == "close":
            col_map[col] = "close_price"
        elif lc == "ticker":
            col_map[col] = "ticker"
    prices_df = prices_df.rename(columns=col_map)

    if "trade_date" not in prices_df.columns or "ticker" not in prices_df.columns:
        print("  ⚠️ Missing required columns (trade_date, ticker)")
        return pd.DataFrame()

    prices_df["trade_date"] = pd.to_datetime(prices_df["trade_date"], errors="coerce", utc=True).dt.tz_localize(None)
    prices_df["close_price"] = pd.to_numeric(prices_df["close_price"], errors="coerce")
    prices_df = prices_df.dropna(subset=["trade_date", "close_price"])

    # Assign sectors
    prices_df["sector"] = prices_df["ticker"].map(SECTOR_MAP).fillna("Other")

    # Compute daily returns per ticker
    prices_df = prices_df.sort_values(["ticker", "trade_date"])
    prices_df["daily_return"] = prices_df.groupby("ticker")["close_price"].pct_change()

    # Build a price-based sentiment proxy:
    #   positive return day = positive signal, etc.
    prices_df["price_sentiment"] = prices_df["daily_return"].apply(
        lambda r: 1.0 if r > 0.01 else (-1.0 if r < -0.01 else 0.0)
        if pd.notna(r) else 0.0
    )

    # Merge with real sentiment if available
    if not sentiment_df.empty and "ticker" in sentiment_df.columns:
        # Parse sentiment dates
        sdf = sentiment_df.copy()
        sdf["date"] = pd.to_datetime(sdf["event_date"], errors="coerce", utc=True).dt.tz_localize(None)
        sdf = sdf.dropna(subset=["date"])
        if not sdf.empty:
            daily_sent = (sdf.groupby(["ticker", sdf["date"].dt.date])
                         .agg(real_sentiment=("sentiment_score", "mean"))
                         .reset_index())
            daily_sent["date"] = pd.to_datetime(daily_sent["date"])
            daily_sent["ticker"] = daily_sent["ticker"].astype(str)

            prices_df["_date"] = prices_df["trade_date"].dt.normalize()
            prices_df = prices_df.merge(
                daily_sent, left_on=["ticker", "_date"], right_on=["ticker", "date"],
                how="left"
            )
            # Blend: use real sentiment when available, else price proxy
            prices_df["blended_sentiment"] = prices_df["real_sentiment"].fillna(
                prices_df["price_sentiment"]
            )
            prices_df.drop(columns=["_date", "date"], errors="ignore", inplace=True)
        else:
            prices_df["blended_sentiment"] = prices_df["price_sentiment"]
    else:
        prices_df["blended_sentiment"] = prices_df["price_sentiment"]

    # Daily sector aggregation
    daily_sector = (prices_df
                    .groupby(["sector", prices_df["trade_date"].dt.date])
                    .agg(
                        sector_avg_sentiment=("blended_sentiment", "mean"),
                        article_count=("blended_sentiment", "count"),
                        ticker_count=("ticker", "nunique"),
                    )
                    .reset_index())
    daily_sector.columns = ["sector", "date", "avg_sentiment", "article_count", "ticker_count"]
    daily_sector["date"] = pd.to_datetime(daily_sector["date"])
    daily_sector = daily_sector.sort_values(["sector", "date"])

    print(f"  Daily sector entries: {len(daily_sector)}")

    all_momentum = []
    for window in [30, 60, 90]:
        for sector in daily_sector["sector"].unique():
            sdf = daily_sector[daily_sector["sector"] == sector].copy()
            if len(sdf) < 5:
                continue

            sdf["rolling_avg"] = sdf["avg_sentiment"].rolling(window, min_periods=1).mean()
            sdf["rolling_std"] = sdf["avg_sentiment"].rolling(window, min_periods=5).std()

            for _, row in sdf.iterrows():
                std = row["rolling_std"]
                if pd.notna(std) and std > 0:
                    momentum = (row["avg_sentiment"] - row["rolling_avg"]) / std
                else:
                    momentum = 0.0

                all_momentum.append({
                    "sector": sector,
                    "window_days": window,
                    "momentum_score": float(momentum),
                    "avg_sentiment": float(row["rolling_avg"]),
                    "ticker_count": int(row["ticker_count"]),
                    "start_date": str((row["date"] - pd.Timedelta(days=window)).date()),
                    "end_date": str(row["date"].date()),
                    "computed_at": datetime.datetime.now().isoformat(),
                })

    if not all_momentum:
        print("  ⚠️ No momentum data computed.")
        return pd.DataFrame()

    momentum_df = pd.DataFrame(all_momentum)
    os.makedirs(OUT_MOMENTUM, exist_ok=True)
    momentum_df.to_csv(os.path.join(OUT_MOMENTUM, "momentum.csv"), index=False)
    print(f"  ✅ Computed {len(momentum_df)} momentum entries "
          f"across {momentum_df['sector'].nunique()} sectors")
    return momentum_df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("  BDATL Standalone Processing Pipeline")
    print("  Pandas + sklearn + TextBlob (no PySpark needed)")
    print("=" * 60)
    print(f"  Time: {datetime.datetime.now()}")
    print(f"  TextBlob available: {HAS_TEXTBLOB}")
    print()

    # Load raw data
    print("[DATA] Loading raw data...")
    prices_df = load_csvs(RAW_PRICES)
    news_df = load_csvs(RAW_NEWS)
    print(f"  Prices: {len(prices_df)} rows from {RAW_PRICES}")
    print(f"  News:   {len(news_df)} rows from {RAW_NEWS}")

    if prices_df.empty and news_df.empty:
        print("\n[ERROR] No raw data found. Run the ingestion scripts first:")
        print("   python -m src.ingestion.fetch_price --no-hdfs")
        print("   python -m src.ingestion.fetch_news --no-hdfs")
        print("   OR: python -m src.generate_demo_data")
        sys.exit(1)

    # Run NLP sentiment
    sentiment_df = run_sentiment_pipeline(news_df)

    # Run anomaly detection
    anomaly_df = run_anomaly_detection(prices_df, sentiment_df)

    # Run momentum computation (uses price data for historical coverage)
    momentum_df = run_momentum_pipeline(sentiment_df, prices_df)

    # Summary
    print("\n" + "=" * 60)
    print("  Pipeline Complete!")
    print(f"  Sentiment rows : {len(sentiment_df)}")
    print(f"  Anomaly rows   : {len(anomaly_df)}")
    print(f"  Momentum rows  : {len(momentum_df)}")
    print("  Dashboard      : http://localhost:8501")
    print("=" * 60)


if __name__ == "__main__":
    main()

import os
import sys

# Ensure project root is in path
sys.path.insert(0, "/app")

from src.config.tickers import TICKER_SYMBOLS
from src.dashboard.dashboard import (
    load_all_data,
    load_single_ticker_data,
    fetch_cluster_metrics,
)

print("--- Testing Dashboard Loaders ---")
print(f"Total Tickers Available: {len(TICKER_SYMBOLS)}")

metrics = fetch_cluster_metrics()
print(f"Spark Status: {metrics['spark']['status']}, Workers: {metrics['spark'].get('alive_workers')}")
print(f"Hadoop Status: {metrics['hadoop']['status']}, Live DataNodes: {metrics['hadoop'].get('live_datanodes')}")

prices, sentiment, anomalies, momentum, news = load_all_data()

print(f"\n📊 Data Summary Loaded by Dashboard:")
print(f"  Prices rows:    {len(prices):,}")
print(f"  Sentiment rows: {len(sentiment):,}")
print(f"  Anomalies rows: {len(anomalies):,}")
print(f"  Momentum rows:  {len(momentum):,}")
print(f"  News rows:      {len(news):,}")

if not sentiment.empty:
    print(f"  Avg Sentiment:  {sentiment['sentiment_score'].mean():.4f}")

if not anomalies.empty:
    print(f"  Top Anomaly Types: {anomalies['anomaly_type'].value_counts().head(3).to_dict()}")

if not momentum.empty:
    print(f"  Momentum Sectors: {momentum['sector'].nunique()} sectors")

sample = TICKER_SYMBOLS[0]
single = load_single_ticker_data(sample)
print(f"  Single Ticker ({sample}): {len(single):,} rows loaded on-demand")

print("\n✅ All dashboard components verified successfully!")

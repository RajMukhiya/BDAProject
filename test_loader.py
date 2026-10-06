from src.dashboard.dashboard import (
    load_sentiment_data,
    load_anomalies_data,
    load_momentum_data,
    load_ticker_symbols,
    load_single_ticker_data,
)

print("--- Testing Dashboard Loaders ---")
tickers = load_ticker_symbols()
print(f"Total Tickers Available: {len(tickers)}")

sent = load_sentiment_data()
print(f"Sentiment records loaded: {len(sent) if sent is not None else 0}")
if sent is not None and not sent.empty:
    print(f"Sentiment columns: {list(sent.columns)}")
    print(f"Avg sentiment: {sent['sentiment_score'].mean():.4f}")

anom = load_anomalies_data()
print(f"Anomalies records loaded: {len(anom) if anom is not None else 0}")
if anom is not None and not anom.empty:
    print(f"Anomalies columns: {list(anom.columns)}")
    print(f"Top anomaly types: {anom['anomaly_type'].value_counts().to_dict()}")

mom = load_momentum_data()
print(f"Momentum records loaded: {len(mom) if mom is not None else 0}")
if mom is not None and not mom.empty:
    print(f"Momentum columns: {list(mom.columns)}")
    print(f"Sectors in momentum: {mom['sector'].unique().tolist()}")

sample_ticker = tickers[0]
single = load_single_ticker_data(sample_ticker)
print(f"Single ticker ({sample_ticker}) rows: {len(single) if single is not None else 0}")
print("--- Test Complete ---")

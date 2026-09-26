"""
generate_demo_data.py — Synthetic Indian Market Data Generator
==============================================================
Generates realistic OHLCV price data and financial news for the BDATL engine.
Use this when Yahoo Finance / network is unavailable.

Usage:
    python -m src.generate_demo_data
"""

import os
import sys
import datetime
import numpy as np
import pandas as pd
from pathlib import Path

# Resolve data root
def _resolve_data_root() -> Path:
    docker_root = Path("/app/data")
    if docker_root.exists():
        return docker_root
    here = Path(__file__).resolve()
    for parent in [here.parent.parent, here.parent.parent.parent]:
        candidate = parent / "data"
        if candidate.exists():
            return candidate
    return Path("data")

_DATA_ROOT = _resolve_data_root()
OUT_PRICES    = _DATA_ROOT / "raw" / "prices"
OUT_NEWS      = _DATA_ROOT / "raw" / "news"

# -----------------------------------------------------------------------
# Indian Stock Universe (20 major NSE stocks)
# -----------------------------------------------------------------------
STOCKS = {
    "RELIANCE":  {"sector": "Energy",               "start_price": 2400,  "volatility": 0.018},
    "TCS":       {"sector": "Information Technology","start_price": 3600,  "volatility": 0.015},
    "HDFCBANK":  {"sector": "Banking",              "start_price": 1650,  "volatility": 0.014},
    "ICICIBANK": {"sector": "Banking",              "start_price": 980,   "volatility": 0.016},
    "INFY":      {"sector": "Information Technology","start_price": 1520,  "volatility": 0.017},
    "WIPRO":     {"sector": "Information Technology","start_price": 450,   "volatility": 0.020},
    "HCLTECH":   {"sector": "Information Technology","start_price": 1350,  "volatility": 0.018},
    "SBIN":      {"sector": "Banking",              "start_price": 620,   "volatility": 0.022},
    "KOTAKBANK": {"sector": "Banking",              "start_price": 1750,  "volatility": 0.015},
    "TATASTEEL": {"sector": "Metals & Mining",      "start_price": 130,   "volatility": 0.030},
    "NTPC":      {"sector": "Power",                "start_price": 240,   "volatility": 0.019},
    "ONGC":      {"sector": "Oil Gas & Consumable Fuels","start_price": 165, "volatility": 0.021},
    "POWERGRID": {"sector": "Power",                "start_price": 250,   "volatility": 0.016},
    "SUNPHARMA": {"sector": "Healthcare",           "start_price": 1100,  "volatility": 0.020},
    "DRREDDY":   {"sector": "Healthcare",           "start_price": 5400,  "volatility": 0.022},
    "MARUTI":    {"sector": "Automobile and Auto Components","start_price": 10000, "volatility": 0.018},
    "TATAMOTORS":{"sector": "Automobile and Auto Components","start_price": 840,   "volatility": 0.028},
    "BAJFINANCE":{"sector": "Financial Services",   "start_price": 7000,  "volatility": 0.025},
    "ADANIENT":  {"sector": "Energy",               "start_price": 2500,  "volatility": 0.035},
    "ADANIPORTS":{"sector": "Capital Goods",        "start_price": 1100,  "volatility": 0.028},
}

# -----------------------------------------------------------------------
# Events that cause price shocks (for realistic patterns)
# -----------------------------------------------------------------------
EVENTS = [
    # (date, ticker, shock_factor, description)
    ("2023-01-26", "ADANIENT",  -0.15, "Hindenburg Research short report released"),
    ("2023-01-26", "ADANIPORTS",-0.12, "Hindenburg Research contagion effect"),
    ("2023-03-15", "ADANIENT",   0.08, "Adani Group announces buyback plan"),
    ("2022-02-24", "TATASTEEL",  -0.08, "Russia-Ukraine war impact on steel prices"),
    ("2022-05-10", "BAJFINANCE", -0.07, "RBI rate hike 40bps surprise announcement"),
    ("2024-06-04", "SBIN",       -0.09, "Election result uncertainty — PSU selloff"),
    ("2024-06-04", "NTPC",       -0.08, "PSU stocks sell-off on election day"),
    ("2023-07-14", "TCS",         0.06, "TCS Q1FY24 earnings beat expectations"),
    ("2023-10-12", "INFY",       -0.07, "Infosys FY24 guidance cut"),
    ("2024-01-18", "RELIANCE",    0.04, "Jio Financial Services demerger gains"),
]

rng = np.random.default_rng(42)


def generate_price_series(ticker: str, info: dict, start: datetime.date, end: datetime.date) -> pd.DataFrame:
    """Generate realistic OHLCV data using GBM with event shocks."""
    dates = pd.bdate_range(start, end)  # business days only
    n = len(dates)

    price = info["start_price"]
    vol = info["volatility"]
    mu = 0.0003  # small daily drift

    closes = []
    current = price
    for i, d in enumerate(dates):
        shock = 0.0
        d_str = str(d.date())
        for ev_date, ev_ticker, ev_shock, _ in EVENTS:
            if ev_ticker == ticker and d_str == ev_date:
                shock = ev_shock
        ret = rng.normal(mu, vol) + shock
        current = max(current * (1 + ret), 1.0)
        closes.append(current)

    closes = np.array(closes)
    # Simulate OHLV from closes
    opens   = closes * (1 + rng.normal(0, vol * 0.3, n))
    highs   = closes * (1 + np.abs(rng.normal(0, vol * 0.5, n)))
    lows    = closes * (1 - np.abs(rng.normal(0, vol * 0.5, n)))
    volumes = rng.integers(500_000, 10_000_000, n).astype(float)

    # Volume spikes on event days
    for ev_date, ev_ticker, _, _ in EVENTS:
        if ev_ticker == ticker:
            mask = (dates.date == datetime.date.fromisoformat(ev_date))
            volumes[mask] *= rng.uniform(2.5, 5.0)

    df = pd.DataFrame({
        "Date":   dates.date,
        "Open":   np.round(opens, 2),
        "High":   np.round(highs, 2),
        "Low":    np.round(lows, 2),
        "Close":  np.round(closes, 2),
        "Volume": volumes.astype(int),
        "Ticker": ticker,
    })
    return df


# -----------------------------------------------------------------------
# Synthetic News Headlines
# -----------------------------------------------------------------------
NEWS_TEMPLATES = [
    ("{ticker} shares surge {pct}% as Q{q} results beat analyst estimates; revenue up {rev}%",   "positive"),
    ("{ticker} reports strong {q}Q earnings; EBITDA margin expands to {m}%",                       "positive"),
    ("Analysts upgrade {ticker} to 'Buy'; target price raised to ₹{tp}",                          "positive"),
    ("{ticker} secures ₹{val}Cr order; management outlook remains bullish",                         "positive"),
    ("{ticker} announces ₹{val}Cr share buyback at ₹{tp} per share",                               "positive"),
    ("{ticker} Q{q} profit rises {pct}%; stock hits 52-week high",                                 "positive"),
    ("{ticker} dips {pct}% amid broad market sell-off; analysts cite FII outflows",                "negative"),
    ("{ticker} misses Q{q} earnings estimate; margin pressure weighs on outlook",                   "negative"),
    ("Analysts cut {ticker} target price amid rising input cost concerns",                          "negative"),
    ("{ticker} faces regulatory headwind; stock falls {pct}% on SEBI notice",                      "negative"),
    ("{ticker} reports flat Q{q} revenue; management guides cautiously for H2",                     "negative"),
    ("Nifty50 closes higher; {ticker} among top gainers with {pct}% daily return",                 "positive"),
    ("Indian markets mixed; {ticker} steady as global cues remain uncertain",                       "neutral"),
    ("RBI policy holds rates steady; {ticker} likely to benefit from credit growth",                "neutral"),
    ("{ticker} board meeting scheduled; investors watch for dividend announcement",                   "neutral"),
]

SECTORS_NEWS = {
    "Information Technology": ["deal wins", "attrition eases", "digital transformation", "cloud revenue"],
    "Banking": ["NIM expansion", "credit growth", "asset quality", "loan book"],
    "Energy": ["crude oil prices", "refining margin", "capex plans", "gas production"],
    "Healthcare": ["USFDA approval", "drug launch", "biosimilar", "API exports"],
    "Automobile and Auto Components": ["EV transition", "volume growth", "PLI scheme", "chip shortage"],
    "Metals & Mining": ["steel prices", "iron ore costs", "capacity expansion", "exports"],
}


def generate_news(stocks: dict, start: datetime.date, end: datetime.date, n_articles: int = 400) -> pd.DataFrame:
    """Generate synthetic financial news articles."""
    rows = []
    tickers = list(stocks.keys())
    all_dates = pd.date_range(start, end, freq="D")

    for _ in range(n_articles):
        ticker = rng.choice(tickers)
        info = stocks[ticker]
        d = pd.Timestamp(rng.choice(all_dates))
        tmpl, label = NEWS_TEMPLATES[rng.integers(len(NEWS_TEMPLATES))]

        title = tmpl.format(
            ticker=ticker,
            pct=round(float(rng.uniform(1, 8)), 1),
            q=rng.integers(1, 5),
            m=round(float(rng.uniform(15, 35)), 1),
            tp=int(rng.uniform(info["start_price"] * 0.8, info["start_price"] * 1.4)),
            val=int(rng.uniform(100, 5000)),
            rev=round(float(rng.uniform(5, 25)), 1),
        )

        sector = info.get("sector", "Other")
        sector_kws = SECTORS_NEWS.get(sector, ["market", "sector"])
        summary = f"{ticker} {sector} sector update: {rng.choice(sector_kws)} drives {label} outlook for investors."

        rows.append({
            "title": title,
            "summary": summary,
            "link": f"https://economictimes.indiatimes.com/{ticker.lower()}-{d.strftime('%Y%m%d%H%M%S')}",
            "published": d.strftime("%Y-%m-%d"),
            "source": rng.choice(["Economic Times", "Moneycontrol", "LiveMint", "NDTV Profit"]),
            "matched_tickers": ticker,
            "fetched_at": datetime.datetime.now().isoformat(),
        })

    return pd.DataFrame(rows).sort_values("published").reset_index(drop=True)


def main():
    print("=" * 60)
    print("  BDATL Demo Data Generator")
    print("  Generating 2-year synthetic Indian market data...")
    print("=" * 60)

    end   = datetime.date.today()
    start = end - datetime.timedelta(days=730)

    # Generate price data
    print(f"\n[PRICES] Generating OHLCV data for {len(STOCKS)} stocks ({start} to {end})...")
    OUT_PRICES.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    for ticker, info in STOCKS.items():
        df = generate_price_series(ticker, info, start, end)
        path = OUT_PRICES / f"{ticker}_NS.csv"
        df.to_csv(path, index=False)
        total_rows += len(df)
        print(f"  OK {ticker}: {len(df)} rows -> {path.name}")

    print(f"\n  Total price rows: {total_rows:,}")

    # Generate news
    print(f"\n[NEWS] Generating 400 synthetic news articles...")
    OUT_NEWS.mkdir(parents=True, exist_ok=True)
    news_df = generate_news(STOCKS, start, end, n_articles=400)
    news_path = OUT_NEWS / f"news_demo_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    news_df.to_csv(news_path, index=False)
    print(f"  OK News articles: {len(news_df)} -> {news_path.name}")

    print("\n" + "=" * 60)
    print("  DONE: Demo Data Generation Complete!")
    print(f"  Prices: {total_rows:,} rows for {len(STOCKS)} tickers")
    print(f"  News: {len(news_df)} articles")
    print("  Next: python -m src.pipeline_standalone")
    print("=" * 60)


if __name__ == "__main__":
    main()

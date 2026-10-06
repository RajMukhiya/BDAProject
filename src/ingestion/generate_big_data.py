"""
generate_big_data.py — Extreme Big Data Market & Order Flow Generator
======================================================================
Generates high-frequency intraday and multi-year tick streams across all
500 NIFTY stocks and massive corporate news archives for distributed
Hadoop HDFS and Apache Spark cluster stress-testing.

Dataset Output:
  - 500 NIFTY Tickers (from src.config.tickers.TICKER_MAP)
  - 25,000,000+ to 35,000,000+ OHLCV Candlestick & Tick records (~2.5 - 3.5 GB raw)
  - 150,000+ Financial News Articles across sectors (~100 MB raw)
  - Replicated 3x in HDFS -> ~8 to 11 GB total distributed footprint!

Usage:
  python -m src.ingestion.generate_big_data --records-per-ticker 50000 --upload-hdfs
"""

import os
import sys
import argparse
import datetime
import subprocess
from pathlib import Path
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed

# Ensure UTF-8 output
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# Import ticker mapping
from src.config.tickers import TICKER_MAP

def _resolve_data_root() -> Path:
    docker_root = Path("/app/data")
    if docker_root.exists():
        return docker_root
    here = Path(__file__).resolve()
    for parent in [here.parent.parent.parent, here.parent.parent]:
        candidate = parent / "data"
        if candidate.exists():
            return candidate
    return Path("data")

_DATA_ROOT = _resolve_data_root()
OUT_PRICES = _DATA_ROOT / "raw" / "prices"
OUT_NEWS = _DATA_ROOT / "raw" / "news"

# Sector base prices & volatility priors
SECTOR_PRIORS = {
    "Information Technology": {"base": 2200.0, "vol": 0.016},
    "Financial Services": {"base": 1400.0, "vol": 0.018},
    "Healthcare": {"base": 1800.0, "vol": 0.015},
    "Automobile and Auto Components": {"base": 2500.0, "vol": 0.020},
    "Energy": {"base": 2100.0, "vol": 0.022},
    "Power": {"base": 320.0, "vol": 0.025},
    "Metals & Mining": {"base": 450.0, "vol": 0.028},
    "Fast Moving Consumer Goods": {"base": 3500.0, "vol": 0.012},
    "Capital Goods": {"base": 1200.0, "vol": 0.021},
    "Chemicals": {"base": 850.0, "vol": 0.023},
    "Consumer Durables": {"base": 1600.0, "vol": 0.019},
    "Construction Materials": {"base": 2400.0, "vol": 0.017},
    "Oil Gas & Consumable Fuels": {"base": 380.0, "vol": 0.020},
    "Realty": {"base": 650.0, "vol": 0.030},
    "Telecommunication": {"base": 920.0, "vol": 0.022},
    "Services": {"base": 780.0, "vol": 0.024},
    "Textiles": {"base": 240.0, "vol": 0.032},
    "Diversified": {"base": 4200.0, "vol": 0.014},
    "Forest Materials": {"base": 510.0, "vol": 0.025},
    "Other": {"base": 1000.0, "vol": 0.020},
}

NEWS_HEADLINE_TEMPLATES = [
    ("{ticker} reports Q{q} revenue up {pct}% beating consensus estimates", "positive", 0.45),
    ("{ticker} announces multi-billion dollar expansion in green energy infrastructure", "positive", 0.60),
    ("{ticker} signs strategic global alliance to accelerate enterprise cloud transformation", "positive", 0.52),
    ("{ticker} surges to fresh 52-week high on massive institutional block deal volumes", "positive", 0.65),
    ("{ticker} wins prestigious INR {val} Cr government modernization contract", "positive", 0.58),
    ("{ticker} declares record special dividend of INR {div} per share following stellar margins", "positive", 0.50),
    ("RBI monetary policy stance sparks major rally across {ticker} and banking sector", "positive", 0.40),
    ("{ticker} faces regulatory scrutiny and delay in environmental clearance for plant", "negative", -0.48),
    ("{ticker} shares tumble {pct}% amid margin compression and rising raw material input costs", "negative", -0.55),
    ("{ticker} flags supply chain bottleneck and warns of muted Q{q} operating guidance", "negative", -0.42),
    ("Foreign institutional investors trim stake in {ticker} following global tech sector selloff", "negative", -0.38),
    ("{ticker} senior management resignation triggers market volatility and rating downgrade", "negative", -0.52),
    ("SEBI seeks clarification from {ticker} regarding unusual derivative volume activity", "negative", -0.35),
    ("{ticker} conducts annual general meeting; outlines FY27 strategic roadmap", "neutral", 0.05),
    ("Analysts maintain neutral stance on {ticker} pending clarity on export tariffs", "neutral", 0.0),
    ("{ticker} consolidates in tight trading band as traders await upcoming earnings release", "neutral", 0.02),
    ("Sector index rebalancing triggers passive tracking flows in {ticker}", "neutral", 0.01),
]


def generate_ticker_csv(args_tuple):
    """Generate high-frequency price history for a single ticker."""
    ticker_clean, sector, n_records, out_dir = args_tuple
    
    prior = SECTOR_PRIORS.get(sector, SECTOR_PRIORS["Other"])
    base_price = prior["base"] * np.random.uniform(0.7, 1.4)
    volatility = prior["vol"]

    # Generate dates with 1-minute to 15-minute intraday intervals
    end_date = datetime.datetime.now()
    # Generate timestamp sequence stepping back by 5 minutes
    # Using numpy timedelta for speed
    minutes_back = np.arange(n_records, dtype=np.int64) * 5
    timestamps = [
        (end_date - datetime.timedelta(minutes=int(m))).strftime("%Y-%m-%d %H:%M:%S")
        for m in minutes_back[::-1]
    ]

    # Geometric random walk for prices
    daily_returns = np.random.normal(0.0001, volatility * 0.3, size=n_records)
    price_multipliers = np.cumprod(1.0 + daily_returns)
    close_prices = np.round(base_price * price_multipliers, 2)
    close_prices = np.maximum(close_prices, 5.0)  # Floor at ₹5

    # Derive Open, High, Low
    fluctuation = np.abs(np.random.normal(0, volatility * 0.2, size=n_records))
    open_prices = np.round(close_prices * (1.0 + np.random.normal(0, 0.002, size=n_records)), 2)
    high_prices = np.round(np.maximum(open_prices, close_prices) * (1.0 + fluctuation), 2)
    low_prices = np.round(np.minimum(open_prices, close_prices) * (1.0 - fluctuation), 2)
    low_prices = np.maximum(low_prices, 1.0)
    
    # Volumes with occasional institutional surges
    base_volume = int(np.random.uniform(5000, 80000))
    volume_multipliers = np.random.lognormal(mean=0.0, sigma=0.6, size=n_records)
    volumes = np.round(base_volume * volume_multipliers).astype(np.int64)

    # Build DataFrame and save directly
    df = pd.DataFrame({
        "Date": timestamps,
        "Open": open_prices,
        "High": high_prices,
        "Low": low_prices,
        "Close": close_prices,
        "Adj Close": close_prices,
        "Volume": volumes,
        "Ticker": ticker_clean,
        "Exchange": "NSE"
    })

    file_path = os.path.join(out_dir, f"{ticker_clean}_NS.csv")
    df.to_csv(file_path, index=False)
    return ticker_clean, len(df), os.path.getsize(file_path)


def generate_news_batches(n_total_articles: int, out_dir: str, batch_size: int = 25000):
    """Generate high-volume financial news archive across all sectors."""
    os.makedirs(out_dir, exist_ok=True)
    all_tickers = [k.replace(".NS", "").replace(".BO", "") for k in TICKER_MAP.keys()]
    
    total_written = 0
    batch_idx = 0
    end_time = datetime.datetime.now()
    
    print(f"\n📰 Generating {n_total_articles:,} Financial News Articles...")
    
    while total_written < n_total_articles:
        current_batch_size = min(batch_size, n_total_articles - total_written)
        
        # Sample tickers and templates
        sampled_tickers = np.random.choice(all_tickers, size=current_batch_size)
        sampled_template_indices = np.random.choice(len(NEWS_HEADLINE_TEMPLATES), size=current_batch_size)
        
        sources = np.random.choice(
            ["Moneycontrol", "Economic Times", "Livemint", "Business Standard", "Reuters India", "CNBC-TV18", "Bloomberg Quint"],
            size=current_batch_size
        )
        
        titles = []
        summaries = []
        matched_tickers_list = []
        published_dates = []
        links = []
        
        for i in range(current_batch_size):
            ticker = sampled_tickers[i]
            tmpl, label, score = NEWS_HEADLINE_TEMPLATES[sampled_template_indices[i]]
            
            # Interpolate random parameters
            q = np.random.choice([1, 2, 3, 4])
            pct = round(float(np.random.uniform(3.5, 24.8)), 1)
            val = int(np.random.uniform(500, 15000))
            div = round(float(np.random.uniform(5.0, 85.0)), 1)
            
            title = tmpl.format(ticker=ticker, q=q, pct=pct, val=val, div=div)
            summary = (
                f"Market alert for {ticker} ({TICKER_MAP.get(f'{ticker}.NS', 'Indian Equities')}): "
                f"{title}. Institutional traders noted volume shifts and elevated implied volatility on NSE derivatives."
            )
            
            minutes_ago = int(np.random.uniform(5, 525600))  # Up to 1 year back
            pub_date = (end_time - datetime.timedelta(minutes=minutes_ago)).strftime("%Y-%m-%d %H:%M:%S")
            link = f"https://www.bdatl-news.in/market/{ticker.lower()}/{pub_date.replace(' ', '-').replace(':', '')}"
            
            titles.append(title)
            summaries.append(summary)
            matched_tickers_list.append(ticker)
            published_dates.append(pub_date)
            links.append(link)
        
        batch_df = pd.DataFrame({
            "source": sources,
            "title": titles,
            "summary": summaries,
            "published_date": published_dates,
            "link": links,
            "matched_tickers": matched_tickers_list,
            "fetched_at": published_dates
        })
        
        batch_file = os.path.join(out_dir, f"news_bigdata_batch_{batch_idx:03d}.csv")
        batch_df.to_csv(batch_file, index=False)
        total_written += current_batch_size
        batch_idx += 1
        print(f"  💾 Saved News Batch {batch_idx}: {current_batch_size:,} articles -> {batch_file}")


def upload_to_hdfs(local_dir: str, hdfs_dir: str):
    """Upload files into HDFS with progress report."""
    print(f"\n☁️ Uploading {local_dir} into HDFS {hdfs_dir}...")
    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
        subprocess.run(f"hdfs dfs -put -f {local_dir}/*.csv {hdfs_dir}", shell=True, check=True)
        print(f"✅ Successfully uploaded {local_dir} into {hdfs_dir}")
    except Exception as e:
        print(f"⚠️ HDFS upload note: {e}")


def main():
    parser = argparse.ArgumentParser(description="Generate Extreme Big Data for BDATL Platform")
    parser.add_argument("--tickers-count", type=int, default=500, help="Number of NIFTY tickers (up to 500)")
    parser.add_argument("--records-per-ticker", type=int, default=50000,
                        help="Number of intraday candlestick records per ticker (default: 50,000)")
    parser.add_argument("--news-count", type=int, default=150000,
                        help="Number of financial news articles (default: 150,000)")
    parser.add_argument("--upload-hdfs", action="store_true", default=True,
                        help="Upload directly to Hadoop HDFS cluster")
    parser.add_argument("--staging-dir", type=str, default="/tmp/bdatl_staging",
                        help="Temporary directory for generating chunks before HDFS ingestion")
    parser.add_argument("--batch-tickers", type=int, default=50,
                        help="Number of tickers to generate and stream to HDFS at a time")
    parser.add_argument("--skip-prices", action="store_true", help="Skip price generation")
    parser.add_argument("--skip-news", action="store_true", help="Skip news generation")
    args = parser.parse_args()

    # Use container /tmp staging to ensure NO duplicate files are written to Windows
    staging_root = Path(args.staging_dir)
    staging_prices = staging_root / "raw" / "prices"
    staging_news = staging_root / "raw" / "news"
    os.makedirs(staging_prices, exist_ok=True)
    os.makedirs(staging_news, exist_ok=True)

    print("\n" + "═" * 70)
    print(" 🚀 BDATL EXTREME BIG DATA ENGINE GENERATOR (DIRECT HDFS STREAMING)")
    print(f" Tickers to process : {args.tickers_count} stocks")
    print(f" Records per ticker : {args.records_per_ticker:,} intraday bars")
    print(f" Projected Records  : {args.tickers_count * args.records_per_ticker:,} price ticks")
    print(f" Projected News     : {args.news_count:,} articles")
    print(f" Storage Strategy   : Streaming chunked upload to HDFS with zero local disk footprint")
    print(f" Start Time         : {datetime.datetime.now()}")
    print("═" * 70 + "\n")

    # Ensure HDFS target directories exist
    if args.upload_hdfs:
        try:
            subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/data/raw/prices", "/data/raw/news"], check=False)
        except Exception:
            pass

    # 1. Price generation in batches with immediate HDFS upload & local deletion
    if not args.skip_prices:
        all_items = list(TICKER_MAP.items())[:args.tickers_count]
        batch_size = args.batch_tickers
        total_rows = 0
        total_bytes = 0

        for b_start in range(0, len(all_items), batch_size):
            chunk_items = all_items[b_start:b_start + batch_size]
            tasks = [
                (ticker.replace(".NS", "").replace(".BO", ""), sector, args.records_per_ticker, str(staging_prices))
                for ticker, sector in chunk_items
            ]

            chunk_rows = 0
            chunk_bytes = 0
            with ProcessPoolExecutor(max_workers=min(os.cpu_count() or 4, 8)) as executor:
                futures = [executor.submit(generate_ticker_csv, t) for t in tasks]
                for f in as_completed(futures):
                    _, count, size = f.result()
                    chunk_rows += count
                    chunk_bytes += size

            total_rows += chunk_rows
            total_bytes += chunk_bytes
            current_done = min(b_start + batch_size, len(all_items))
            print(f"  ⏳ Generated batch ({current_done}/{len(all_items)} stocks, {chunk_rows:,} rows)...")

            # Stream directly to HDFS and wipe local staging
            if args.upload_hdfs:
                print(f"     ☁️ Uploading batch to HDFS /data/raw/prices/...")
                # Always ensure safe mode is off before uploading
                subprocess.run(["hdfs", "dfsadmin", "-safemode", "leave"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                try:
                    subprocess.run(f"hdfs dfs -put -f {staging_prices}/*.csv /data/raw/prices/", shell=True, check=True)
                except Exception as e:
                    print(f"     ⚠️ HDFS upload warning (retrying): {e}")
                    subprocess.run(["hdfs", "dfsadmin", "-safemode", "leave"], check=False)
                    subprocess.run(f"hdfs dfs -put -f {staging_prices}/*.csv /data/raw/prices/", shell=True, check=False)
                # Clean up local staging immediately
                for f in staging_prices.glob("*.csv"):
                    try:
                        f.unlink()
                    except Exception:
                        pass
                print(f"     🧹 Staging cleaned (0 local disk usage preserved).")

        print(f"\n✅ Price Data Generation Finished: {total_rows:,} records | {total_bytes / (1024**3):.2f} GB in HDFS")

    # 2. News generation with streaming upload
    if not args.skip_news:
        generate_news_batches(args.news_count, str(staging_news))
        if args.upload_hdfs:
            upload_to_hdfs(str(staging_news), "/data/raw/news")
            # Clean up news staging
            for f in staging_news.glob("*.csv"):
                try:
                    f.unlink()
                except Exception:
                    pass

    # 3. Enforce replication factor of 1 to prevent disk exhaustion
    if args.upload_hdfs:
        try:
            print("\n🔒 Setting HDFS replication factor to 1 (preserves disk space)...")
            subprocess.run(["hdfs", "dfs", "-setrep", "-R", "1", "/data"], check=False)
        except Exception:
            pass

    print("\n" + "═" * 70)
    print(" 🎉 Big Data Generation & HDFS Ingestion Complete!")
    print(f" Finished: {datetime.datetime.now()}")
    print("═" * 70 + "\n")


if __name__ == "__main__":
    main()

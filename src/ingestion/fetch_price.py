"""
fetch_price.py — OHLCV Price Data Ingestion for Indian Stocks (NSE/BSE)
========================================================================
Uses yfinance to download historical price data for major Indian tickers
and writes the output as CSV files (local + HDFS upload).

Usage:
    python -m src.ingestion.fetch_price [--tickers RELIANCE.NS TCS.NS ...]
    python -m src.ingestion.fetch_price --period 2y
"""

import os
import sys
import argparse
import datetime
import subprocess
import yfinance as yf
import pandas as pd

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# ---------------------------------------------------------------------------
# Default Indian stock tickers (NSE)
# ---------------------------------------------------------------------------
from pathlib import Path

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
OUTPUT_DIR = str(_DATA_ROOT / "raw" / "prices")
HDFS_DIR = "/data/raw/prices"

from src.config.tickers import TICKER_LIST
DEFAULT_TICKERS = TICKER_LIST

def fetch_ohlcv(tickers: list, period: str = "2y", interval: str = "1d") -> dict:
    """Download OHLCV data for a list of Indian tickers."""
    results = {}
    for ticker in tickers:
        print(f"  📈 Fetching {ticker} (period={period}, interval={interval})...")
        try:
            df = yf.download(ticker, period=period, interval=interval, progress=False)
            if df.empty:
                print(f"  ⚠️  No data returned for {ticker}")
                continue
            # Flatten multi-level columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df["Ticker"] = ticker.replace(".NS", "").replace(".BO", "")
            df["Exchange"] = "NSE" if ticker.endswith(".NS") else "BSE"
            df.index.name = "Date"
            results[ticker] = df
            print(f"  ✅ {ticker}: {len(df)} rows fetched")
        except Exception as e:
            print(f"  ❌ Error fetching {ticker}: {e}")
    return results


def save_local(data: dict, output_dir: str):
    """Save each ticker's DataFrame as a CSV file."""
    os.makedirs(output_dir, exist_ok=True)
    for ticker, df in data.items():
        clean_name = ticker.replace(".", "_")
        path = os.path.join(output_dir, f"{clean_name}.csv")
        df.to_csv(path)
        print(f"  💾 Saved {path} ({len(df)} rows)")


def upload_to_hdfs(local_dir: str, hdfs_dir: str):
    """Upload CSV files to HDFS (only works inside the Docker cluster)."""
    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
        subprocess.run(f"hdfs dfs -put -f {local_dir}/*.csv {hdfs_dir}", shell=True, check=True)
        print(f"  ☁️  Uploaded to HDFS: {hdfs_dir}")
    except FileNotFoundError:
        print("  ⚠️  HDFS CLI not available — skipping HDFS upload (local mode).")
    except subprocess.CalledProcessError as e:
        print(f"  ❌ HDFS upload failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Fetch Indian stock OHLCV data")
    parser.add_argument("--tickers", nargs="+", default=DEFAULT_TICKERS,
                        help="List of yfinance tickers (e.g., RELIANCE.NS TCS.NS)")
    parser.add_argument("--period", default="5y", help="Data period (e.g., 1y, 2y, 5y, max)")
    parser.add_argument("--interval", default="1d", help="Data interval (1d, 1wk, 1mo)")
    parser.add_argument("--output-dir", default=OUTPUT_DIR, help="Local output directory")
    parser.add_argument("--hdfs-dir", default=HDFS_DIR, help="HDFS destination directory")
    parser.add_argument("--limit", type=int, default=None,
                        help="Maximum number of tickers to fetch (e.g. 30 for quick pipeline)")
    parser.add_argument("--no-hdfs", action="store_true", help="Skip HDFS upload")
    args = parser.parse_args()

    target_tickers = args.tickers
    if args.limit and args.limit > 0:
        target_tickers = target_tickers[:args.limit]

    print(f"\n{'='*60}")
    print(f" Indian Stock Price Ingestion")
    print(f" Tickers : {len(target_tickers)} stocks (out of {len(args.tickers)} total)")
    print(f" Period  : {args.period}")
    print(f" Time    : {datetime.datetime.now()}")
    print(f"{'='*60}\n")

    data = fetch_ohlcv(target_tickers, period=args.period, interval=args.interval)
    save_local(data, args.output_dir)

    if not args.no_hdfs:
        upload_to_hdfs(args.output_dir, args.hdfs_dir)

    print(f"\n✅ Ingestion complete: {len(data)}/{len(args.tickers)} tickers fetched.\n")


if __name__ == "__main__":
    main()

"""
run_all.py — Full End-to-End Pipeline Orchestrator (Local / Standalone Mode)
==============================================================================
Runs ALL project stages in sequence, connected together:

  Stage 1 ── Ingestion
              ├─ fetch_price.py   (NSE/BSE OHLCV via yfinance)
              ├─ fetch_news.py    (RSS financial news)
              └─ fetch_reddit.py  (Retail Reddit sentiment)

  Stage 2 ── Processing  (pipeline_standalone.py)
              ├─ NLP Sentiment Analysis (TextBlob)
              ├─ Isolation Forest Anomaly Detection
              └─ Sector Momentum MapReduce

  Stage 3 ── Dashboard   (Streamlit on http://localhost:8501)

Usage:
    python run_all.py
    python run_all.py --skip-ingestion   # skip to processing if data exists
    python run_all.py --skip-processing  # skip to dashboard if processed data exists
"""

import os
import sys
import time
import argparse
import datetime
import subprocess
from pathlib import Path

# ── ensure project root on path ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

PYTHON = sys.executable

BANNER = """
╔══════════════════════════════════════════════════════════════╗
║   BDATL — Indian Stock Market Sentiment & Anomaly Engine     ║
║   Full End-to-End Pipeline Orchestrator (Standalone Mode)    ║
╚══════════════════════════════════════════════════════════════╝
"""

SEP  = "─" * 62
SEP2 = "═" * 62


def header(title: str):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(f"  Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(SEP)


def run(cmd: list, label: str) -> int:
    """Run a subprocess, stream output live, return exit code."""
    print(f"\n  ▶  {label}")
    print(f"     CMD: {' '.join(cmd)}\n")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(PROJECT_ROOT),
    )
    for line in proc.stdout:
        print(f"     {line}", end="")
    proc.wait()
    status = "✅ Done" if proc.returncode == 0 else f"⚠️  Exit code {proc.returncode}"
    print(f"\n  {status}: {label}\n")
    return proc.returncode


def check_data_exists(directory: str, min_files: int = 1) -> bool:
    p = Path(directory)
    if not p.exists():
        return False
    files = list(p.glob("*.csv"))
    return len(files) >= min_files


def main():
    parser = argparse.ArgumentParser(description="Full BDATL pipeline orchestrator")
    parser.add_argument("--skip-ingestion",  action="store_true",
                        help="Skip Stage 1 (ingestion) — use existing raw data")
    parser.add_argument("--skip-processing", action="store_true",
                        help="Skip Stage 2 (processing) — go straight to dashboard")
    parser.add_argument("--no-dashboard",    action="store_true",
                        help="Skip Stage 3 (dashboard) — pipeline only, no UI")
    parser.add_argument("--limit-tickers",   type=int, default=None,
                        help="Limit number of tickers fetched (default: all ~30)")
    args = parser.parse_args()

    print(BANNER)
    print(f"  Python  : {PYTHON}")
    print(f"  Root    : {PROJECT_ROOT}")
    print(f"  Time    : {datetime.datetime.now()}")
    print()

    data_root    = PROJECT_ROOT / "data"
    prices_dir   = data_root / "raw" / "prices"
    news_dir     = data_root / "raw" / "news"
    reddit_dir   = data_root / "raw" / "reddit"
    processed_dir = data_root / "processed"

    # ── STAGE 1: INGESTION ──────────────────────────────────────────────────
    if not args.skip_ingestion:
        header("STAGE 1 / 3 — Data Ingestion")

        # 1a. Price data (yfinance)
        price_cmd = [PYTHON, "-m", "src.ingestion.fetch_price", "--no-hdfs"]
        if args.limit_tickers:
            price_cmd += ["--limit", str(args.limit_tickers)]
        run(price_cmd, "Fetch NSE/BSE OHLCV price data (yfinance)")

        # 1b. Financial news (RSS)
        run([PYTHON, "-m", "src.ingestion.fetch_news", "--no-hdfs"],
            "Fetch Indian financial news (RSS feeds)")

        # 1c. Reddit posts (synthetic if no API key)
        run([PYTHON, "-m", "src.ingestion.fetch_reddit"],
            "Fetch Reddit retail sentiment (r/IndianStreetBets etc.)")

        # Verify
        prices_ok = check_data_exists(str(prices_dir))
        news_ok   = check_data_exists(str(news_dir))
        reddit_ok = check_data_exists(str(reddit_dir))
        print(f"\n  📂 Data check after ingestion:")
        print(f"     Prices  : {'✅' if prices_ok else '❌'}  {prices_dir}")
        print(f"     News    : {'✅' if news_ok   else '❌'}  {news_dir}")
        print(f"     Reddit  : {'✅' if reddit_ok else '❌'}  {reddit_dir}")
    else:
        print("\n  ⏩ Skipping Stage 1 (--skip-ingestion flag set)")
        print(f"     Using existing data in: {data_root / 'raw'}")

    # ── STAGE 2: PROCESSING ─────────────────────────────────────────────────
    if not args.skip_processing:
        header("STAGE 2 / 3 — Distributed Processing & ML")
        print("  Modules:")
        print("    🧠 NLP Sentiment Analysis  (TextBlob polarity scoring)")
        print("    🔍 Isolation Forest         (Anomaly Detection, n_estimators=50)")
        print("    📊 Sector Momentum          (30d / 60d / 90d MapReduce)")
        print()

        rc = run([PYTHON, "-m", "src.pipeline_standalone"],
                 "Run standalone processing pipeline (sentiment + anomaly + momentum)")

        # Show processed output summary
        for sub in ["sentiment", "anomalies", "momentum"]:
            d = processed_dir / sub
            if d.exists():
                files = list(d.glob("*.csv"))
                total_rows = 0
                for f in files:
                    try:
                        import pandas as pd
                        total_rows += len(pd.read_csv(f))
                    except Exception:
                        pass
                print(f"     {sub:12s}: {len(files)} file(s), ~{total_rows:,} rows → {d}")
    else:
        print("\n  ⏩ Skipping Stage 2 (--skip-processing flag set)")

    # ── STAGE 3: DASHBOARD ──────────────────────────────────────────────────
    if not args.no_dashboard:
        header("STAGE 3 / 3 — Launching Streamlit Dashboard")
        print()
        print(f"  🌐 Dashboard URL  :  http://localhost:8501")
        print(f"  📂 Data feeds from:  {processed_dir}")
        print()
        print("  Press  Ctrl+C  to stop the dashboard when done.\n")
        print(SEP2)
        print()

        # Launch Streamlit (blocks until Ctrl+C)
        try:
            subprocess.run(
                [PYTHON, "-m", "streamlit", "run",
                 str(PROJECT_ROOT / "src" / "dashboard" / "dashboard.py"),
                 "--server.port", "8501",
                 "--server.headless", "false",
                 "--browser.gatherUsageStats", "false"],
                cwd=str(PROJECT_ROOT),
            )
        except KeyboardInterrupt:
            print("\n\n  🛑 Dashboard stopped by user.")
    else:
        print("\n  ⏩ Skipping Stage 3 (--no-dashboard flag set)")

    # ── DONE ─────────────────────────────────────────────────────────────────
    print()
    print(SEP2)
    print("  ✅ BDATL Pipeline Complete!")
    print(SEP2)
    print()


if __name__ == "__main__":
    main()

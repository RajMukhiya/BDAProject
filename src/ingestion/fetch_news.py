"""
fetch_news.py — Indian Financial News Ingestion via RSS Feeds
=============================================================
Fetches headlines from Moneycontrol, Economic Times, Mint, and NDTV Profit.
Parses RSS feeds and writes structured CSV output (local + HDFS upload).

Usage:
    python -m src.ingestion.fetch_news
    python -m src.ingestion.fetch_news --max-articles 500
"""

import os
import sys
import re
import html
import argparse
import datetime
import subprocess
import pandas as pd
import feedparser

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# ---------------------------------------------------------------------------
# Indian Financial News RSS Feeds
# ---------------------------------------------------------------------------
RSS_FEEDS = {
    "moneycontrol": [
        "https://www.moneycontrol.com/rss/latestnews.xml",
        "https://www.moneycontrol.com/rss/marketreports.xml",
        "https://www.moneycontrol.com/rss/stocksnews.xml",
    ],
    "economic_times": [
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
    ],
    "livemint": [
        "https://www.livemint.com/rss/markets",
        "https://www.livemint.com/rss/money",
    ],
    "ndtv_profit": [
        "https://feeds.feedburner.com/ndtvprofit-latest",
    ],
}

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
OUTPUT_DIR = str(_DATA_ROOT / "raw" / "news")
HDFS_DIR = "/data/raw/news"

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.config.tickers import TICKER_KEYWORDS


def clean_html(raw_text: str) -> str:
    """Strip HTML tags and decode entities."""
    clean = re.sub(r"<[^>]+>", "", raw_text)
    return html.unescape(clean).strip()


def extract_tickers(text: str) -> list:
    """Match article text against known Indian ticker keywords with word boundary matching."""
    text_lower = text.lower()
    matched = []
    for ticker, keywords in TICKER_KEYWORDS.items():
        for kw in keywords:
            kw_clean = kw.strip().lower()
            if not kw_clean:
                continue
            # For short symbols (<= 4 chars, e.g. 'it', 'be', 'lt', 'sbi'), require word boundary to avoid false positives
            if len(kw_clean) <= 4:
                pattern = r'\b' + re.escape(kw_clean) + r'\b'
                if re.search(pattern, text_lower):
                    matched.append(ticker.strip().upper())
                    break
            else:
                if kw_clean in text_lower:
                    matched.append(ticker.strip().upper())
                    break
    return matched


def fetch_feeds(max_articles: int = 1000) -> pd.DataFrame:
    """Parse all RSS feeds and return a structured DataFrame."""
    articles = []
    for source, urls in RSS_FEEDS.items():
        for url in urls:
            print(f"  📰 Fetching {source}: {url}")
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:max_articles]:
                    title = clean_html(entry.get("title", ""))
                    summary = clean_html(entry.get("summary", entry.get("description", "")))
                    published = entry.get("published", entry.get("updated", ""))

                    full_text = f"{title} {summary}"
                    tickers = extract_tickers(full_text)

                    articles.append({
                        "source": source,
                        "title": title,
                        "summary": summary[:500],
                        "published_date": published,  # matches NLP sentiment schema
                        "link": entry.get("link", ""),
                        "matched_tickers": ",".join(tickers) if tickers else "GENERAL",
                        "fetched_at": datetime.datetime.now().isoformat(),
                    })
            except Exception as e:
                print(f"  ❌ Error fetching {url}: {e}")

    df = pd.DataFrame(articles)
    print(f"  ✅ Total articles fetched: {len(df)}")
    return df


def save_local(df: pd.DataFrame, output_dir: str):
    """Save news articles as a timestamped CSV."""
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"news_{ts}.csv")
    df.to_csv(path, index=False)
    print(f"  💾 Saved {path} ({len(df)} articles)")
    return path


def upload_to_hdfs(local_path: str, hdfs_dir: str):
    """Upload CSV to HDFS."""
    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
        subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_dir], check=True)
        print(f"  ☁️  Uploaded to HDFS: {hdfs_dir}")
    except FileNotFoundError:
        print("  ⚠️  HDFS CLI not available — skipping HDFS upload (local mode).")
    except subprocess.CalledProcessError as e:
        print(f"  ❌ HDFS upload failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Fetch Indian financial news via RSS")
    parser.add_argument("--max-articles", type=int, default=1000,
                        help="Max articles per feed")
    parser.add_argument("--output-dir", default=OUTPUT_DIR)
    parser.add_argument("--hdfs-dir", default=HDFS_DIR)
    parser.add_argument("--no-hdfs", action="store_true")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f" Indian Financial News Ingestion")
    print(f" Sources : {len(RSS_FEEDS)} providers, {sum(len(v) for v in RSS_FEEDS.values())} feeds")
    print(f" Time    : {datetime.datetime.now()}")
    print(f"{'='*60}\n")

    df = fetch_feeds(max_articles=args.max_articles)
    if df.empty:
        print("  ⚠️  No articles fetched. Check network/feed URLs.")
        return

    local_path = save_local(df, args.output_dir)

    if not args.no_hdfs:
        upload_to_hdfs(local_path, args.hdfs_dir)

    # Print summary per source
    print(f"\n{'='*60}")
    print(" Summary per source:")
    print(df["source"].value_counts().to_string())
    print(f"\n Ticker Matches:")
    ticker_counts = df["matched_tickers"].str.split(",").explode().value_counts().head(15)
    print(ticker_counts.to_string())
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()

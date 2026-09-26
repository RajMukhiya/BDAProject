"""
dashboard.py — Interactive Streamlit Dashboard for BDATL Engine
================================================================
Provides: ticker search, sentiment-price timelines, anomaly alerts,
sector heatmaps, and historical event replay.

Run inside the master container:
    streamlit run /app/src/dashboard/dashboard.py --server.port 8501
"""

import os
import subprocess
import datetime
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

# ---------------------------------------------------------------------------
# Page Configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="BDATL — Indian Market Sentiment Engine",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Indian Sector Colors
# ---------------------------------------------------------------------------
SECTOR_COLORS = {
    "IT": "#00d2ff", "Banking": "#3a7bd5", "Energy": "#f7971e",
    "FMCG": "#56ab2f", "Pharma": "#e44d26", "Auto": "#8e44ad",
    "Metals": "#7f8c8d", "Finance": "#2ecc71", "Telecom": "#e74c3c",
    "Infrastructure": "#f39c12", "Consumer": "#1abc9c", "Other": "#95a5a6"
}

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    .main { font-family: 'Inter', sans-serif; }
    .stMetric { background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                border-radius: 12px; padding: 16px; border: 1px solid #0f3460; }
    .anomaly-alert { background: linear-gradient(135deg, #ff416c, #ff4b2b);
                     color: white; padding: 16px; border-radius: 12px;
                     margin: 8px 0; font-weight: 500; }
    .positive-badge { background: #00b894; color: white; padding: 4px 12px;
                      border-radius: 20px; font-size: 0.85em; }
    .negative-badge { background: #d63031; color: white; padding: 4px 12px;
                      border-radius: 20px; font-size: 0.85em; }
    .neutral-badge  { background: #636e72; color: white; padding: 4px 12px;
                      border-radius: 20px; font-size: 0.85em; }
    div[data-testid="stSidebar"] { background: linear-gradient(180deg, #0f0c29 0%, #302b63 50%, #24243e 100%); }
    h1, h2, h3 { font-weight: 600; }
    .pipeline-status { padding: 12px; border-radius: 8px; margin: 8px 0; }
    .pipeline-running { background: #fdcb6e; color: #2d3436; }
    .pipeline-done { background: #00b894; color: white; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Data Loading (from HDFS — written by ingestion/ML scripts)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60)
def load_data(hdfs_path):
    """Load CSV files from HDFS using subprocess."""
    import io
    if hdfs_path:
        try:
            if not hdfs_path.endswith("/"):
                hdfs_path += "/"
            
            # Read all CSV parts from the HDFS directory
            result = subprocess.run(
                ["hdfs", "dfs", "-cat", f"{hdfs_path}*.csv"],
                capture_output=True, text=True
            )
            
            if result.returncode == 0 and result.stdout.strip():
                df = pd.read_csv(io.StringIO(result.stdout), on_bad_lines="skip")
                if len(df) > 0:
                    first_col = df.columns[0]
                    df = df[df[first_col] != first_col]
                    # Since headers mixed into data force columns to string type, we cast them back to numeric
                    df = df.apply(pd.to_numeric, errors="ignore")
                return df
            else:
                print(f"HDFS empty or error for {hdfs_path}: {result.stderr}")
        except Exception as e:
            print(f"HDFS read exception for {hdfs_path}: {e}")
    return pd.DataFrame()


def load_all_data():
    """Load all datasets used by the dashboard from HDFS."""
    prices = load_data("hdfs://master:9000/data/raw/prices")
    sentiment = load_data("hdfs://master:9000/data/processed/sentiment")
    anomalies = load_data("hdfs://master:9000/data/processed/anomalies")
    momentum = load_data("hdfs://master:9000/data/processed/momentum")
    news = load_data("hdfs://master:9000/data/raw/news")
    return prices, sentiment, anomalies, momentum, news


def get_pipeline_status():
    """Check if the pipeline is currently running."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "run_pipeline.sh"],
            capture_output=True, text=True
        )
        return result.returncode == 0
    except Exception:
        return False


def run_pipeline_background():
    """Launch the data pipeline in the background."""
    subprocess.Popen(
        ["bash", "/app/run_pipeline.sh"],
        stdout=open("/tmp/pipeline.log", "w"),
        stderr=subprocess.STDOUT,
        cwd="/app"
    )


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
def render_sidebar(prices_df, anomalies_df):
    with st.sidebar:
        st.image("https://img.icons8.com/fluency/96/bull-market.png", width=64)
        st.title("🇮🇳 BDATL Engine")
        st.caption("Real-Time Sentiment & Anomaly Detection")
        st.divider()

        # Pipeline controls
        st.subheader("⚙️ Pipeline Control")
        is_running = get_pipeline_status()
        if is_running:
            st.markdown('<div class="pipeline-status pipeline-running">⏳ Pipeline is running...</div>',
                        unsafe_allow_html=True)
            if st.button("🔄 Refresh Data", use_container_width=True):
                st.cache_data.clear()
                st.rerun()
        else:
            has_data = not prices_df.empty
            if has_data:
                st.markdown('<div class="pipeline-status pipeline-done">✅ Data loaded</div>',
                            unsafe_allow_html=True)
            if st.button("🚀 Run Pipeline" if not has_data else "🔄 Re-run Pipeline",
                         use_container_width=True):
                run_pipeline_background()
                st.toast("Pipeline started! This takes 2-5 minutes. Click Refresh when done.")
                st.rerun()

        st.divider()

        # Ticker selection
        tickers = sorted(prices_df["Ticker"].unique()) if not prices_df.empty and "Ticker" in prices_df.columns else []
        selected_ticker = st.selectbox("🔍 Search Ticker", ["ALL"] + tickers, index=0)

        st.divider()

        # Date range
        st.subheader("📅 Date Range")
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("From", datetime.date(2020, 1, 1))
        with col2:
            end_date = st.date_input("To", datetime.date.today())

        st.divider()

        # Anomaly filter
        anomaly_types = ["All"]
        if not anomalies_df.empty and "anomaly_type" in anomalies_df.columns:
            anomaly_types += sorted(anomalies_df["anomaly_type"].unique())
        selected_anomaly_type = st.selectbox("⚡ Anomaly Type", anomaly_types)

        st.divider()

        # Quick stats
        st.subheader("📊 Quick Stats")
        if not prices_df.empty:
            st.metric("Tickers Tracked", len(tickers))
        if not anomalies_df.empty:
            st.metric("Anomalies Detected", len(anomalies_df))

        return selected_ticker, start_date, end_date, selected_anomaly_type


# ---------------------------------------------------------------------------
# Helper: normalize date/price column names from yfinance CSVs
# ---------------------------------------------------------------------------
def normalize_price_columns(df):
    """Normalize column names to handle yfinance CSV format."""
    rename = {}
    for col in df.columns:
        lc = col.lower().strip()
        if lc == "date" or lc == "trade_date":
            rename[col] = "Date"
        elif lc == "open" or lc == "open_price":
            rename[col] = "Open"
        elif lc == "high" or lc == "high_price":
            rename[col] = "High"
        elif lc == "low" or lc == "low_price":
            rename[col] = "Low"
        elif lc in ("close", "close_price"):
            rename[col] = "Close"
        elif lc == "volume":
            rename[col] = "Volume"
        elif lc == "ticker":
            rename[col] = "Ticker"
    if rename:
        df = df.rename(columns=rename)
    return df


# ---------------------------------------------------------------------------
# Main Dashboard Pages
# ---------------------------------------------------------------------------
def render_overview(prices_df, sentiment_df, anomalies_df, momentum_df):
    """Overview page with key metrics and recent alerts."""
    st.title("📊 Market Sentiment Overview")

    # KPI Row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Stocks Tracked",
                  len(prices_df["Ticker"].unique()) if not prices_df.empty and "Ticker" in prices_df.columns else 0)
    with col2:
        if not sentiment_df.empty and "sentiment_score" in sentiment_df.columns:
            avg_sent = pd.to_numeric(sentiment_df["sentiment_score"], errors="coerce").mean()
            st.metric("Avg Sentiment", f"{avg_sent:.3f}",
                      "Positive" if avg_sent > 0 else "Negative")
        else:
            st.metric("Avg Sentiment", "N/A")
    with col3:
        st.metric("Anomalies (Total)",
                  len(anomalies_df) if not anomalies_df.empty else 0)
    with col4:
        if not anomalies_df.empty and "anomaly_type" in anomalies_df.columns:
            divergences = len(anomalies_df[
                anomalies_df["anomaly_type"].str.contains("divergence", case=False, na=False)
            ])
            st.metric("Divergences", divergences)
        else:
            st.metric("Divergences", 0)

    st.divider()

    # Recent Anomaly Alerts
    col_left, col_right = st.columns([2, 1])
    with col_left:
        st.subheader("⚡ Recent Anomaly Alerts")
        if not anomalies_df.empty:
            recent = anomalies_df.sort_values("anomaly_date", ascending=False).head(10)
            for _, row in recent.iterrows():
                desc = row.get("description", row.get("anomaly_type", "Unknown"))
                st.markdown(
                    '<div class="anomaly-alert">'
                    f'<strong>{row.get("ticker", "?")}</strong> — '
                    f'{row.get("anomaly_date", "?")} | '
                    f'Score: {float(row.get("anomaly_score", 0)):.2f}<br/>'
                    f'{desc}</div>',
                    unsafe_allow_html=True
                )
        else:
            st.info("No anomalies detected yet. Click **Run Pipeline** in the sidebar to start data processing.")

    with col_right:
        st.subheader("🏭 Sector Sentiment")
        if not momentum_df.empty and "sector" in momentum_df.columns:
            latest_momentum = momentum_df[momentum_df["window_days"] == 30]
            if not latest_momentum.empty:
                fig = px.bar(
                    latest_momentum.sort_values("momentum_score"),
                    x="momentum_score", y="sector",
                    orientation="h",
                    color="momentum_score",
                    color_continuous_scale="RdYlGn",
                    labels={"momentum_score": "Momentum (30d)"},
                )
                fig.update_layout(
                    height=400, showlegend=False,
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No momentum data yet. Click **Run Pipeline** in the sidebar.")


def render_ticker_detail(ticker, prices_df, sentiment_df, anomalies_df):
    """Detailed ticker view with price-sentiment overlay."""
    st.title(f"📈 {ticker} — Sentiment & Price Timeline")

    if prices_df.empty:
        st.warning("No price data available. Run the pipeline first.")
        return

    prices_df = normalize_price_columns(prices_df)
    ticker_prices = prices_df[prices_df["Ticker"] == ticker].copy()
    if ticker_prices.empty:
        st.warning(f"No data for ticker: {ticker}")
        return

    # Parse dates
    if "Date" in ticker_prices.columns:
        ticker_prices["Date"] = pd.to_datetime(ticker_prices["Date"], errors="coerce")
        ticker_prices = ticker_prices.dropna(subset=["Date"])
        ticker_prices = ticker_prices.sort_values("Date")

    # Create dual-axis plot: Price + Sentiment
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
        row_heights=[0.7, 0.3],
        subplot_titles=[f"{ticker} Price", "Sentiment Score"]
    )

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=ticker_prices["Date"],
        open=ticker_prices["Open"], high=ticker_prices["High"],
        low=ticker_prices["Low"], close=ticker_prices["Close"],
        name="Price"
    ), row=1, col=1)

    # Anomaly markers
    if not anomalies_df.empty and "ticker" in anomalies_df.columns:
        ticker_anomalies = anomalies_df[anomalies_df["ticker"] == ticker].copy()
        if not ticker_anomalies.empty and "close_price" in ticker_anomalies.columns:
            ticker_anomalies["anomaly_date"] = pd.to_datetime(
                ticker_anomalies["anomaly_date"], errors="coerce"
            )
            fig.add_trace(go.Scatter(
                x=ticker_anomalies["anomaly_date"],
                y=ticker_anomalies["close_price"],
                mode="markers",
                marker=dict(size=12, color="red", symbol="triangle-up"),
                name="Anomaly",
                text=ticker_anomalies.get("description", ticker_anomalies.get("anomaly_type", "")),
            ), row=1, col=1)

    # Sentiment line
    if not sentiment_df.empty and "ticker" in sentiment_df.columns:
        ticker_sentiment = sentiment_df[sentiment_df["ticker"] == ticker].copy()
        if not ticker_sentiment.empty and "sentiment_score" in ticker_sentiment.columns:
            # Parse event_date
            date_col = "event_date" if "event_date" in ticker_sentiment.columns else None
            if date_col:
                ticker_sentiment["date"] = pd.to_datetime(
                    ticker_sentiment[date_col], errors="coerce"
                )
                ticker_sentiment = ticker_sentiment.dropna(subset=["date"])
                if not ticker_sentiment.empty:
                    daily_sent = ticker_sentiment.groupby("date")["sentiment_score"].mean().reset_index()
                    fig.add_trace(go.Scatter(
                        x=daily_sent["date"], y=daily_sent["sentiment_score"],
                        mode="lines+markers", name="Avg Sentiment",
                        line=dict(color="#00d2ff", width=2),
                        marker=dict(size=4),
                    ), row=2, col=1)

                    # Zero line
                    fig.add_hline(y=0, line_dash="dash", line_color="gray", row=2, col=1)

    fig.update_layout(
        height=700,
        plot_bgcolor="rgba(15,15,30,1)",
        paper_bgcolor="rgba(15,15,30,1)",
        font_color="white",
        xaxis_rangeslider_visible=False,
    )
    # Ensure X-axis dates are visible
    fig.update_xaxes(
        type="date",
        tickformat="%b %Y",
        tickangle=-45,
        showticklabels=True,
        row=1, col=1
    )
    fig.update_xaxes(
        type="date",
        tickformat="%b %Y",
        tickangle=-45,
        showticklabels=True,
        row=2, col=1
    )
    st.plotly_chart(fig, use_container_width=True)

    # Quick sentiment summary below chart
    if not sentiment_df.empty and "ticker" in sentiment_df.columns:
        ts = sentiment_df[sentiment_df["ticker"] == ticker]
        if not ts.empty and "sentiment_score" in ts.columns:
            st.subheader("📝 Sentiment Summary")
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.metric("Articles Analyzed", len(ts))
            with c2:
                st.metric("Avg Score", f"{pd.to_numeric(ts['sentiment_score'], errors='coerce').mean():.3f}")
            with c3:
                if "sentiment_label" in ts.columns:
                    pos = len(ts[ts["sentiment_label"] == "positive"])
                    st.metric("Positive", pos)
            with c4:
                if "sentiment_label" in ts.columns:
                    neg = len(ts[ts["sentiment_label"] == "negative"])
                    st.metric("Negative", neg)
        else:
            st.info("No sentiment data for this ticker. Run the NLP pipeline.")
    else:
        st.info("No sentiment data available. Click **Run Pipeline** in the sidebar.")


def render_sector_heatmap(momentum_df, sentiment_df):
    """Sector-level heatmap visualization."""
    st.title("🗺️ Sector Sentiment Heatmap")

    if momentum_df.empty:
        st.warning("No momentum data available yet.")
        st.info("Click **🚀 Run Pipeline** in the sidebar to process data. "
                "The momentum analysis requires the NLP sentiment stage to complete first.")
        return

    # Window selector
    window = st.selectbox("Rolling Window", [30, 60, 90], index=0)
    window_data = momentum_df[momentum_df["window_days"] == window]

    if window_data.empty:
        st.warning(f"No data for {window}-day window.")
        return

    # Pivot for heatmap
    if "end_date" in window_data.columns:
        pivot = window_data.pivot_table(
            values="momentum_score", index="sector",
            columns="end_date", aggfunc="mean"
        )
        fig = px.imshow(
            pivot, color_continuous_scale="RdYlGn",
            labels={"color": "Momentum Score"},
            aspect="auto",
        )
        fig.update_layout(
            height=500,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="white",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Sector comparison bar
    st.subheader(f"Sector Momentum Comparison ({window}d)")
    latest = window_data.sort_values("end_date").groupby("sector").last().reset_index()
    fig2 = px.bar(
        latest.sort_values("momentum_score"),
        x="sector", y="momentum_score",
        color="momentum_score",
        color_continuous_scale="RdYlGn",
    )
    fig2.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font_color="white",
    )
    st.plotly_chart(fig2, use_container_width=True)


def render_event_replay(prices_df, sentiment_df, anomalies_df):
    """Historical event replay — step through landmark market events."""
    st.title("🎬 Event Replay")

    prices_df = normalize_price_columns(prices_df)

    # Landmark Indian market events
    events = {
        "Adani-Hindenburg Crisis (Jan 2023)": ("2023-01-20", "2023-03-31", ["ADANIENT", "ADANIPORTS", "SBIN"]),
        "Yes Bank Crisis (Mar 2020)": ("2020-02-15", "2020-05-15", ["SBIN", "ICICIBANK", "HDFCBANK"]),
        "COVID Crash (Mar 2020)": ("2020-01-15", "2020-06-30", ["RELIANCE", "TCS", "HDFCBANK", "INFY"]),
        "IT Rally Post-COVID (Oct 2020)": ("2020-09-01", "2021-03-31", ["TCS", "INFY", "WIPRO", "HCLTECH"]),
        "Nifty All-Time High (Sep 2024)": ("2024-08-01", "2024-12-31", ["RELIANCE", "TCS", "HDFCBANK"]),
        "Russia-Ukraine Impact (Feb 2022)": ("2022-02-01", "2022-04-30", ["TATASTEEL", "JSWSTEEL", "NTPC", "RELIANCE"]),
        "Banking Sector Rally (2023)": ("2023-03-01", "2023-06-30", ["SBIN", "ICICIBANK", "HDFCBANK", "KOTAKBANK"]),
    }

    selected_event = st.selectbox("Select Market Event", list(events.keys()))
    start, end, tickers = events[selected_event]

    st.info(f"**Event**: {selected_event}  \n"
            f"**Period**: {start} → {end}  \n"
            f"**Key Tickers**: {', '.join(tickers)}")

    if prices_df.empty:
        st.warning("No price data available. Run the pipeline first.")
        return

    # Parse dates for filtering
    if "Date" in prices_df.columns:
        prices_df["Date"] = pd.to_datetime(prices_df["Date"], errors="coerce")
        mask = (
            prices_df["Ticker"].isin(tickers) &
            (prices_df["Date"] >= pd.to_datetime(start)) &
            (prices_df["Date"] <= pd.to_datetime(end))
        )
        event_prices = prices_df[mask]

        if not event_prices.empty and "Close" in event_prices.columns:
            fig = px.line(
                event_prices, x="Date", y="Close", color="Ticker",
                title=f"Price Action: {selected_event}",
            )
            fig.update_layout(
                plot_bgcolor="rgba(15,15,30,1)",
                paper_bgcolor="rgba(15,15,30,1)",
                font_color="white",
                xaxis=dict(tickformat="%d %b %Y", tickangle=-45),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(f"No price data found for {', '.join(tickers)} during {start} → {end}. "
                       f"The price fetch period may not cover this timeframe. "
                       f"Re-run the pipeline with `--period 5y` or `--period max`.")

    # Show anomalies during this period
    if not anomalies_df.empty and "ticker" in anomalies_df.columns:
        anomalies_df["anomaly_date"] = pd.to_datetime(anomalies_df["anomaly_date"], errors="coerce")
        event_anomalies = anomalies_df[
            (anomalies_df["ticker"].isin(tickers)) &
            (anomalies_df["anomaly_date"] >= pd.to_datetime(start)) &
            (anomalies_df["anomaly_date"] <= pd.to_datetime(end))
        ]
        if not event_anomalies.empty:
            st.subheader("⚡ Anomalies During This Event")
            st.dataframe(event_anomalies, use_container_width=True)


# ---------------------------------------------------------------------------
# Main App
# ---------------------------------------------------------------------------
def main():
    # Load data
    prices, sentiment, anomalies, momentum, news = load_all_data()

    # Sidebar
    selected_ticker, start_date, end_date, anomaly_type = render_sidebar(prices, anomalies)

    # Navigation
    page = st.sidebar.radio("📍 Navigate", [
        "🏠 Overview",
        "📈 Ticker Detail",
        "🗺️ Sector Heatmap",
        "🎬 Event Replay",
    ])

    if page == "🏠 Overview":
        render_overview(prices, sentiment, anomalies, momentum)
    elif page == "📈 Ticker Detail":
        if selected_ticker != "ALL":
            render_ticker_detail(selected_ticker, prices, sentiment, anomalies)
        else:
            st.info("Select a specific ticker from the sidebar to view details.")
    elif page == "🗺️ Sector Heatmap":
        render_sector_heatmap(momentum, sentiment)
    elif page == "🎬 Event Replay":
        render_event_replay(prices, sentiment, anomalies)


if __name__ == "__main__":
    main()

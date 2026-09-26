"""
dashboard.py — Interactive Streamlit Dashboard for BDATL Engine
================================================================
Provides: ticker search, sentiment-price timelines, anomaly alerts,
sector heatmaps, and historical event replay.

Run inside the master container:
    streamlit run /app/src/dashboard/dashboard.py --server.port 8501
"""

import os
import glob
import io
import shutil
import socket
import subprocess
import datetime
import requests
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from typing import Any, Dict

# ---------------------------------------------------------------------------
# Page Configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="BDATL — Indian Market Sentiment & Spark Cluster Engine",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Indian Sector Colors
# ---------------------------------------------------------------------------
SECTOR_COLORS = {
    "IT": "#00d2ff", "Information Technology": "#00d2ff",
    "Banking": "#3a7bd5", "Financial Services": "#3a7bd5",
    "Energy": "#f7971e", "Power": "#f7971e", "Oil Gas & Consumable Fuels": "#f7971e",
    "FMCG": "#56ab2f", "Fast Moving Consumer Goods": "#56ab2f",
    "Pharma": "#e44d26", "Healthcare": "#e44d26",
    "Auto": "#8e44ad", "Automobile and Auto Components": "#8e44ad",
    "Metals": "#7f8c8d", "Metals & Mining": "#7f8c8d",
    "Telecom": "#e74c3c", "Telecommunication": "#e74c3c",
    "Capital Goods": "#f39c12", "Construction Materials": "#95a5a6",
    "Consumer Services": "#1abc9c", "Realty": "#e67e22", "Chemicals": "#16a085",
    "Other": "#95a5a6"
}

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    .main { font-family: 'Inter', sans-serif; }
    .stMetric { background: linear-gradient(135deg, rgba(26,26,46,0.95) 0%, rgba(22,33,62,0.95) 100%);
                border-radius: 12px; padding: 16px; border: 1px solid rgba(80, 100, 180, 0.4);
                box-shadow: 0 4px 15px rgba(0,0,0,0.25); }
    .stMetric label, .stMetric [data-testid="stMetricLabel"] p { color: #cbd5e1 !important; font-weight: 500; font-size: 0.95em; }
    .stMetric [data-testid="stMetricValue"], .stMetric [data-testid="stMetricValue"] div { color: #ffffff !important; font-weight: 700; }
    .anomaly-alert { background: linear-gradient(135deg, #ff416c, #ff4b2b);
                     color: white; padding: 16px; border-radius: 12px;
                     margin: 8px 0; font-weight: 500; box-shadow: 0 4px 12px rgba(255, 65, 108, 0.3); }
    .positive-badge { background: #00b894; color: white; padding: 4px 12px;
                      border-radius: 20px; font-size: 0.85em; font-weight: 600; }
    .negative-badge { background: #d63031; color: white; padding: 4px 12px;
                      border-radius: 20px; font-size: 0.85em; font-weight: 600; }
    .neutral-badge  { background: #636e72; color: white; padding: 4px 12px;
                      border-radius: 20px; font-size: 0.85em; }
    div[data-testid="stSidebar"] { background: linear-gradient(180deg, #0a0720 0%, #1e1b4b 50%, #16162a 100%); }
    h1, h2, h3 { font-weight: 600; }
    .pipeline-status { padding: 12px; border-radius: 8px; margin: 8px 0; font-weight: 500; }
    .pipeline-running { background: #fdcb6e; color: #2d3436; border-left: 4px solid #e17055; }
    .pipeline-done { background: #00b894; color: white; border-left: 4px solid #00cec9; }

    /* Modern Big Data Cluster HUD Styles */
    .cluster-card {
        background: linear-gradient(135deg, rgba(20, 24, 48, 0.85) 0%, rgba(15, 18, 38, 0.85) 100%);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(80, 100, 180, 0.25);
        border-radius: 14px;
        padding: 18px;
        box-shadow: 0 8px 24px rgba(0,0,0,0.35);
        margin-bottom: 16px;
    }
    .cluster-title {
        font-size: 1.1em;
        font-weight: 700;
        margin-bottom: 8px;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .status-dot-active {
        display: inline-block;
        width: 10px;
        height: 10px;
        background-color: #00b894;
        border-radius: 50%;
        margin-right: 6px;
        box-shadow: 0 0 10px #00b894;
        animation: pulse-green 2s infinite;
    }
    .status-dot-offline {
        display: inline-block;
        width: 10px;
        height: 10px;
        background-color: #d63031;
        border-radius: 50%;
        margin-right: 6px;
        box-shadow: 0 0 10px #d63031;
    }
    @keyframes pulse-green {
        0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(0, 184, 148, 0.7); }
        70% { transform: scale(1.15); box-shadow: 0 0 0 8px rgba(0, 184, 148, 0); }
        100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(0, 184, 148, 0); }
    }
    .terminal-box {
        background: #0d1117;
        color: #58a6ff;
        font-family: 'JetBrains Mono', 'Consolas', monospace;
        padding: 16px;
        border-radius: 10px;
        border: 1px solid #30363d;
        height: 340px;
        overflow-y: auto;
        white-space: pre-wrap;
        font-size: 0.86em;
        line-height: 1.45;
        box-shadow: inset 0 2px 8px rgba(0,0,0,0.6);
    }
    .node-tile {
        background: rgba(30, 27, 75, 0.4);
        border: 1px solid rgba(139, 92, 246, 0.25);
        border-radius: 10px;
        padding: 12px;
        margin-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)



# ---------------------------------------------------------------------------
# Helper: Safe Timezone-Naive Datetime Parser
# ---------------------------------------------------------------------------
def to_naive_datetime(series):
    """Convert pandas series to datetime and strip all timezones for safe comparison."""
    dt = pd.to_datetime(series, errors="coerce")
    try:
        if hasattr(dt, "dt"):
            return dt.dt.tz_localize(None)
    except Exception:
        try:
            return dt.dt.tz_convert(None)
        except Exception:
            pass
    return dt


# ---------------------------------------------------------------------------
# Data Normalization Helpers (Ensures consistent column casing & formats)
# ---------------------------------------------------------------------------
def normalize_prices(df):
    """Normalize price data columns and types."""
    if df.empty:
        return df
    rename = {}
    for col in df.columns:
        lc = str(col).lower().strip()
        if lc in ("date", "trade_date"):
            rename[col] = "Date"
        elif lc in ("open", "open_price"):
            rename[col] = "Open"
        elif lc in ("high", "high_price"):
            rename[col] = "High"
        elif lc in ("low", "low_price"):
            rename[col] = "Low"
        elif lc in ("close", "close_price", "adj close", "adj_close"):
            if "Close" not in rename.values():
                rename[col] = "Close"
        elif lc == "volume":
            rename[col] = "Volume"
        elif lc == "ticker":
            rename[col] = "Ticker"
    df = df.rename(columns=rename)
    if "Ticker" in df.columns:
        df["Ticker"] = (
            df["Ticker"].astype(str)
            .str.replace(".NS", "", regex=False)
            .str.replace(".BO", "", regex=False)
            .str.strip().str.upper()
        )
    if "Date" in df.columns:
        df["Date"] = to_naive_datetime(df["Date"])
    for num_col in ["Open", "High", "Low", "Close", "Volume"]:
        if num_col in df.columns:
            df[num_col] = pd.to_numeric(df[num_col], errors="coerce")
    return df


def normalize_price_columns(df):
    """Alias for backward compatibility."""
    return normalize_prices(df)


def normalize_sentiment(df):
    """Normalize sentiment data columns and types."""
    if df.empty:
        return df
    rename = {}
    for col in df.columns:
        lc = str(col).lower().strip()
        if lc == "ticker":
            rename[col] = "ticker"
        elif lc in ("event_date", "date", "published_date", "published"):
            rename[col] = "event_date"
        elif lc in ("sentiment_score", "score", "polarity"):
            rename[col] = "sentiment_score"
        elif lc in ("sentiment_label", "label"):
            rename[col] = "sentiment_label"
    df = df.rename(columns=rename)
    if "ticker" in df.columns:
        df["ticker"] = (
            df["ticker"].astype(str)
            .str.replace(".NS", "", regex=False)
            .str.replace(".BO", "", regex=False)
            .str.strip().str.upper()
        )
    if "sentiment_score" in df.columns:
        df["sentiment_score"] = pd.to_numeric(df["sentiment_score"], errors="coerce")
    if "event_date" in df.columns:
        df["event_date"] = to_naive_datetime(df["event_date"])
    return df


def normalize_anomalies(df):
    """Normalize anomalies data columns and types."""
    if df.empty:
        return df
    rename = {}
    for col in df.columns:
        lc = str(col).lower().strip()
        if lc == "ticker":
            rename[col] = "ticker"
        elif lc in ("anomaly_date", "trade_date", "date"):
            rename[col] = "anomaly_date"
        elif lc in ("close_price", "close"):
            rename[col] = "close_price"
        elif lc == "volume":
            rename[col] = "volume"
        elif lc in ("anomaly_score", "score"):
            rename[col] = "anomaly_score"
        elif lc in ("anomaly_type", "type"):
            rename[col] = "anomaly_type"
    df = df.rename(columns=rename)
    if "ticker" in df.columns:
        df["ticker"] = (
            df["ticker"].astype(str)
            .str.replace(".NS", "", regex=False)
            .str.replace(".BO", "", regex=False)
            .str.strip().str.upper()
        )
    if "anomaly_date" in df.columns:
        df["anomaly_date"] = to_naive_datetime(df["anomaly_date"])
    if "anomaly_score" in df.columns:
        df["anomaly_score"] = pd.to_numeric(df["anomaly_score"], errors="coerce")
    if "close_price" in df.columns:
        df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    if "anomaly_type" in df.columns:
        df["anomaly_type"] = df["anomaly_type"].astype(str).str.strip()
    return df


def normalize_momentum(df):
    """Normalize sector momentum columns and types."""
    if df.empty:
        return df
    rename = {}
    for col in df.columns:
        lc = str(col).lower().strip()
        if lc in ("sector", "sector_name"):
            rename[col] = "sector"
        elif lc in ("momentum_score", "momentum"):
            rename[col] = "momentum_score"
        elif lc in ("window_days", "window"):
            rename[col] = "window_days"
        elif lc in ("end_date", "date"):
            rename[col] = "end_date"
    df = df.rename(columns=rename)
    if "momentum_score" in df.columns:
        df["momentum_score"] = pd.to_numeric(df["momentum_score"], errors="coerce")
    if "window_days" in df.columns:
        df["window_days"] = pd.to_numeric(df["window_days"], errors="coerce")
    if "end_date" in df.columns:
        df["end_date"] = to_naive_datetime(df["end_date"])
    return df


# ---------------------------------------------------------------------------
# Data Loading (HDFS with local fallback)
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Cluster Telemetry & Engine Inspection (Spark, HDFS, YARN)
# ---------------------------------------------------------------------------
def is_port_open(host: str, port: int, timeout: float = 0.25) -> bool:
    """Fast check whether a network port is reachable without blocking on long HTTP timeouts."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


def _safe_numeric(df):
    """Safely convert numeric columns without deprecated errors='ignore'."""
    if df.empty:
        return df
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            pass
    return df


@st.cache_data(ttl=5)
def fetch_cluster_metrics() -> dict[str, Any]:
    """Query live cluster telemetry from Spark Master, Hadoop NameNode, and YARN."""
    spark_default: dict[str, Any] = {
        "status": "OFFLINE", "url": "spark://master:7077",
        "alive_workers": 0, "total_workers": 0,
        "cores": 0, "cores_used": 0, "memory_gb": 0.0,
        "apps_running": 0, "apps_completed": 0,
        "active_apps": [], "completed_apps": [], "workers": []
    }
    hadoop_default: dict[str, Any] = {
        "status": "OFFLINE", "capacity_gb": 0.0, "used_mb": 0.0,
        "percent_used": 0.0, "live_datanodes": 0, "dead_datanodes": 0,
        "blocks": 0, "files": 0
    }
    yarn_default: dict[str, Any] = {
        "status": "OFFLINE", "active_nodes": 0, "allocated_mb": 0,
        "total_mb": 0, "containers": 0, "apps_running": 0, "apps_completed": 0
    }
    metrics: dict[str, dict[str, Any]] = {
        "spark": spark_default,
        "hadoop": hadoop_default,
        "yarn": yarn_default,
    }

    hosts = ["localhost", "127.0.0.1", "master"]

    # 1. Spark Master REST API (port 8080)
    for h in hosts:
        if not is_port_open(h, 8080, timeout=0.2):
            continue
        try:
            r = requests.get(f"http://{h}:8080/json", timeout=1.0)
            if r.status_code == 200:
                data = r.json()
                metrics["spark"]["status"] = "ALIVE"
                metrics["spark"]["url"] = data.get("url", f"spark://{h}:7077")
                workers = data.get("workers", [])
                metrics["spark"]["total_workers"] = len(workers)
                metrics["spark"]["alive_workers"] = data.get("aliveworkers", len([w for w in workers if w.get("state") == "ALIVE"]))
                metrics["spark"]["cores"] = data.get("cores", 0)
                metrics["spark"]["cores_used"] = data.get("coresused", 0)
                metrics["spark"]["memory_gb"] = round(data.get("memory", 0) / 1024, 1)
                active_apps = data.get("activeapps", [])
                completed_apps = data.get("completedapps", [])
                metrics["spark"]["apps_running"] = len(active_apps)
                metrics["spark"]["apps_completed"] = len(completed_apps)
                metrics["spark"]["active_apps"] = active_apps
                metrics["spark"]["completed_apps"] = completed_apps
                metrics["spark"]["workers"] = workers
                break
        except Exception:
            continue

    # 2. Hadoop NameNode JMX (port 9870)
    for h in hosts:
        if not is_port_open(h, 9870, timeout=0.2):
            continue
        try:
            r = requests.get(f"http://{h}:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState", timeout=1.0)
            if r.status_code == 200:
                beans = r.json().get("beans", [])
                if beans:
                    b = beans[0]
                    used_gb = float(b.get("CapacityUsedGB", 0.0))
                    metrics["hadoop"].update({
                        "status": "ACTIVE",
                        "capacity_gb": round(float(b.get("CapacityTotalGB", 0.0)), 2),
                        "used_mb": round(used_gb * 1024, 2),
                        "percent_used": round(float(b.get("PercentUsed", 0.0)), 2),
                        "live_datanodes": int(b.get("NumLiveDataNodes", 0)),
                        "dead_datanodes": int(b.get("NumDeadDataNodes", 0)),
                        "blocks": int(b.get("TotalBlocks", 0)),
                        "files": int(b.get("TotalFiles", 0)),
                    })
                    break
        except Exception:
            continue

    # 3. YARN ResourceManager REST API (port 8088)
    for h in hosts:
        if not is_port_open(h, 8088, timeout=0.2):
            continue
        try:
            r = requests.get(f"http://{h}:8088/ws/v1/cluster/metrics", timeout=1.0)
            if r.status_code == 200:
                cm = r.json().get("clusterMetrics", {})
                metrics["yarn"]["status"] = "ACTIVE"
                metrics["yarn"]["active_nodes"] = cm.get("activeNodes", 0)
                metrics["yarn"]["allocated_mb"] = cm.get("allocatedMB", 0)
                metrics["yarn"]["total_mb"] = cm.get("totalMB", 0)
                metrics["yarn"]["containers"] = cm.get("containersAllocated", 0)
                metrics["yarn"]["apps_running"] = cm.get("appsRunning", 0)
                metrics["yarn"]["apps_completed"] = cm.get("appsCompleted", 0)
                break
        except Exception:
            continue

    return metrics


# ---------------------------------------------------------------------------
# Data Loading (Local Staging + HDFS CLI + WebHDFS REST API)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60)
def load_data(hdfs_path, local_fallback_dirs=None, max_rows=150000):
    """Load big data CSV files directly from Hadoop HDFS (CLI + WebHDFS), with local fallback."""
    # 1. Primary Source: Hadoop HDFS CLI (when running inside cluster / master container)
    if hdfs_path and shutil.which("hdfs"):
        hdfs_dir = hdfs_path.rstrip("/")
        clean_rel_path = hdfs_dir.split(":9000")[-1] if ":9000" in hdfs_dir else hdfs_dir

        for pattern in [f"{clean_rel_path}/*.csv", f"{clean_rel_path}/part*.csv", f"{hdfs_dir}/*.csv", f"{hdfs_dir}/part*.csv"]:
            try:
                cmd = f"hdfs dfs -cat '{pattern}' 2>/dev/null | head -n {max_rows}" if max_rows else f"hdfs dfs -cat '{pattern}' 2>/dev/null"
                result = subprocess.run(
                    cmd, shell=True,
                    capture_output=True, text=True, timeout=15
                )
                if result.returncode == 0 and result.stdout.strip():
                    df = pd.read_csv(io.StringIO(result.stdout), on_bad_lines="skip", low_memory=False)
                    if not df.empty:
                        df = _drop_header_rows(df)
                        return _safe_numeric(df)
            except Exception:
                pass

    # 2. Secondary Source: WebHDFS REST API (if NameNode port 9870 is open)
    if hdfs_path:
        hdfs_dir = hdfs_path.rstrip("/")
        clean_rel_path = hdfs_dir.split(":9000")[-1] if ":9000" in hdfs_dir else hdfs_dir

        for host in ["master", "localhost", "127.0.0.1"]:
            if not is_port_open(host, 9870, timeout=0.2):
                continue
            try:
                webhdfs_url = f"http://{host}:9870/webhdfs/v1{clean_rel_path}?op=LISTSTATUS"
                r = requests.get(webhdfs_url, timeout=1.5)
                if r.status_code == 200:
                    statuses = r.json().get("FileStatuses", {}).get("FileStatus", [])
                    csv_statuses = [s for s in statuses if s.get("pathSuffix", "").endswith(".csv")]
                    if csv_statuses:
                        dfs = []
                        for s in csv_statuses[:50]:
                            fn = s.get("pathSuffix")
                            file_url = f"http://{host}:9870/webhdfs/v1{clean_rel_path}/{fn}?op=OPEN"
                            fr = requests.get(file_url, timeout=4)
                            if fr.status_code == 200 and fr.text.strip():
                                tdf = pd.read_csv(io.StringIO(fr.text), on_bad_lines="skip", low_memory=False)
                                if not tdf.empty:
                                    dfs.append(tdf)
                        if dfs:
                            combined = pd.concat(dfs, ignore_index=True)
                            combined = _drop_header_rows(combined)
                            return _safe_numeric(combined)
            except Exception:
                pass

    # 3. Fallback: Local directory files (when HDFS is not accessible)
    if local_fallback_dirs:
        for ldir in local_fallback_dirs:
            if os.path.isdir(ldir):
                csv_files = glob.glob(os.path.join(ldir, "*.csv"))
                if csv_files:
                    dfs = []
                    for f in csv_files:
                        try:
                            tdf = pd.read_csv(f, on_bad_lines="skip", low_memory=False)
                            if not tdf.empty:
                                dfs.append(tdf)
                        except Exception:
                            continue
                    if dfs:
                        combined = pd.concat(dfs, ignore_index=True)
                        combined = _drop_header_rows(combined)
                        return _safe_numeric(combined)

    return pd.DataFrame()


def _drop_header_rows(df):
    """Remove rows where ANY column value equals its own column name (repeated CSV headers)."""
    if df.empty:
        return df
    mask = pd.Series([True] * len(df), index=df.index)
    for col in df.columns:
        col_str = str(col)
        mask = mask & (df[col].astype(str).str.strip() != col_str)
    return df[mask].reset_index(drop=True)


def load_all_data():
    """Load all datasets used by the dashboard from HDFS or local fallbacks."""
    # Resolve data root relative to this file (works on Windows dev and Linux/Docker)
    from pathlib import Path
    _here = Path(__file__).resolve()
    _data_candidates = []
    for _parent in [_here.parent.parent, _here.parent.parent.parent]:
        _cand = _parent / "data"
        if _cand.exists():
            _data_candidates.append(str(_cand))
    # Also add Docker / container path
    _data_candidates = ["/app/data"] + _data_candidates + ["data", "../data"]

    def _local_dirs(subpath):
        dirs = []
        for root in _data_candidates:
            dirs.append(os.path.join(root, subpath))
        return dirs

    prices = load_data(
        "hdfs://master:9000/data/raw/prices",
        local_fallback_dirs=_local_dirs("raw/prices")
    )
    sentiment = load_data(
        "hdfs://master:9000/data/processed/sentiment",
        local_fallback_dirs=_local_dirs("processed/sentiment")
    )
    anomalies = load_data(
        "hdfs://master:9000/data/processed/anomalies",
        local_fallback_dirs=_local_dirs("processed/anomalies")
    )
    momentum = load_data(
        "hdfs://master:9000/data/processed/momentum",
        local_fallback_dirs=_local_dirs("processed/momentum")
    )
    news = load_data(
        "hdfs://master:9000/data/raw/news",
        local_fallback_dirs=_local_dirs("raw/news")
    )

    # Standardize column naming and types across all loaded DataFrames immediately
    prices = normalize_prices(prices)
    sentiment = normalize_sentiment(sentiment)
    anomalies = normalize_anomalies(anomalies)
    momentum = normalize_momentum(momentum)

    return prices, sentiment, anomalies, momentum, news


def get_pipeline_status():
    """Check if the pipeline or spark-submit is currently running."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "run_pipeline.sh|spark-submit|fetch_price"],
            capture_output=True, text=True
        )
        if result.returncode == 0 and result.stdout.strip():
            return True
    except Exception:
        pass

    try:
        result = subprocess.run(
            ["docker", "exec", "master", "pgrep", "-f", "run_pipeline.sh|spark-submit|fetch_price"],
            capture_output=True, text=True, timeout=2
        )
        if result.returncode == 0 and result.stdout.strip():
            return True
    except Exception:
        pass

    return False


def run_pipeline_background(mode="--quick"):
    """Launch the data pipeline in the background (inside container or via docker exec)."""
    if os.path.exists("/app/run_pipeline.sh"):
        subprocess.Popen(
            ["bash", "/app/run_pipeline.sh", mode],
            stdout=open("/tmp/pipeline.log", "w"),
            stderr=subprocess.STDOUT,
            cwd="/app"
        )
    else:
        try:
            subprocess.Popen(
                ["docker", "exec", "master", "bash", "/app/run_pipeline.sh", mode],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
        except Exception:
            pass


def get_latest_pipeline_logs(lines=45):
    """Read the latest execution logs from /tmp/pipeline.log."""
    if os.path.exists("/tmp/pipeline.log"):
        try:
            with open("/tmp/pipeline.log", "r", encoding="utf-8", errors="replace") as f:
                content = f.readlines()
                return "".join(content[-lines:])
        except Exception as e:
            return f"Error reading log: {e}"
    try:
        r = subprocess.run(
            ["docker", "exec", "master", "tail", f"-n{lines}", "/tmp/pipeline.log"],
            capture_output=True, text=True, timeout=2
        )
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout
    except Exception:
        pass
    return "No execution log recorded yet. Launch a pipeline or Spark job below to begin streaming logs."


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
def render_sidebar(prices_df, anomalies_df, cluster_metrics=None):
    with st.sidebar:
        st.markdown("<div style='font-size: 2.8em; margin-bottom: 4px;'>📈</div>", unsafe_allow_html=True)
        st.title("🇮🇳 BDATL Engine")
        st.caption("Distributed Sentiment & Anomaly Detection")

        # Cluster HUD Mini Badge
        if cluster_metrics:
            sp = cluster_metrics.get("spark", {})
            hd = cluster_metrics.get("hadoop", {})
            sp_icon = "🟢" if sp.get("status") == "ALIVE" else "🔴"
            hd_icon = "🟢" if hd.get("status") == "ACTIVE" else "🔴"
            st.markdown(
                f"<div style='font-size:0.82em; background:rgba(255,255,255,0.06); padding:8px 12px; border-radius:8px; margin-bottom:10px; border:1px solid rgba(255,255,255,0.1);'>"
                f"{sp_icon} <b>Spark:</b> {sp.get('alive_workers',0)}/3 Workers ({sp.get('cores',0)} Cores)<br/>"
                f"{hd_icon} <b>HDFS:</b> {hd.get('used_mb',0)} MB ({hd.get('live_datanodes',0)}/3 DataNodes)"
                f"</div>",
                unsafe_allow_html=True
            )

        st.divider()

        # Pipeline controls
        st.subheader("⚙️ Cluster Pipeline")
        is_running = get_pipeline_status()
        if is_running:
            st.markdown('<div class="pipeline-status pipeline-running">⏳ Spark / Ingestion Job is running...</div>',
                        unsafe_allow_html=True)
            if st.button("🔄 Check & Refresh Data", use_container_width=True):
                st.cache_data.clear()
                st.rerun()
        else:
            has_data = not prices_df.empty
            if has_data:
                st.markdown('<div class="pipeline-status pipeline-done">✅ Cluster Data Loaded</div>',
                            unsafe_allow_html=True)
            else:
                st.markdown('<div class="pipeline-status" style="background:#2d3436; color:#dfe6e9;">ℹ️ Waiting for Spark ML</div>',
                            unsafe_allow_html=True)

            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                if st.button("⚡ Spark ML (HDFS)", use_container_width=True, help="Run Spark ML directly on existing HDFS data without waiting for ingestion"):
                    run_pipeline_background("--spark-only")
                    st.toast("⚡ Spark ML job submitted! Check Spark Master at :8080")
                    st.rerun()
            with col_btn2:
                if st.button("🚀 Quick Run", use_container_width=True, help="Ingests top 30 stocks + news + Spark ML in ~2 min"):
                    run_pipeline_background("--quick")
                    st.toast("🚀 Quick Pipeline launched! Tracking top liquid stocks.")
                    st.rerun()

        st.divider()


        # Ticker selection
        tickers = []
        if not prices_df.empty and "Ticker" in prices_df.columns:
            tickers = sorted([str(t) for t in prices_df["Ticker"].dropna().unique() if str(t).strip() and str(t) != "GENERAL"])
        if not tickers and not anomalies_df.empty and "ticker" in anomalies_df.columns:
            tickers = sorted([str(t) for t in anomalies_df["ticker"].dropna().unique() if str(t).strip() and str(t) != "GENERAL"])
        if not tickers:
            try:
                from src.config.tickers import TICKER_SYMBOLS
                tickers = sorted(TICKER_SYMBOLS)
            except Exception:
                pass

        selected_ticker = st.selectbox("🔍 Search Ticker", ["ALL"] + tickers, index=0)

        # Show Sector if specific ticker is selected
        if selected_ticker != "ALL":
            try:
                from src.config.tickers import TICKER_MAP
                sector = TICKER_MAP.get(f"{selected_ticker}.NS", "Other")
                st.caption(f"🏢 Sector: **{sector}**")
            except Exception:
                pass

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
            raw_types = sorted([str(t) for t in anomalies_df["anomaly_type"].dropna().unique() if str(t).strip()])
            for at in raw_types:
                if at not in anomaly_types:
                    anomaly_types.append(at)
        selected_anomaly_type = st.selectbox("⚡ Anomaly Type", anomaly_types)

        st.divider()

        # Quick stats
        st.subheader("📊 Quick Stats")
        if not prices_df.empty:
            st.metric("Tickers Tracked", len(tickers) if tickers else len(prices_df["Ticker"].unique()))
        if not anomalies_df.empty:
            st.metric("Anomalies Detected", len(anomalies_df))

        return selected_ticker, start_date, end_date, selected_anomaly_type


# ---------------------------------------------------------------------------
# 1. Overview Page
# ---------------------------------------------------------------------------
def render_overview(prices_df, sentiment_df, anomalies_df, momentum_df,
                    selected_ticker="ALL", start_date=None, end_date=None, anomaly_type="All"):
    """Overview page with dynamic metrics and alerts filtered by user selection."""
    try:
        from src.config.tickers import TICKER_MAP
    except Exception:
        try:
            from config.tickers import TICKER_MAP
        except Exception:
            TICKER_MAP = {}

    # Header title changes dynamically depending on selected ticker
    if selected_ticker == "ALL":
        st.title("📊 Market Sentiment Overview")
        st.caption("Displaying aggregate metrics across all tracked Indian stocks.")
    else:
        sector = TICKER_MAP.get(f"{selected_ticker}.NS", "Indian Equities")
        st.title(f"📊 Market Overview — {selected_ticker}")
        st.caption(f"🏢 Sector: **{sector}** | Real-time sentiment & price anomaly profile")

    # 1. Filter DataFrames based on selected ticker
    p_df = prices_df.copy()
    s_df = sentiment_df.copy()
    a_df = anomalies_df.copy()

    if selected_ticker != "ALL":
        if not p_df.empty and "Ticker" in p_df.columns:
            p_df = p_df[p_df["Ticker"] == selected_ticker]
        if not s_df.empty and "ticker" in s_df.columns:
            s_df = s_df[s_df["ticker"] == selected_ticker]
        if not a_df.empty and "ticker" in a_df.columns:
            a_df = a_df[a_df["ticker"] == selected_ticker]

    # 2. Filter by date range using timezone-naive timestamps
    if start_date and end_date:
        start_ts = pd.to_datetime(start_date).tz_localize(None) if hasattr(pd.to_datetime(start_date), "tz_localize") else pd.to_datetime(start_date)
        end_ts = pd.to_datetime(end_date).tz_localize(None) if hasattr(pd.to_datetime(end_date), "tz_localize") else pd.to_datetime(end_date)
        
        if not p_df.empty and "Date" in p_df.columns:
            p_df = p_df[(p_df["Date"] >= start_ts) & (p_df["Date"] <= end_ts)]
        if not s_df.empty and "event_date" in s_df.columns:
            s_df = s_df[(s_df["event_date"] >= start_ts) & (s_df["event_date"] <= end_ts)]
        if not a_df.empty and "anomaly_date" in a_df.columns:
            a_df = a_df[(a_df["anomaly_date"] >= start_ts) & (a_df["anomaly_date"] <= end_ts)]

    # 3. Filter by anomaly type
    if anomaly_type != "All" and not a_df.empty and "anomaly_type" in a_df.columns:
        a_df = a_df[a_df["anomaly_type"] == anomaly_type]

    # 4. Dynamic KPI Row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if selected_ticker == "ALL":
            tracked_count = len(prices_df["Ticker"].unique()) if not prices_df.empty and "Ticker" in prices_df.columns else 0
            st.metric("Stocks Tracked", tracked_count)
        else:
            sector = TICKER_MAP.get(f"{selected_ticker}.NS", "Equities")
            st.metric("Selected Stock", selected_ticker, delta=sector, delta_color="off")

    with col2:
        if not s_df.empty and "sentiment_score" in s_df.columns:
            valid_scores = pd.to_numeric(s_df["sentiment_score"], errors="coerce").dropna()
            if len(valid_scores) > 0:
                avg_sent = valid_scores.mean()
                delta_lbl = "Bullish" if avg_sent > 0.05 else ("Bearish" if avg_sent < -0.05 else "Neutral")
                st.metric("Avg Sentiment", f"{avg_sent:+.3f}", delta=delta_lbl)
            else:
                st.metric("Avg Sentiment", "Neutral (0.00)", delta="No News", delta_color="off")
        else:
            if selected_ticker != "ALL":
                st.metric("Avg Sentiment", "N/A", delta="No News", delta_color="off")
            else:
                st.metric("Avg Sentiment", "N/A")

    with col3:
        anom_count = len(a_df) if not a_df.empty else 0
        if selected_ticker == "ALL":
            st.metric("Anomalies (Total)", anom_count)
        else:
            total_ticker_anoms = len(anomalies_df[anomalies_df["ticker"] == selected_ticker]) if not anomalies_df.empty and "ticker" in anomalies_df.columns else 0
            st.metric("Anomalies", anom_count, delta=f"{total_ticker_anoms} all-time", delta_color="off")

    with col4:
        if not a_df.empty and "anomaly_type" in a_df.columns:
            divergences = len(a_df[a_df["anomaly_type"].astype(str).str.contains("divergence", case=False, na=False)])
            st.metric("Divergences", divergences)
        else:
            st.metric("Divergences", 0)

    st.divider()

    # 5. Visual Section: Alerts + Contextual Right Column
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("⚡ Recent Anomaly Alerts" if selected_ticker == "ALL" else f"⚡ Recent Alerts: {selected_ticker}")
        if not a_df.empty:
            recent = a_df.sort_values("anomaly_date", ascending=False).head(10)
            for _, row in recent.iterrows():
                desc = row.get("description", row.get("anomaly_type", "Unknown"))
                date_str = str(row.get("anomaly_date", "?"))[:10]
                t_sym = row.get("ticker", selected_ticker)
                score_val = row.get("anomaly_score", 0)
                try:
                    score_str = f"{float(score_val):.2f}"
                except Exception:
                    score_str = str(score_val)
                st.markdown(
                    '<div class="anomaly-alert">'
                    f'<strong>{t_sym}</strong> — '
                    f'{date_str} | '
                    f'Score: {score_str}<br/>'
                    f'{desc}</div>',
                    unsafe_allow_html=True
                )
        else:
            if selected_ticker == "ALL":
                st.info("No anomalies detected yet under current filters. Click **Run Pipeline** in the sidebar to start data processing.")
            else:
                st.info(f"No anomalies detected for **{selected_ticker}** matching current filters.")

    with col_right:
        if selected_ticker == "ALL":
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
        else:
            # Contextual drill-down for selected stock
            sector = TICKER_MAP.get(f"{selected_ticker}.NS", "Other")
            st.subheader(f"🏢 Sector: {sector}")
            if not momentum_df.empty and "sector" in momentum_df.columns:
                sec_row = momentum_df[(momentum_df["sector"] == sector) & (momentum_df["window_days"] == 30)]
                if not sec_row.empty:
                    score = float(sec_row["momentum_score"].iloc[-1])
                    st.metric("Sector Momentum (30d)", f"{score:+.2f}")
            
            # Quick price sparkline if price data is available
            if not p_df.empty and "Close" in p_df.columns and "Date" in p_df.columns:
                pdf_chart = p_df.dropna(subset=["Date", "Close"]).sort_values("Date").tail(60)
                if len(pdf_chart) > 1:
                    st.caption("Recent 60-Day Price Trend:")
                    fig_mini = px.line(pdf_chart, x="Date", y="Close", title=f"{selected_ticker} Price (₹)")
                    fig_mini.update_layout(
                        height=250, margin=dict(l=0, r=0, t=30, b=0),
                        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                        font_color="white", xaxis_visible=False
                    )
                    st.plotly_chart(fig_mini, use_container_width=True)


# ---------------------------------------------------------------------------
# 2. Ticker Detail Page
# ---------------------------------------------------------------------------
def render_ticker_detail(ticker, prices_df, sentiment_df, anomalies_df, start_date=None, end_date=None):
    """Detailed ticker view with price-sentiment overlay, volume, and date range filtering."""
    st.title(f"📈 {ticker} — Sentiment & Price Timeline")

    if prices_df.empty:
        st.warning("No price data available. Run the pipeline first.")
        return

    ticker_prices = prices_df[prices_df["Ticker"] == ticker].copy()
    if ticker_prices.empty:
        st.warning(f"No price data found for ticker: {ticker}. Check if data has been fetched for this symbol.")
        return

    # Filter by user selected date range
    if start_date and end_date and "Date" in ticker_prices.columns:
        start_ts = pd.to_datetime(start_date)
        end_ts = pd.to_datetime(end_date)
        ticker_prices = ticker_prices[(ticker_prices["Date"] >= start_ts) & (ticker_prices["Date"] <= end_ts)]
    
    ticker_prices = ticker_prices.dropna(subset=["Date"]).sort_values("Date")
    if ticker_prices.empty:
        st.warning(f"No price data for {ticker} in the selected date range ({start_date} to {end_date}).")
        return

    # Summary metrics header
    latest_close = ticker_prices["Close"].iloc[-1]
    prev_close = ticker_prices["Close"].iloc[-2] if len(ticker_prices) > 1 else latest_close
    chg = ((latest_close - prev_close) / prev_close) * 100.0 if prev_close else 0.0
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Latest Close", f"₹{latest_close:,.2f}", f"{chg:+.2f}%")
    with c2:
        st.metric("52-Week High", f"₹{ticker_prices['High'].max():,.2f}")
    with c3:
        st.metric("52-Week Low", f"₹{ticker_prices['Low'].min():,.2f}")
    with c4:
        st.metric("Trading Volume (Avg)", f"{int(ticker_prices['Volume'].mean()):,}")

    st.divider()

    # Create dual-axis plot: Price + Volume/Sentiment
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
        row_heights=[0.7, 0.3],
        subplot_titles=[f"{ticker} Price Action & Anomalies", "Sentiment / Volume Profile"]
    )

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=ticker_prices["Date"],
        open=ticker_prices["Open"], high=ticker_prices["High"],
        low=ticker_prices["Low"], close=ticker_prices["Close"],
        name="Price"
    ), row=1, col=1)

    # Anomaly markers (Red triangles)
    if not anomalies_df.empty and "ticker" in anomalies_df.columns:
        ticker_anomalies = anomalies_df[anomalies_df["ticker"] == ticker].copy()
        if start_date and end_date and not ticker_anomalies.empty and "anomaly_date" in ticker_anomalies.columns:
            ticker_anomalies = ticker_anomalies[
                (ticker_anomalies["anomaly_date"] >= pd.to_datetime(start_date)) &
                (ticker_anomalies["anomaly_date"] <= pd.to_datetime(end_date))
            ]
        if not ticker_anomalies.empty and "close_price" in ticker_anomalies.columns:
            fig.add_trace(go.Scatter(
                x=ticker_anomalies["anomaly_date"],
                y=ticker_anomalies["close_price"],
                mode="markers",
                marker=dict(size=12, color="#ff416c", symbol="triangle-up"),
                name="Anomaly Detected",
                text=ticker_anomalies.get("description", ticker_anomalies.get("anomaly_type", "")),
            ), row=1, col=1)

    # Sentiment line or volume profile
    has_real_sentiment = False
    if not sentiment_df.empty and "ticker" in sentiment_df.columns:
        ticker_sentiment = sentiment_df[sentiment_df["ticker"] == ticker].copy()
        if start_date and end_date and not ticker_sentiment.empty and "event_date" in ticker_sentiment.columns:
            ticker_sentiment = ticker_sentiment[
                (ticker_sentiment["event_date"] >= pd.to_datetime(start_date)) &
                (ticker_sentiment["event_date"] <= pd.to_datetime(end_date))
            ]
        if not ticker_sentiment.empty and "sentiment_score" in ticker_sentiment.columns:
            ticker_sentiment = ticker_sentiment.dropna(subset=["event_date"])
            if not ticker_sentiment.empty:
                has_real_sentiment = True
                daily_sent = ticker_sentiment.groupby("event_date")["sentiment_score"].mean().reset_index()
                fig.add_trace(go.Scatter(
                    x=daily_sent["event_date"], y=daily_sent["sentiment_score"],
                    mode="lines+markers", name="News Sentiment Score",
                    line=dict(color="#00d2ff", width=2),
                    marker=dict(size=4),
                ), row=2, col=1)
                fig.add_hline(y=0, line_dash="dash", line_color="gray", row=2, col=1)

    # Fallback to Volume bars if no news sentiment
    if not has_real_sentiment:
        fig.add_trace(go.Bar(
            x=ticker_prices["Date"], y=ticker_prices["Volume"],
            name="Daily Volume", marker_color="#0f3460"
        ), row=2, col=1)

    fig.update_layout(
        height=700,
        plot_bgcolor="rgba(15,15,30,1)",
        paper_bgcolor="rgba(15,15,30,1)",
        font_color="white",
        xaxis_rangeslider_visible=False,
    )
    fig.update_xaxes(
        type="date", tickformat="%b %Y", tickangle=-45,
        showticklabels=True, row=1, col=1
    )
    fig.update_xaxes(
        type="date", tickformat="%b %Y", tickangle=-45,
        showticklabels=True, row=2, col=1
    )
    st.plotly_chart(fig, use_container_width=True)

    # Sentiment summary metrics
    if not sentiment_df.empty and "ticker" in sentiment_df.columns:
        ts = sentiment_df[sentiment_df["ticker"] == ticker]
        if not ts.empty and "sentiment_score" in ts.columns:
            st.subheader("📝 Sentiment Summary")
            sc1, sc2, sc3, sc4 = st.columns(4)
            with sc1:
                st.metric("Articles Analyzed", len(ts))
            with sc2:
                st.metric("Avg Score", f"{pd.to_numeric(ts['sentiment_score'], errors='coerce').mean():+.3f}")
            with sc3:
                pos = len(ts[ts["sentiment_label"] == "positive"]) if "sentiment_label" in ts.columns else 0
                st.metric("Positive", pos)
            with sc4:
                neg = len(ts[ts["sentiment_label"] == "negative"]) if "sentiment_label" in ts.columns else 0
                st.metric("Negative", neg)
        else:
            st.info(f"ℹ️ No specific financial news articles tracked for **{ticker}** in the latest RSS ingestion window. Volume bars displayed in the lower chart.")
    else:
        st.info("No sentiment data available. Click **Run Pipeline** in the sidebar.")


# ---------------------------------------------------------------------------
# 3. Sector Heatmap Page
# ---------------------------------------------------------------------------
def render_sector_heatmap(momentum_df, sentiment_df, prices_df, anomalies_df, selected_ticker="ALL"):
    """Sector-level heatmap visualization with ticker context and peer comparisons."""
    try:
        from src.config.tickers import TICKER_MAP
    except Exception:
        try:
            from config.tickers import TICKER_MAP
        except Exception:
            TICKER_MAP = {}

    st.title("🗺️ Sector Sentiment Heatmap")

    active_sector = None
    if selected_ticker != "ALL":
        active_sector = TICKER_MAP.get(f"{selected_ticker}.NS")
        if active_sector:
            st.info(f"🔍 Currently viewing ticker: **{selected_ticker}** | Sector: **{active_sector}**")

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
            title=f"Sector Momentum Heatmap ({window}-Day Rolling)",
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="white",
            height=450,
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

    # Sector Peer Comparison Table when a stock is selected
    if active_sector and not prices_df.empty and "Ticker" in prices_df.columns:
        st.divider()
        st.subheader(f"👥 Peer Comparison in {active_sector}")
        st.caption(f"Comparing **{selected_ticker}** against other tracked stocks in the **{active_sector}** sector.")
        
        # Find peers in same sector
        peer_symbols = [
            k.replace(".NS", "").replace(".BO", "")
            for k, v in TICKER_MAP.items() if v == active_sector
        ]
        peer_df = prices_df[prices_df["Ticker"].isin(peer_symbols)]
        if not peer_df.empty:
            summary_rows = []
            for sym in peer_symbols:
                s_prices = peer_df[peer_df["Ticker"] == sym].sort_values("Date")
                if len(s_prices) >= 2:
                    curr = s_prices["Close"].iloc[-1]
                    prev_30 = s_prices["Close"].iloc[-30] if len(s_prices) >= 30 else s_prices["Close"].iloc[0]
                    ret_30 = ((curr - prev_30) / prev_30) * 100.0 if prev_30 else 0.0
                    anom_cnt = len(anomalies_df[anomalies_df["ticker"] == sym]) if not anomalies_df.empty else 0
                    summary_rows.append({
                        "Ticker": sym,
                        "Current Price (₹)": round(curr, 2),
                        "30D Return (%)": round(ret_30, 2),
                        "Total Anomalies": anom_cnt,
                        "Status": "⭐ Selected" if sym == selected_ticker else "Peer"
                    })
            if summary_rows:
                peer_table = pd.DataFrame(summary_rows).sort_values("30D Return (%)", ascending=False)
                st.dataframe(peer_table, use_container_width=True)


# ---------------------------------------------------------------------------
# 4. Event Replay Page (Fully Interactive & Stock-Aware)
# ---------------------------------------------------------------------------
def render_event_replay(prices_df, sentiment_df, anomalies_df, selected_ticker="ALL"):
    """Historical event replay — step through landmark market events or stock-specific shocks."""
    st.title("🎬 Event Replay")

    try:
        from src.config.tickers import TICKER_MAP
    except Exception:
        try:
            from config.tickers import TICKER_MAP
        except Exception:
            TICKER_MAP = {}

    # Define landmark Indian market events
    landmark_events = {
        "Adani-Hindenburg Crisis (Jan 2023)": ("2023-01-20", "2023-03-31", ["ADANIENT", "ADANIPORTS", "SBIN"]),
        "Yes Bank Crisis (Mar 2020)": ("2020-02-15", "2020-05-15", ["SBIN", "ICICIBANK", "HDFCBANK"]),
        "COVID Crash (Mar 2020)": ("2020-01-15", "2020-06-30", ["RELIANCE", "TCS", "HDFCBANK", "INFY"]),
        "IT Rally Post-COVID (Oct 2020)": ("2020-09-01", "2021-03-31", ["TCS", "INFY", "WIPRO", "HCLTECH"]),
        "Nifty All-Time High (Sep 2024)": ("2024-08-01", "2024-12-31", ["RELIANCE", "TCS", "HDFCBANK"]),
        "Russia-Ukraine Impact (Feb 2022)": ("2022-02-01", "2022-04-30", ["TATASTEEL", "JSWSTEEL", "NTPC", "RELIANCE"]),
        "Banking Sector Rally (2023)": ("2023-03-01", "2023-06-30", ["SBIN", "ICICIBANK", "HDFCBANK", "KOTAKBANK"]),
    }

    # Replay mode selector when a specific ticker is active
    if selected_ticker != "ALL":
        replay_mode = st.radio(
            "Replay Mode",
            [
                f"🏛️ Compare {selected_ticker} Against Landmark Crises",
                f"⚡ Replay Top Anomaly Shocks for {selected_ticker}",
            ],
            horizontal=True
        )
    else:
        replay_mode = "🏛️ Landmark Market Crises"

    if "Landmark" in replay_mode:
        selected_event = st.selectbox("Select Market Event", list(landmark_events.keys()))
        start, end, base_tickers = landmark_events[selected_event]

        # Crucial Fix: Always include the selected ticker in the comparison!
        if selected_ticker != "ALL":
            plot_tickers = list(dict.fromkeys(base_tickers + [selected_ticker]))
            st.info(f"**Event**: {selected_event} | **Period**: {start} → {end}  \n"
                    f"**Comparing**: {', '.join(base_tickers)} + **`{selected_ticker}`** (Active Selection)")
        else:
            plot_tickers = base_tickers
            st.info(f"**Event**: {selected_event} | **Period**: {start} → {end}  \n"
                    f"**Key Tickers**: {', '.join(plot_tickers)}")

        if prices_df.empty:
            st.warning("No price data available.")
            return

        start_ts = pd.to_datetime(start)
        end_ts = pd.to_datetime(end)

        mask = (
            prices_df["Ticker"].isin(plot_tickers) &
            (prices_df["Date"] >= start_ts) &
            (prices_df["Date"] <= end_ts)
        )
        event_prices = prices_df[mask]

        if not event_prices.empty and "Close" in event_prices.columns:
            # Normalized return comparison or absolute price
            comp_type = st.radio("Price Scale", ["Normalized % Return (Base 100)", "Absolute Price (₹)"], horizontal=True)
            if "Normalized" in comp_type:
                # Normalize each ticker to start at 100 on the first available date in window
                norm_dfs = []
                for t in plot_tickers:
                    t_sub = event_prices[event_prices["Ticker"] == t].sort_values("Date")
                    if not t_sub.empty:
                        first_close = t_sub["Close"].iloc[0]
                        if first_close and first_close > 0:
                            t_sub = t_sub.copy()
                            t_sub["Normalized_Return"] = (t_sub["Close"] / first_close) * 100.0
                            norm_dfs.append(t_sub)
                if norm_dfs:
                    plot_df = pd.concat(norm_dfs, ignore_index=True)
                    fig = px.line(
                        plot_df, x="Date", y="Normalized_Return", color="Ticker",
                        title=f"Relative Performance (% Return, Base=100): {selected_event}",
                        labels={"Normalized_Return": "% of Initial Price"}
                    )
                else:
                    fig = px.line(event_prices, x="Date", y="Close", color="Ticker", title=f"Price Action: {selected_event}")
            else:
                fig = px.line(event_prices, x="Date", y="Close", color="Ticker", title=f"Price Action: {selected_event}")

            fig.update_layout(
                plot_bgcolor="rgba(15,15,30,1)", paper_bgcolor="rgba(15,15,30,1)",
                font_color="white", xaxis=dict(tickformat="%d %b %Y", tickangle=-45)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(f"No price data found for {', '.join(plot_tickers)} during {start} → {end}.")

        # Show anomalies for ALL plotted tickers (including selected_ticker)
        if not anomalies_df.empty and "ticker" in anomalies_df.columns:
            anom_mask = (
                anomalies_df["ticker"].isin(plot_tickers) &
                (anomalies_df["anomaly_date"] >= start_ts) &
                (anomalies_df["anomaly_date"] <= end_ts)
            )
            event_anomalies = anomalies_df[anom_mask]
            st.subheader(f"⚡ Anomalies During This Event ({len(event_anomalies)} detected)")
            if not event_anomalies.empty:
                st.dataframe(event_anomalies.sort_values("anomaly_date", ascending=False), use_container_width=True)
            else:
                st.info(f"No anomalies detected for {', '.join(plot_tickers)} in this window.")

    else:
        # Mode 2: Replay Top Historical Shock Events for selected_ticker
        st.subheader(f"⚡ Historical Anomaly Shocks for {selected_ticker}")
        t_anoms = anomalies_df[anomalies_df["ticker"] == selected_ticker].sort_values("anomaly_score", ascending=False)
        if t_anoms.empty:
            st.info(f"No recorded anomalies for {selected_ticker}.")
            return

        # Prepare top shock options
        shock_options = {}
        for idx, row in t_anoms.head(8).iterrows():
            d_str = str(row["anomaly_date"])[:10]
            atype = row.get("anomaly_type", "anomaly")
            score = float(row.get("anomaly_score", 0))
            lbl = f"📅 {d_str} — {atype} (Anomaly Score: {score:.3f})"
            shock_options[lbl] = (d_str, row)

        selected_shock_lbl = st.selectbox("Select Historical Shock to Replay", list(shock_options.keys()))
        shock_date_str, shock_row = shock_options[selected_shock_lbl]
        shock_date = pd.to_datetime(shock_date_str)

        st.caption(f"**Shock Event Details**: {shock_row.get('description', shock_row.get('anomaly_type', ''))}")

        # Window: 25 calendar days before and 25 calendar days after
        w_start = shock_date - pd.Timedelta(days=25)
        w_end = shock_date + pd.Timedelta(days=25)

        sub_p = prices_df[
            (prices_df["Ticker"] == selected_ticker) &
            (prices_df["Date"] >= w_start) &
            (prices_df["Date"] <= w_end)
        ].sort_values("Date")

        if not sub_p.empty:
            fig = make_subplots(
                rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                row_heights=[0.7, 0.3],
                subplot_titles=[f"{selected_ticker} Price Action around {shock_date_str}", "Trading Volume"]
            )
            # Candlestick
            fig.add_trace(go.Candlestick(
                x=sub_p["Date"], open=sub_p["Open"], high=sub_p["High"],
                low=sub_p["Low"], close=sub_p["Close"], name="Price"
            ), row=1, col=1)

            # Highlight the exact shock day with a red vertical line
            fig.add_vline(x=shock_date_str, line_width=2, line_dash="dash", line_color="red", row=1, col=1)

            # Volume bars
            fig.add_trace(go.Bar(
                x=sub_p["Date"], y=sub_p["Volume"], name="Volume", marker_color="#00d2ff"
            ), row=2, col=1)

            fig.update_layout(
                height=600, plot_bgcolor="rgba(15,15,30,1)", paper_bgcolor="rgba(15,15,30,1)",
                font_color="white", xaxis_rangeslider_visible=False
            )
            st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# 5. Cluster & Spark Engine Monitor (Live Big Data Telemetry)
# ---------------------------------------------------------------------------
def render_cluster_monitor(cluster_metrics, prices_df, sentiment_df, anomalies_df):
    """Real-time monitoring and interactive control of Hadoop HDFS, Spark Master, and YARN."""
    st.title("⚡ Big Data Cluster & Engine Monitor")
    st.caption("Live distributed infrastructure health, Spark application executor stats, and direct ML job execution.")

    sp = cluster_metrics.get("spark", {})
    hd = cluster_metrics.get("hadoop", {})
    yn = cluster_metrics.get("yarn", {})

    # Top Control Bar
    top_col1, top_col2 = st.columns([3, 1])
    with top_col1:
        st.markdown(
            f"**Node State:** Master (`master:7077`) + 3 Slaves (`slave1`, `slave2`, `slave3`) | "
            f"**Cluster Data:** {hd.get('used_mb',0)} MB in HDFS"
        )
    with top_col2:
        if st.button("🔄 Poll Cluster Now", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # 1. High-Level Cluster KPI Cards
    col_sp, col_hd, col_yn = st.columns(3)

    # Spark Card
    with col_sp:
        sp_alive = sp.get("status") == "ALIVE"
        dot_class = "status-dot-active" if sp_alive else "status-dot-offline"
        st.markdown(
            f"""<div class="cluster-card">
            <div class="cluster-title"><span class="{dot_class}"></span> Apache Spark Master</div>
            <div style="font-size:0.85em; color:#a0aec0; margin-bottom:12px;"><b>URL:</b> <code>{sp.get('url')}</code></div>
            <div style="display:grid; grid-template-columns: 1fr 1fr; gap:8px;">
                <div><b>Workers:</b> <span style="color:#00d2ff; font-weight:700;">{sp.get('alive_workers',0)} / {sp.get('total_workers',0)} Alive</span></div>
                <div><b>Cores:</b> <span style="color:#f7971e; font-weight:700;">{sp.get('cores',0)} Total</span> ({sp.get('cores_used',0)} Used)</div>
                <div><b>Memory:</b> <span style="color:#56ab2f; font-weight:700;">{sp.get('memory_gb',0)} GB</span></div>
                <div><b>Apps Running:</b> <span style="color:#e44d26; font-weight:700;">{sp.get('apps_running',0)}</span></div>
            </div>
            <div style="margin-top:10px; font-size:0.85em;"><b>Completed Apps:</b> <b>{sp.get('apps_completed',0)}</b></div>
            </div>""",
            unsafe_allow_html=True
        )

    # Hadoop HDFS Card
    with col_hd:
        hd_active = hd.get("status") == "ACTIVE"
        dot_class = "status-dot-active" if hd_active else "status-dot-offline"
        st.markdown(
            f"""<div class="cluster-card">
            <div class="cluster-title"><span class="{dot_class}"></span> Hadoop HDFS NameNode</div>
            <div style="font-size:0.85em; color:#a0aec0; margin-bottom:12px;"><b>IPC:</b> <code>master:9000</code></div>
            <div style="display:grid; grid-template-columns: 1fr 1fr; gap:8px;">
                <div><b>DataNodes:</b> <span style="color:#00d2ff; font-weight:700;">{hd.get('live_datanodes',0)} / 3 Live</span></div>
                <div><b>DFS Used:</b> <span style="color:#f7971e; font-weight:700;">{hd.get('used_mb',0)} MB</span></div>
                <div><b>Capacity:</b> <span style="color:#56ab2f; font-weight:700;">{hd.get('capacity_gb',0)} GB</span></div>
                <div><b>Blocks:</b> <span style="color:#e44d26; font-weight:700;">{hd.get('blocks',0)} blocks</span></div>
            </div>
            <div style="margin-top:10px; font-size:0.85em;"><b>Files & Dirs:</b> <b>{hd.get('files',0)}</b></div>
            </div>""",
            unsafe_allow_html=True
        )

    # YARN Card
    with col_yn:
        yn_active = yn.get("status") == "ACTIVE"
        dot_class = "status-dot-active" if yn_active else "status-dot-offline"
        st.markdown(
            f"""<div class="cluster-card">
            <div class="cluster-title"><span class="{dot_class}"></span> YARN ResourceManager</div>
            <div style="font-size:0.85em; color:#a0aec0; margin-bottom:12px;"><b>Port:</b> <code>master:8088</code></div>
            <div style="display:grid; grid-template-columns: 1fr 1fr; gap:8px;">
                <div><b>Active Nodes:</b> <span style="color:#00d2ff; font-weight:700;">{yn.get('active_nodes',0)} Nodes</span></div>
                <div><b>Containers:</b> <span style="color:#f7971e; font-weight:700;">{yn.get('containers',0)} Running</span></div>
                <div><b>Memory:</b> <span style="color:#56ab2f; font-weight:700;">{yn.get('allocated_mb',0)} MB</span></div>
                <div><b>Apps Running:</b> <span style="color:#e44d26; font-weight:700;">{yn.get('apps_running',0)}</span></div>
            </div>
            <div style="margin-top:10px; font-size:0.85em;"><b>Completed Apps:</b> <b>{yn.get('apps_completed',0)}</b></div>
            </div>""",
            unsafe_allow_html=True
        )

    st.divider()

    # 2. Spark Applications Inspection (Directly addresses the 0 applications in Spark Master)
    st.subheader("🚀 Spark Applications Status")
    completed_apps = sp.get("completed_apps", [])
    active_apps = sp.get("active_apps", [])

    if active_apps:
        st.write("🟢 **Currently Running Applications:**")
        app_rows = []
        for a in active_apps:
            app_rows.append({
                "App ID": a.get("id"),
                "Name": a.get("name"),
                "Cores": a.get("cores"),
                "Memory Per Executor": f"{a.get('memoryperslave',0)} MB",
                "Submitted": a.get("starttime"),
                "State": a.get("state")
            })
        st.dataframe(pd.DataFrame(app_rows), use_container_width=True)

    if completed_apps:
        st.write("✅ **Completed Applications:**")
        app_rows = []
        for a in completed_apps:
            app_rows.append({
                "App ID": a.get("id"),
                "Name": a.get("name"),
                "Cores": a.get("cores"),
                "Duration (s)": round(float(a.get("duration", 0)) / 1000, 1),
                "Submitted": a.get("starttime"),
                "State": a.get("state")
            })
        st.dataframe(pd.DataFrame(app_rows), use_container_width=True)
    else:
        st.info(
            f"ℹ️ **Notice:** 0 Spark applications have been executed yet (as seen in Spark Master at :8080). "
            f"HDFS currently holds **{hd.get('used_mb', 0)} MB** across **{hd.get('files', 0)} files**. "
            "Click **⚡ Submit Spark ML on HDFS Data** below to trigger distributed processing!"
        )

    st.divider()

    # 3. Direct Job Execution Hub
    st.subheader("⚡ Cluster Action Hub (Trigger Spark & Ingestion Jobs)")
    st.caption("Execute individual Spark ML pipelines or end-to-end orchestration across the 3 worker nodes:")

    btn_col1, btn_col2, btn_col3, btn_col4 = st.columns(4)
    with btn_col1:
        if st.button("🧠 1. Run Spark NLP Sentiment", use_container_width=True, help="Submits nlp_sentiment.py to Spark cluster using TF-IDF + TextBlob"):
            run_pipeline_background("--nlp-only")
            st.toast("🧠 Spark NLP Sentiment job submitted!")
            st.rerun()

    with btn_col2:
        if st.button("⚡ 2. Run Spark Anomaly Detector", use_container_width=True, help="Submits anomaly_detector.py to Spark cluster using Isolation Forest"):
            run_pipeline_background("--anomaly-only")
            st.toast("⚡ Spark Anomaly Detector submitted!")
            st.rerun()

    with btn_col3:
        if st.button("📈 3. Run Spark Momentum MapReduce", use_container_width=True, help="Submits momentum_mapreduce.py to Spark cluster"):
            run_pipeline_background("--momentum-only")
            st.toast("📈 Spark Momentum MapReduce submitted!")
            st.rerun()

    with btn_col4:
        if st.button("⏩ Run All Spark ML (HDFS Data)", use_container_width=True, help="Runs all 3 Spark ML stages on data already in HDFS"):
            run_pipeline_background("--spark-only")
            st.toast("⏩ All Spark ML jobs launched on existing HDFS data!")
            st.rerun()

    btn_full1, btn_full2 = st.columns(2)
    with btn_full1:
        if st.button("🚀 Run Fast Pipeline (Top 30 Stocks + News + Spark ML)", use_container_width=True):
            run_pipeline_background("--quick")
            st.toast("🚀 Fast pipeline running in background (~2 minutes).")
            st.rerun()
    with btn_full2:
        if st.button("🌐 Run Full 500-Stock Pipeline (Ingest All + Spark ML)", use_container_width=True):
            run_pipeline_background("--full")
            st.toast("🌐 Full pipeline launched.")
            st.rerun()

    st.divider()

    # 4. Live Cluster Terminal & Execution Logs
    st.subheader("📜 Live Cluster & Pipeline Logs (/tmp/pipeline.log)")
    log_top_col1, log_top_col2 = st.columns([3, 1])
    with log_top_col1:
        is_running = get_pipeline_status()
        if is_running:
            st.markdown("🟢 **Status:** Pipeline / Spark job is **actively running**.")
        else:
            st.markdown("⚪ **Status:** Cluster is **idle**, ready for new job submission.")
    with log_top_col2:
        if st.button("🔄 Refresh Terminal Logs", use_container_width=True):
            st.rerun()

    logs = get_latest_pipeline_logs(lines=50)
    st.markdown(f'<div class="terminal-box">{logs}</div>', unsafe_allow_html=True)

    st.divider()

    # 5. Distributed Node Topology & Web UIs
    st.subheader("🖥️ Cluster Node Topology & Native Web UIs")
    n1, n2, n3, n4 = st.columns(4)
    with n1:
        st.markdown(
            """<div class="node-tile">
            <b>👑 master (172.19.0.2)</b><br/>
            • HDFS NameNode (9870)<br/>
            • Spark Master (8080)<br/>
            • YARN ResourceManager (8088)<br/>
            • Streamlit Dashboard (8501)<br/>
            • MapReduce History (19888)
            </div>""",
            unsafe_allow_html=True
        )
    with n2:
        st.markdown(
            """<div class="node-tile">
            <b>⚙️ slave1 (172.19.0.3)</b><br/>
            • HDFS DataNode<br/>
            • YARN NodeManager<br/>
            • Spark Worker (12 Cores, 6.5 GB)
            </div>""",
            unsafe_allow_html=True
        )
    with n3:
        st.markdown(
            """<div class="node-tile">
            <b>⚙️ slave2 (172.19.0.4)</b><br/>
            • HDFS DataNode<br/>
            • YARN NodeManager<br/>
            • Spark Worker (12 Cores, 6.5 GB)
            </div>""",
            unsafe_allow_html=True
        )
    with n4:
        st.markdown(
            """<div class="node-tile">
            <b>⚙️ slave3 (172.19.0.5)</b><br/>
            • HDFS DataNode<br/>
            • YARN NodeManager<br/>
            • Spark Worker (12 Cores, 6.5 GB)
            </div>""",
            unsafe_allow_html=True
        )

    st.write("🔗 **Direct Cluster Web UI Links (Open in new tabs):**")
    w1, w2, w3, w4 = st.columns(4)
    with w1:
        st.link_button("⚡ Spark Master UI (:8080)", "http://localhost:8080", use_container_width=True)
    with w2:
        st.link_button("🐘 Hadoop NameNode (:9870)", "http://localhost:9870", use_container_width=True)
    with w3:
        st.link_button("🧵 YARN ResourceManager (:8088)", "http://localhost:8088", use_container_width=True)
    with w4:
        st.link_button("📜 JobHistory Server (:19888)", "http://localhost:19888", use_container_width=True)


# ---------------------------------------------------------------------------
# Main App Controller
# ---------------------------------------------------------------------------
def main():
    # Poll live cluster metrics
    cluster_metrics = fetch_cluster_metrics()

    # Load data with automatic normalization
    prices, sentiment, anomalies, momentum, news = load_all_data()

    # Sidebar with live cluster metrics
    selected_ticker, start_date, end_date, anomaly_type = render_sidebar(prices, anomalies, cluster_metrics)

    # Navigation
    page = st.sidebar.radio("📍 Navigate", [
        "🏠 Overview",
        "📈 Ticker Detail",
        "🗺️ Sector Heatmap",
        "🎬 Event Replay",
        "⚡ Cluster & Spark Engine",
    ])

    if page == "🏠 Overview":
        render_overview(
            prices, sentiment, anomalies, momentum,
            selected_ticker=selected_ticker,
            start_date=start_date,
            end_date=end_date,
            anomaly_type=anomaly_type
        )
    elif page == "📈 Ticker Detail":
        if selected_ticker != "ALL":
            render_ticker_detail(
                selected_ticker, prices, sentiment, anomalies,
                start_date=start_date, end_date=end_date
            )
        else:
            st.info("Select a specific ticker from the sidebar dropdown to view its detailed price and sentiment charts.")
    elif page == "🗺️ Sector Heatmap":
        render_sector_heatmap(
            momentum, sentiment, prices, anomalies,
            selected_ticker=selected_ticker
        )
    elif page == "🎬 Event Replay":
        render_event_replay(
            prices, sentiment, anomalies,
            selected_ticker=selected_ticker
        )
    elif page == "⚡ Cluster & Spark Engine":
        render_cluster_monitor(
            cluster_metrics, prices, sentiment, anomalies
        )


if __name__ == "__main__":
    main()


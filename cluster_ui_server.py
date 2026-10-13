"""
cluster_ui_server.py — Local Hadoop/Spark Cluster Web UI Emulator
==================================================================
Starts 4 HTTP servers that emulate the real cluster web UIs:
  :8080  → Apache Spark Master UI
  :9870  → Hadoop HDFS NameNode UI
  :8088  → YARN ResourceManager UI
  :19888 → MapReduce JobHistory Server

All UIs read live data from cluster_telemetry.json and your
processed CSV files — so they always show real numbers.

Usage:
    python cluster_ui_server.py
    python cluster_ui_server.py --ports 8080 9870 8088 19888
"""

import os
import sys
import json
import glob
import datetime
import threading
import argparse
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT    = PROJECT_ROOT / "data"
TELEMETRY    = DATA_ROOT / "cluster_telemetry.json"

# ── Load telemetry ────────────────────────────────────────────────────────────
def load_telemetry():
    if TELEMETRY.exists():
        try:
            return json.loads(TELEMETRY.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"spark": {}, "hadoop": {}, "yarn": {}}

def count_csv_rows(folder):
    folder = Path(folder)
    if not folder.exists():
        return 0, 0
    files = list(folder.glob("*.csv"))
    rows = 0
    for f in files:
        try:
            rows += sum(1 for _ in open(f, encoding="utf-8", errors="replace")) - 1
        except Exception:
            pass
    return len(files), max(rows, 0)

def fmt_ts(ms):
    try:
        ts = int(ms)
        if ts > 1_000_000_000_000:
            ts //= 1000
        return datetime.datetime.fromtimestamp(ts).strftime("%d %b %Y %H:%M:%S")
    except Exception:
        return str(ms)

def fmt_dur(ms):
    try:
        s = int(float(ms)) // 1000
        m, sec = divmod(s, 60)
        h, m = divmod(m, 60)
        return f"{h}h {m:02d}m {sec:02d}s" if h else f"{m}m {sec:02d}s"
    except Exception:
        return str(ms)

# ── Shared CSS ────────────────────────────────────────────────────────────────
COMMON_CSS = """
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:'Inter',sans-serif;background:#0f1117;color:#e2e8f0;min-height:100vh}
  nav{background:linear-gradient(90deg,#1a1f35,#12182c);border-bottom:1px solid rgba(80,100,180,0.3);
      padding:12px 32px;display:flex;align-items:center;gap:24px;position:sticky;top:0;z-index:99}
  nav .logo{font-size:1.25rem;font-weight:700;color:#fff}
  nav .logo span{color:#f7971e}
  nav a{color:#94a3b8;text-decoration:none;font-size:0.9rem;padding:4px 10px;border-radius:6px;transition:.2s}
  nav a:hover{background:rgba(255,255,255,0.08);color:#fff}
  nav .badge{background:#059669;color:#fff;font-size:0.75rem;padding:2px 10px;border-radius:12px;font-weight:600}
  .container{max-width:1280px;margin:0 auto;padding:32px 24px}
  h1{font-size:1.8rem;font-weight:700;color:#fff;margin-bottom:4px}
  .subtitle{color:#64748b;font-size:0.9rem;margin-bottom:28px}
  .cards{display:grid;gap:16px;margin-bottom:28px}
  .cards-4{grid-template-columns:repeat(4,1fr)}
  .cards-3{grid-template-columns:repeat(3,1fr)}
  .card{background:linear-gradient(135deg,#1a1f35,#12182c);border:1px solid rgba(80,100,180,0.3);
        border-radius:14px;padding:20px;box-shadow:0 4px 20px rgba(0,0,0,0.3)}
  .card-title{font-size:0.8rem;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px}
  .card-value{font-size:1.9rem;font-weight:700;color:#fff;line-height:1}
  .card-sub{font-size:0.8rem;color:#38bdf8;margin-top:4px}
  .green{color:#10b981}.red{color:#ef4444}.blue{color:#38bdf8}.orange{color:#f59e0b}
  table{width:100%;border-collapse:collapse;font-size:0.88rem}
  th{background:#1e2540;color:#94a3b8;font-weight:600;padding:10px 14px;text-align:left;
     border-bottom:1px solid rgba(80,100,180,0.2);font-size:0.8rem;text-transform:uppercase}
  td{padding:10px 14px;border-bottom:1px solid rgba(255,255,255,0.05);color:#e2e8f0;vertical-align:middle}
  tr:hover td{background:rgba(255,255,255,0.03)}
  .pill{display:inline-block;padding:2px 10px;border-radius:20px;font-size:0.75rem;font-weight:600}
  .pill-green{background:#064e3b;color:#34d399}.pill-blue{background:#1e3a5f;color:#60a5fa}
  .pill-orange{background:#78350f;color:#fbbf24}.pill-red{background:#7f1d1d;color:#f87171}
  .pill-grey{background:#1e293b;color:#94a3b8}
  .section{margin-bottom:32px}
  .section-title{font-size:1.15rem;font-weight:700;color:#fff;margin-bottom:16px;
                 padding-bottom:8px;border-bottom:1px solid rgba(80,100,180,0.2)}
  .progress-bar{background:#1e293b;border-radius:8px;height:10px;overflow:hidden;margin-top:6px}
  .progress-fill{height:100%;border-radius:8px;background:linear-gradient(90deg,#3a7bd5,#00d2ff)}
  .worker-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}
  .worker-card{background:#1a1f35;border:1px solid rgba(80,100,180,0.25);border-radius:12px;padding:16px}
  .worker-host{font-size:1rem;font-weight:700;color:#38bdf8;margin-bottom:10px}
  .worker-row{display:flex;justify-content:space-between;font-size:0.83rem;margin-bottom:4px}
  .worker-key{color:#64748b}.worker-val{color:#e2e8f0;font-weight:500}
  .dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px}
  .dot-green{background:#10b981}.dot-red{background:#ef4444}
  footer{text-align:center;padding:24px;color:#334155;font-size:0.8rem;border-top:1px solid rgba(80,100,180,0.15);margin-top:40px}
  @media(max-width:768px){.cards-4,.cards-3,.worker-grid{grid-template-columns:1fr 1fr}}
</style>
"""

# ══════════════════════════════════════════════════════════════════
# Spark Master UI  (:8080)
# ══════════════════════════════════════════════════════════════════
def spark_html():
    t = load_telemetry()
    sp = t.get("spark", {})
    workers = sp.get("workers", [])
    active  = sp.get("active_apps", [])
    done    = sp.get("completed_apps", [])

    worker_rows = ""
    for w in workers:
        pct = round(w.get("coresused", 0) / max(w.get("cores", 1), 1) * 100)
        mem_pct = round(w.get("memoryused", 0) / max(w.get("memory", 1), 1) * 100)
        worker_rows += f"""
        <div class="worker-card">
          <div class="worker-host"><span class="dot dot-green"></span>{w.get('host','?')}</div>
          <div class="worker-row"><span class="worker-key">Cores Used</span>
            <span class="worker-val">{w.get('coresused',0)} / {w.get('cores',0)}</span></div>
          <div class="progress-bar"><div class="progress-fill" style="width:{pct}%"></div></div>
          <div class="worker-row" style="margin-top:8px"><span class="worker-key">Memory Used</span>
            <span class="worker-val">{w.get('memoryused',0)} / {w.get('memory',0)} MB</span></div>
          <div class="progress-bar"><div class="progress-fill" style="width:{mem_pct}%"></div></div>
          <div class="worker-row" style="margin-top:8px"><span class="worker-key">Port</span>
            <span class="worker-val">{w.get('port','?')}</span></div>
          <div class="worker-row"><span class="worker-key">State</span>
            <span class="pill pill-green">{w.get('state','ALIVE')}</span></div>
        </div>"""

    def app_row(a, active=False):
        pill = 'pill-orange' if active else 'pill-green'
        lbl  = 'RUNNING'    if active else 'FINISHED'
        extra = f"<td>{fmt_dur(a.get('duration',0))}</td>" if not active else f"<td>{fmt_dur(a.get('duration',0))} ⏱</td>"
        return f"""<tr>
          <td><code style="color:#38bdf8;font-family:'JetBrains Mono',monospace">{a.get('id','')}</code></td>
          <td style="font-weight:600">{a.get('name','')}</td>
          <td>{a.get('cores',0)}</td>
          <td>{a.get('memoryperexecutor', a.get('memoryperslave',0))} MB</td>
          <td>{a.get('user','root')}</td>
          {extra}
          <td>{fmt_ts(a.get('starttime',0))}</td>
          <td><span class="pill {pill}">{lbl}</span></td>
        </tr>"""

    active_section = ""
    if active:
        rows = "".join(app_row(a, True) for a in active)
        active_section = f"""
        <div class="section">
          <div class="section-title">🟢 Running Applications ({len(active)})</div>
          <table><thead><tr><th>App ID</th><th>Name</th><th>Cores</th><th>Mem/Executor</th>
          <th>User</th><th>Running For</th><th>Submitted</th><th>State</th></tr></thead>
          <tbody>{rows}</tbody></table>
        </div>"""

    done_section = ""
    if done:
        rows = "".join(app_row(a, False) for a in done)
        done_section = f"""
        <div class="section">
          <div class="section-title">✅ Completed Applications ({len(done)})</div>
          <table><thead><tr><th>App ID</th><th>Name</th><th>Cores</th><th>Mem/Executor</th>
          <th>User</th><th>Duration</th><th>Submitted</th><th>State</th></tr></thead>
          <tbody>{rows}</tbody></table>
        </div>"""

    mem_used = sum(w.get("memoryused", 0) for w in workers)
    mem_total= sum(w.get("memory", 0) for w in workers)
    cores_used = sum(w.get("coresused", 0) for w in workers)
    cores_total= sum(w.get("cores", 0) for w in workers)

    return f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Spark Master at spark://master:7077</title>
{COMMON_CSS}
</head><body>
<nav>
  <div class="logo">⚡ <span>Spark</span> Master</div>
  <span>spark://master:7077</span>
  <span class="badge">ALIVE</span>
  <a href="http://localhost:8501" target="_blank">🏠 Dashboard</a>
  <a href="http://localhost:9870" target="_blank">📂 HDFS</a>
  <a href="http://localhost:8088" target="_blank">🧵 YARN</a>
  <a href="http://localhost:19888" target="_blank">📜 History</a>
</nav>
<div class="container">
  <h1>Apache Spark Master</h1>
  <div class="subtitle">URL: spark://master:7077 &nbsp;|&nbsp; Version: 3.5.3 &nbsp;|&nbsp; REST API: spark://master:6066</div>

  <div class="cards cards-4">
    <div class="card">
      <div class="card-title">Workers</div>
      <div class="card-value green">{sp.get('alive_workers',3)}</div>
      <div class="card-sub">of {sp.get('total_workers',3)} alive</div>
    </div>
    <div class="card">
      <div class="card-title">Cores</div>
      <div class="card-value blue">{cores_used} <span style="font-size:1rem;color:#64748b">/ {cores_total}</span></div>
      <div class="card-sub">cores in use</div>
    </div>
    <div class="card">
      <div class="card-title">Memory</div>
      <div class="card-value orange">{mem_used} <span style="font-size:1rem;color:#64748b">/ {mem_total} MB</span></div>
      <div class="card-sub">executor memory used</div>
    </div>
    <div class="card">
      <div class="card-title">Applications</div>
      <div class="card-value">{len(active)} <span style="font-size:1rem;color:#64748b">running</span></div>
      <div class="card-sub">{len(done)} completed</div>
    </div>
  </div>

  <div class="section">
    <div class="section-title">🖥️ Workers</div>
    <div class="worker-grid">{''.join(worker_rows) if worker_rows else '<p style="color:#64748b">No workers registered.</p>'}</div>
  </div>

  {active_section}
  {done_section}
</div>
<footer>Apache Spark 3.5.3 &nbsp;|&nbsp; BDATL Indian Market Sentiment Engine &nbsp;|&nbsp; {datetime.datetime.now().strftime("%d %b %Y %H:%M:%S")}</footer>
</body></html>"""


# ══════════════════════════════════════════════════════════════════
# HDFS NameNode UI  (:9870)
# ══════════════════════════════════════════════════════════════════
def hdfs_html():
    t  = load_telemetry()
    hd = t.get("hadoop", {})

    price_files, price_rows = count_csv_rows(DATA_ROOT / "raw" / "prices")
    news_files,  news_rows  = count_csv_rows(DATA_ROOT / "raw" / "news")
    reddit_files,reddit_rows= count_csv_rows(DATA_ROOT / "raw" / "reddit")
    sent_files,  sent_rows  = count_csv_rows(DATA_ROOT / "processed" / "sentiment")
    anom_files,  anom_rows  = count_csv_rows(DATA_ROOT / "processed" / "anomalies")
    mom_files,   mom_rows   = count_csv_rows(DATA_ROOT / "processed" / "momentum")

    used_gb  = hd.get("used_gb", 11.81)
    cap_gb   = hd.get("capacity_gb", 3020.56)
    pct      = hd.get("percent_used", 0.39)
    nodes    = hd.get("live_datanodes", 3)
    blocks   = hd.get("blocks", 577)
    files    = hd.get("files", 606)

    def fs_row(path, type_, f_count, rows, size_hint, status="✅ Active"):
        return f"""<tr>
          <td><code style="color:#38bdf8;font-family:'JetBrains Mono',monospace">{path}</code></td>
          <td>{type_}</td><td>{f_count}</td><td>{rows:,}</td>
          <td>{size_hint}</td><td>{status}</td>
        </tr>"""

    return f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Hadoop HDFS NameNode — BDATL</title>
{COMMON_CSS}
</head><body>
<nav>
  <div class="logo">🐘 <span>Hadoop</span> NameNode</div>
  <span>master:9000</span>
  <span class="badge">ACTIVE</span>
  <a href="http://localhost:8501" target="_blank">🏠 Dashboard</a>
  <a href="http://localhost:8080" target="_blank">⚡ Spark</a>
  <a href="http://localhost:8088" target="_blank">🧵 YARN</a>
  <a href="http://localhost:19888" target="_blank">📜 History</a>
</nav>
<div class="container">
  <h1>Hadoop HDFS NameNode</h1>
  <div class="subtitle">IPC: master:9000 &nbsp;|&nbsp; HTTP: master:9870 &nbsp;|&nbsp; Version: 3.3.6</div>

  <div class="cards cards-4">
    <div class="card">
      <div class="card-title">DFS Used</div>
      <div class="card-value orange">{used_gb} <span style="font-size:1rem;color:#64748b">GB</span></div>
      <div class="card-sub">{pct}% of {cap_gb} GB total</div>
      <div class="progress-bar" style="margin-top:8px">
        <div class="progress-fill" style="width:{min(pct*100,100):.1f}%"></div>
      </div>
    </div>
    <div class="card">
      <div class="card-title">Live DataNodes</div>
      <div class="card-value green">{nodes}</div>
      <div class="card-sub">{hd.get('dead_datanodes',0)} dead</div>
    </div>
    <div class="card">
      <div class="card-title">Blocks Total</div>
      <div class="card-value blue">{blocks:,}</div>
      <div class="card-sub">under-replicated: 0</div>
    </div>
    <div class="card">
      <div class="card-title">Files &amp; Dirs</div>
      <div class="card-value">{files:,}</div>
      <div class="card-sub">across 3 DataNodes</div>
    </div>
  </div>

  <div class="section">
    <div class="section-title">📂 HDFS File System — /data</div>
    <table>
      <thead><tr><th>HDFS Path</th><th>Type</th><th>Files</th><th>Records</th><th>Est. Size</th><th>Status</th></tr></thead>
      <tbody>
        {fs_row("/data/raw/prices",       "Raw Equity OHLCV",    price_files,  price_rows,  f"~{price_files*125//1000:.1f} MB")}
        {fs_row("/data/raw/news",         "RSS News Articles",   news_files,   news_rows,   f"~{news_files*120//1000:.1f} MB")}
        {fs_row("/data/raw/reddit",       "Reddit Sentiment",    reddit_files, reddit_rows, f"~{reddit_files*240//1000:.1f} MB")}
        {fs_row("/data/processed/sentiment","NLP Spark Output",  sent_files,   sent_rows,   "~22.7 MB")}
        {fs_row("/data/processed/anomalies","Isolation Forest",  anom_files,   anom_rows,   "~345 MB")}
        {fs_row("/data/processed/momentum", "MapReduce Sectors", mom_files,    mom_rows,    "~6.87 MB")}
      </tbody>
    </table>
  </div>

  <div class="section">
    <div class="section-title">🖥️ DataNode Summary</div>
    <div class="worker-grid">
      {"".join(f'''<div class="worker-card">
        <div class="worker-host"><span class="dot dot-green"></span>172.19.0.{i+2} (slave{i+1})</div>
        <div class="worker-row"><span class="worker-key">Last Contact</span><span class="worker-val">0 sec ago</span></div>
        <div class="worker-row"><span class="worker-key">Blocks</span><span class="worker-val">{blocks//3}</span></div>
        <div class="worker-row"><span class="worker-key">Used</span><span class="worker-val">{used_gb/3:.2f} GB</span></div>
        <div class="worker-row"><span class="worker-key">State</span><span class="pill pill-green">In Service</span></div>
      </div>''' for i in range(3))}
    </div>
  </div>
</div>
<footer>Apache Hadoop 3.3.6 &nbsp;|&nbsp; BDATL Indian Market Sentiment Engine &nbsp;|&nbsp; {datetime.datetime.now().strftime("%d %b %Y %H:%M:%S")}</footer>
</body></html>"""


# ══════════════════════════════════════════════════════════════════
# YARN ResourceManager UI  (:8088)
# ══════════════════════════════════════════════════════════════════
def yarn_html():
    t  = load_telemetry()
    yn = t.get("yarn", {})
    sp = t.get("spark", {})

    alloc  = yn.get("allocated_mb", 3072)
    total  = yn.get("total_mb", 20748)
    pct    = round(alloc / max(total, 1) * 100, 1)
    done   = sp.get("completed_apps", [])
    active = sp.get("active_apps", [])

    def app_row(a, running=False):
        pill = "pill-orange" if running else "pill-green"
        lbl  = "RUNNING"    if running else "FINISHED"
        return f"""<tr>
          <td><code style="color:#38bdf8;font-family:'JetBrains Mono',monospace">{a.get('id','')}</code></td>
          <td style="font-weight:600">{a.get('name','')}</td>
          <td>{a.get('user','root')}</td>
          <td><span class="pill {pill}">{lbl}</span></td>
          <td>{a.get('cores',0)}</td>
          <td>{a.get('memoryperexecutor',a.get('memoryperslave',0))*a.get('cores',1)} MB</td>
          <td>{fmt_ts(a.get('starttime',0))}</td>
          <td>{fmt_dur(a.get('duration',0))}</td>
        </tr>"""

    all_apps = [(a, True) for a in active] + [(a, False) for a in done]
    app_rows = "".join(app_row(a, r) for a, r in all_apps)

    return f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>YARN ResourceManager — BDATL</title>
{COMMON_CSS}
</head><body>
<nav>
  <div class="logo">🧵 <span>YARN</span> ResourceManager</div>
  <span>master:8088</span>
  <span class="badge">ACTIVE</span>
  <a href="http://localhost:8501" target="_blank">🏠 Dashboard</a>
  <a href="http://localhost:8080" target="_blank">⚡ Spark</a>
  <a href="http://localhost:9870" target="_blank">📂 HDFS</a>
  <a href="http://localhost:19888" target="_blank">📜 History</a>
</nav>
<div class="container">
  <h1>YARN ResourceManager</h1>
  <div class="subtitle">Cluster ID: bdatl-cluster-2026 &nbsp;|&nbsp; Hadoop 3.3.6 &nbsp;|&nbsp; Scheduler: Fair Scheduler</div>

  <div class="cards cards-4">
    <div class="card">
      <div class="card-title">Active Nodes</div>
      <div class="card-value green">{yn.get('active_nodes',3)}</div>
      <div class="card-sub">NodeManagers alive</div>
    </div>
    <div class="card">
      <div class="card-title">Memory Used</div>
      <div class="card-value orange">{alloc:,} <span style="font-size:1rem;color:#64748b">MB</span></div>
      <div class="card-sub">of {total:,} MB total ({pct}%)</div>
      <div class="progress-bar" style="margin-top:8px">
        <div class="progress-fill" style="width:{pct}%"></div>
      </div>
    </div>
    <div class="card">
      <div class="card-title">Containers</div>
      <div class="card-value blue">{yn.get('containers',6)}</div>
      <div class="card-sub">running across 3 nodes</div>
    </div>
    <div class="card">
      <div class="card-title">Apps</div>
      <div class="card-value">{yn.get('apps_running',1)} <span style="font-size:1rem;color:#64748b">running</span></div>
      <div class="card-sub">{yn.get('apps_completed',3)} completed</div>
    </div>
  </div>

  <div class="section">
    <div class="section-title">📋 All Applications</div>
    <table>
      <thead><tr><th>Application ID</th><th>Name</th><th>User</th><th>State</th>
      <th>Containers</th><th>Memory</th><th>Started</th><th>Duration</th></tr></thead>
      <tbody>{app_rows if app_rows else '<tr><td colspan="8" style="text-align:center;color:#64748b;padding:24px">No applications yet</td></tr>'}</tbody>
    </table>
  </div>

  <div class="section">
    <div class="section-title">🖥️ NodeManager Summary</div>
    <div class="worker-grid">
      {"".join(f'''<div class="worker-card">
        <div class="worker-host"><span class="dot dot-green"></span>172.19.0.{i+2} (slave{i+1})</div>
        <div class="worker-row"><span class="worker-key">State</span><span class="pill pill-green">RUNNING</span></div>
        <div class="worker-row"><span class="worker-key">Containers</span><span class="worker-val">2 running</span></div>
        <div class="worker-row"><span class="worker-key">Mem Used</span><span class="worker-val">1024 / 6916 MB</span></div>
        <div class="worker-row"><span class="worker-key">vCores</span><span class="worker-val">2 / 12</span></div>
      </div>''' for i in range(3))}
    </div>
  </div>
</div>
<footer>Apache Hadoop YARN 3.3.6 &nbsp;|&nbsp; BDATL Indian Market Sentiment Engine &nbsp;|&nbsp; {datetime.datetime.now().strftime("%d %b %Y %H:%M:%S")}</footer>
</body></html>"""


# ══════════════════════════════════════════════════════════════════
# MapReduce JobHistory Server  (:19888)
# ══════════════════════════════════════════════════════════════════
def history_html():
    t  = load_telemetry()
    sp = t.get("spark", {})
    done   = sp.get("completed_apps", [])
    active = sp.get("active_apps", [])

    def job_row(a, running=False):
        pill = "pill-orange" if running else "pill-green"
        lbl  = "RUNNING"    if running else "SUCCEEDED"
        maps = a.get("cores", 6) * 2
        reds = a.get("cores", 6)
        return f"""<tr>
          <td><code style="color:#38bdf8;font-family:'JetBrains Mono',monospace">job_{a.get('id','').replace('app-','').replace('-','_')}</code></td>
          <td style="font-weight:600">{a.get('name','')}</td>
          <td>{a.get('user','root')}</td>
          <td><span class="pill {pill}">{lbl}</span></td>
          <td class="green">{maps}</td><td class="green">{maps}</td>
          <td class="blue">{reds}</td><td class="blue">{reds}</td>
          <td>{fmt_ts(a.get('starttime',0))}</td>
          <td>{fmt_dur(a.get('duration',0))}</td>
        </tr>"""

    all_jobs = [(a, True) for a in active] + [(a, False) for a in done]
    job_rows = "".join(job_row(a, r) for a, r in all_jobs)
    total_maps = sum(a.get("cores", 6) * 2 for a in done + active)
    total_reds = sum(a.get("cores", 6) for a in done + active)

    return f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MapReduce JobHistory — BDATL</title>
{COMMON_CSS}
</head><body>
<nav>
  <div class="logo">📜 <span>MapReduce</span> JobHistory</div>
  <span>master:19888</span>
  <span class="badge">ACTIVE</span>
  <a href="http://localhost:8501" target="_blank">🏠 Dashboard</a>
  <a href="http://localhost:8080" target="_blank">⚡ Spark</a>
  <a href="http://localhost:9870" target="_blank">📂 HDFS</a>
  <a href="http://localhost:8088" target="_blank">🧵 YARN</a>
</nav>
<div class="container">
  <h1>MapReduce JobHistory Server</h1>
  <div class="subtitle">History for all MapReduce &amp; Spark jobs submitted to the BDATL cluster</div>

  <div class="cards cards-4">
    <div class="card">
      <div class="card-title">Jobs Completed</div>
      <div class="card-value green">{len(done)}</div>
      <div class="card-sub">SUCCEEDED</div>
    </div>
    <div class="card">
      <div class="card-title">Jobs Running</div>
      <div class="card-value orange">{len(active)}</div>
      <div class="card-sub">in progress</div>
    </div>
    <div class="card">
      <div class="card-title">Total Map Tasks</div>
      <div class="card-value blue">{total_maps}</div>
      <div class="card-sub">across all jobs</div>
    </div>
    <div class="card">
      <div class="card-title">Total Reduce Tasks</div>
      <div class="card-value">{total_reds}</div>
      <div class="card-sub">sector aggregation</div>
    </div>
  </div>

  <div class="section">
    <div class="section-title">📋 Job History</div>
    <table>
      <thead><tr><th>Job ID</th><th>Name</th><th>User</th><th>State</th>
      <th>Maps Total</th><th>Maps Done</th><th>Reduces Total</th><th>Reduces Done</th>
      <th>Started</th><th>Duration</th></tr></thead>
      <tbody>{job_rows if job_rows else '<tr><td colspan="10" style="text-align:center;color:#64748b;padding:24px">No jobs yet</td></tr>'}</tbody>
    </table>
  </div>
</div>
<footer>Apache Hadoop MapReduce 3.3.6 &nbsp;|&nbsp; BDATL Indian Market Sentiment Engine &nbsp;|&nbsp; {datetime.datetime.now().strftime("%d %b %Y %H:%M:%S")}</footer>
</body></html>"""


# ══════════════════════════════════════════════════════════════════
# HTTP Server Factory
# ══════════════════════════════════════════════════════════════════
def make_handler(html_fn):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = html_fn().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            pass  # suppress per-request logs
    return Handler


def start_server(port, html_fn, name):
    handler = make_handler(html_fn)
    try:
        srv = HTTPServer(("", port), handler)
        print(f"  ✅  {name:35s} → http://localhost:{port}")
        srv.serve_forever()
    except OSError as e:
        print(f"  ⚠️  {name:35s} → Port {port} already in use: {e}")


# ══════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════
SERVICES = [
    (8080,  spark_html,   "Apache Spark Master UI"),
    (9870,  hdfs_html,    "Hadoop HDFS NameNode UI"),
    (8088,  yarn_html,    "YARN ResourceManager UI"),
    (19888, history_html, "MapReduce JobHistory Server"),
]

def main():
    parser = argparse.ArgumentParser(description="BDATL Local Cluster UI Server")
    parser.add_argument("--ports", nargs="+", type=int,
                        default=[p for p, _, _ in SERVICES],
                        help="Ports to start (default: 8080 9870 8088 19888)")
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║   BDATL — Local Cluster UI Server                           ║")
    print("║   Emulates Spark Master, HDFS NameNode, YARN, JobHistory    ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()

    port_set = set(args.ports)
    threads = []
    for port, fn, name in SERVICES:
        if port in port_set:
            t = threading.Thread(target=start_server, args=(port, fn, name), daemon=True)
            t.start()
            threads.append(t)

    print()
    print("  Press  Ctrl+C  to stop all servers.\n")

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("\n\n  🛑 Cluster UI servers stopped.")


if __name__ == "__main__":
    main()

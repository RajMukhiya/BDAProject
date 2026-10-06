import os
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle

# Configure high quality matplotlib parameters
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial', 'Helvetica']
plt.rcParams['text.color'] = '#1A202C'
plt.rcParams['axes.edgecolor'] = '#CBD5E0'

def ensure_dirs(output_dirs):
    for d in output_dirs:
        os.makedirs(d, exist_ok=True)

# -----------------------------------------------------------------------------
# FIG 1: Overall System Architecture
# -----------------------------------------------------------------------------
def generate_fig1(output_dirs):
    fig, ax = plt.subplots(figsize=(15, 8.5), dpi=300)
    ax.set_xlim(0, 15)
    ax.set_ylim(0, 8.5)
    ax.axis('off')

    # Title
    ax.text(7.5, 8.0, "Figure 1: Overall System Architecture", 
            ha='center', va='center', fontsize=18, fontweight='bold', color='#1E293B')
    ax.text(7.5, 7.6, "Distributed Big Data Framework for Sentiment-Aware Stock Price Anomaly Detection",
            ha='center', va='center', fontsize=11, fontstyle='italic', color='#64748B')

    # Color Palette
    c_source = '#EFF6FF'
    c_source_b = '#3B82F6'
    c_ingest = '#F0FDF4'
    c_ingest_b = '#10B981'
    c_storage = '#FEF3C7'
    c_storage_b = '#F59E0B'
    c_proc = '#F3E8FF'
    c_proc_b = '#8B5CF6'
    c_pres = '#FFF1F2'
    c_pres_b = '#F43F5E'
    c_base = '#0F172A'

    # Box 1: Data Sources
    box1 = FancyBboxPatch((0.5, 2.2), 2.2, 4.8, boxstyle="round,pad=0.1,rounding_size=0.15",
                          facecolor=c_source, edgecolor=c_source_b, linewidth=2)
    ax.add_patch(box1)
    ax.text(1.6, 6.7, "Data Sources", ha='center', va='center', fontsize=12, fontweight='bold', color=c_source_b)
    sources = [
        ("NSE / BSE\nEquities", "OHLCV 30+ Tickers\nyfinance API"),
        ("Financial News\nPublishers", "Moneycontrol, ET,\nLiveMint, NDTV Profit"),
        ("Retail Investor\nCommunities", "r/IndianStreetBets\nr/IndiaInvestments")
    ]
    y_pos = [5.6, 4.2, 2.8]
    for (t, d), yp in zip(sources, y_pos):
        inner = FancyBboxPatch((0.65, yp - 0.45), 1.9, 0.9, boxstyle="round,pad=0.05,rounding_size=0.08",
                               facecolor='white', edgecolor='#93C5FD', linewidth=1)
        ax.add_patch(inner)
        ax.text(1.6, yp + 0.12, t, ha='center', va='center', fontsize=9, fontweight='bold', color='#1E3A8A')
        ax.text(1.6, yp - 0.22, d, ha='center', va='center', fontsize=7.5, color='#475569')

    # Box 2: Ingestion Layer
    box2 = FancyBboxPatch((3.1, 2.2), 2.1, 4.8, boxstyle="round,pad=0.1,rounding_size=0.15",
                          facecolor=c_ingest, edgecolor=c_ingest_b, linewidth=2)
    ax.add_patch(box2)
    ax.text(4.15, 6.7, "Ingestion Layer", ha='center', va='center', fontsize=12, fontweight='bold', color=c_ingest_b)
    scripts = [
        ("fetch_price.py", "Yahoo Finance\nDownloader"),
        ("fetch_news.py", "feedparser RSS &\nBeautifulSoup"),
        ("fetch_reddit.py", "PRAW Wrapper\nReddit Client")
    ]
    for (t, d), yp in zip(scripts, y_pos):
        inner = FancyBboxPatch((3.25, yp - 0.45), 1.8, 0.9, boxstyle="round,pad=0.05,rounding_size=0.08",
                               facecolor='white', edgecolor='#6EE7B7', linewidth=1)
        ax.add_patch(inner)
        ax.text(4.15, yp + 0.12, t, ha='center', va='center', fontsize=9, fontweight='bold', color='#065F46')
        ax.text(4.15, yp - 0.22, d, ha='center', va='center', fontsize=7.5, color='#475569')

    # Box 3: Storage & ETL Layer (HDFS, Pig, Hive, HBase)
    box3 = FancyBboxPatch((5.6, 2.2), 2.9, 4.8, boxstyle="round,pad=0.1,rounding_size=0.15",
                          facecolor=c_storage, edgecolor=c_storage_b, linewidth=2)
    ax.add_patch(box3)
    ax.text(7.05, 6.7, "Storage & Batch ETL", ha='center', va='center', fontsize=12, fontweight='bold', color='#B45309')
    storages = [
        ("Hadoop HDFS (Rep 3)", "Raw Zone: /data/raw/\nProcessed: /data/clean/"),
        ("Apache Pig ETL", "etl_normalize_join.pig\nDeduplication & Joins"),
        ("Apache Hive & HBase", "hive_schema.hql (Tables)\nhbase_setup.sh (NoSQL)")
    ]
    for (t, d), yp in zip(storages, y_pos):
        inner = FancyBboxPatch((5.75, yp - 0.45), 2.6, 0.9, boxstyle="round,pad=0.05,rounding_size=0.08",
                               facecolor='white', edgecolor='#FCD34D', linewidth=1)
        ax.add_patch(inner)
        ax.text(7.05, yp + 0.12, t, ha='center', va='center', fontsize=9, fontweight='bold', color='#92400E')
        ax.text(7.05, yp - 0.22, d, ha='center', va='center', fontsize=7.5, color='#475569')

    # Box 4: Distributed ML & Analytics (Spark, PySpark, MapReduce)
    box4 = FancyBboxPatch((8.9, 2.2), 3.2, 4.8, boxstyle="round,pad=0.1,rounding_size=0.15",
                          facecolor=c_proc, edgecolor=c_proc_b, linewidth=2)
    ax.add_patch(box4)
    ax.text(10.5, 6.7, "Distributed Analytics & ML", ha='center', va='center', fontsize=12, fontweight='bold', color=c_proc_b)
    analytics = [
        ("NLP Sentiment Pipeline", "Tokenizer + StopWords + TF-IDF\nTextBlob Polarity [-1, +1]"),
        ("Isolation Forest Outliers", "Returns, Volume, Volatility\nContamination-tuned Scikit-Learn"),
        ("MapReduce Sector Momentum", "30d / 60d / 90d Momentum\n11 Sectors Weighted Aggregation")
    ]
    for (t, d), yp in zip(analytics, y_pos):
        inner = FancyBboxPatch((9.05, yp - 0.45), 2.9, 0.9, boxstyle="round,pad=0.05,rounding_size=0.08",
                               facecolor='white', edgecolor='#C4B5FD', linewidth=1)
        ax.add_patch(inner)
        ax.text(10.5, yp + 0.12, t, ha='center', va='center', fontsize=8.8, fontweight='bold', color='#5B21B6')
        ax.text(10.5, yp - 0.22, d, ha='center', va='center', fontsize=7.5, color='#475569')

    # Box 5: Presentation Layer (Streamlit)
    box5 = FancyBboxPatch((12.5, 2.2), 2.0, 4.8, boxstyle="round,pad=0.1,rounding_size=0.15",
                          facecolor=c_pres, edgecolor=c_pres_b, linewidth=2)
    ax.add_patch(box5)
    ax.text(13.5, 6.7, "Presentation Layer", ha='center', va='center', fontsize=12, fontweight='bold', color=c_pres_b)
    pres_views = [
        ("Ticker Timeline", "Price vs Sentiment"),
        ("Anomaly Radar", "Flagged Spikes & News"),
        ("Sector Heatmap", "Momentum Analysis")
    ]
    for (t, d), yp in zip(pres_views, y_pos):
        inner = FancyBboxPatch((12.65, yp - 0.45), 1.7, 0.9, boxstyle="round,pad=0.05,rounding_size=0.08",
                               facecolor='white', edgecolor='#FDA4AF', linewidth=1)
        ax.add_patch(inner)
        ax.text(13.5, yp + 0.12, t, ha='center', va='center', fontsize=8.8, fontweight='bold', color='#9F1239')
        ax.text(13.5, yp - 0.22, d, ha='center', va='center', fontsize=7.5, color='#475569')

    # Connectors between layers
    arrow_props = dict(arrowstyle="-|>", mutation_scale=16, color='#475569', lw=1.8)
    ax.annotate("", xy=(3.1, 4.6), xytext=(2.7, 4.6), arrowprops=arrow_props)
    ax.annotate("", xy=(5.6, 4.6), xytext=(5.2, 4.6), arrowprops=arrow_props)
    ax.annotate("", xy=(8.9, 4.6), xytext=(8.5, 4.6), arrowprops=arrow_props)
    ax.annotate("", xy=(12.5, 4.6), xytext=(12.1, 4.6), arrowprops=arrow_props)

    # Base Band: Infrastructure & Cluster Resource Management
    base_band = FancyBboxPatch((0.5, 0.6), 14.0, 1.2, boxstyle="round,pad=0.08,rounding_size=0.15",
                               facecolor=c_base, edgecolor='#334155', linewidth=1.5)
    ax.add_patch(base_band)
    ax.text(7.5, 1.45, "CLUSTER INFRASTRUCTURE & DISTRIBUTED RESOURCE MANAGEMENT", 
            ha='center', va='center', fontsize=11.5, fontweight='bold', color='#38BDF8')
    ax.text(7.5, 1.0, "Docker Multi-Container Orchestration (Docker Compose)   |   Apache YARN ResourceManager & NodeManagers\nMaster Node + 3 Slave Nodes (HDFS Block Replication = 3, Spark Standalone / YARN Execution)",
            ha='center', va='center', fontsize=9, color='#E2E8F0')

    # Vertical connectors down to base band
    for x_c in [1.6, 4.15, 7.05, 10.5, 13.5]:
        ax.annotate("", xy=(x_c, 1.8), xytext=(x_c, 2.2),
                    arrowprops=dict(arrowstyle="<|-|>", mutation_scale=10, color='#64748B', lw=1.2, linestyle=':'))

    plt.tight_layout()
    for d in output_dirs:
        fig.savefig(os.path.join(d, "fig1_system_architecture.png"), dpi=300, bbox_inches='tight')
    plt.close(fig)

# -----------------------------------------------------------------------------
# FIG 2: Four-Node Distributed Cluster
# -----------------------------------------------------------------------------
def generate_fig2(output_dirs):
    fig, ax = plt.subplots(figsize=(15, 9.0), dpi=300)
    ax.set_xlim(0, 15)
    ax.set_ylim(0, 9.0)
    ax.axis('off')

    # Title
    ax.text(7.5, 8.5, "Figure 2: Four-Node Distributed Cluster Topology", 
            ha='center', va='center', fontsize=18, fontweight='bold', color='#1E293B')
    ax.text(7.5, 8.1, "Single Master Node Coordination with Three Identical Worker/Slave Nodes",
            ha='center', va='center', fontsize=11, fontstyle='italic', color='#64748B')

    # Master Box (Left)
    master_box = FancyBboxPatch((0.8, 1.2), 5.5, 6.4, boxstyle="round,pad=0.1,rounding_size=0.2",
                                facecolor='#F8FAFC', edgecolor='#1E3A8A', linewidth=2.5)
    ax.add_patch(master_box)

    header_m = FancyBboxPatch((0.8, 7.0), 5.5, 0.6, boxstyle="round,pad=0.05,rounding_size=0.1",
                              facecolor='#1E3A8A', edgecolor='#1E3A8A')
    ax.add_patch(header_m)
    ax.text(3.55, 7.3, "MASTER NODE (Controller)", ha='center', va='center', 
            fontsize=13, fontweight='bold', color='white')

    master_services = [
        ("HDFS NameNode", "Metadata & Namespace Manager", "Port 9870", "#DBEAFE", "#1D4ED8"),
        ("YARN ResourceManager", "Global Scheduling & Resource Alloc", "Port 8088", "#DCFCE7", "#15803D"),
        ("Spark Master", "Cluster Driver & DAG Coordinator", "Port 8080", "#F3E8FF", "#7E22CE"),
        ("MapReduce JobHistory Server", "Historical Execution Logs", "Port 19888", "#FEF3C7", "#B45309"),
        ("Hive Metastore", "Schema Catalog & Relational Metadata", "Port 9083", "#FFEDD5", "#C2410C"),
        ("Streamlit Dashboard", "Interactive User Analytics Interface", "Port 8501", "#FFE4E6", "#BE123C")
    ]

    y_start = 6.4
    for name, desc, port, bg, border in master_services:
        s_box = FancyBboxPatch((1.1, y_start - 0.7), 4.9, 0.75, boxstyle="round,pad=0.05,rounding_size=0.08",
                               facecolor=bg, edgecolor=border, linewidth=1.2)
        ax.add_patch(s_box)
        ax.text(1.3, y_start - 0.22, name, va='center', fontsize=9.5, fontweight='bold', color='#0F172A')
        ax.text(1.3, y_start - 0.52, desc, va='center', fontsize=8, color='#475569')
        
        # Port pill badge
        port_pill = FancyBboxPatch((4.7, y_start - 0.55), 1.2, 0.45, boxstyle="round,pad=0.05,rounding_size=0.08",
                                   facecolor='white', edgecolor=border, linewidth=1.5)
        ax.add_patch(port_pill)
        ax.text(5.3, y_start - 0.32, port, ha='center', va='center', fontsize=8.5, fontweight='bold', color=border)
        
        y_start -= 0.95

    # Three Slave Boxes (Right)
    slave_configs = [
        ("SLAVE NODE 1 (Worker)", 5.6),
        ("SLAVE NODE 2 (Worker)", 3.4),
        ("SLAVE NODE 3 (Worker)", 1.2)
    ]

    for title, y_bottom in slave_configs:
        sl_box = FancyBboxPatch((8.8, y_bottom), 5.4, 1.9, boxstyle="round,pad=0.1,rounding_size=0.15",
                                facecolor='#F8FAFC', edgecolor='#0369A1', linewidth=2)
        ax.add_patch(sl_box)
        
        sl_hdr = FancyBboxPatch((8.8, y_bottom + 1.45), 5.4, 0.45, boxstyle="round,pad=0.05,rounding_size=0.1",
                                facecolor='#0369A1', edgecolor='#0369A1')
        ax.add_patch(sl_hdr)
        ax.text(11.5, y_bottom + 1.68, title, ha='center', va='center', fontsize=11, fontweight='bold', color='white')
        
        # Services in slave
        servs = [
            ("HDFS DataNode", "Block Replica Storage (Factor = 3)", "Port 9864"),
            ("YARN NodeManager", "Container Monitoring & Execution", "Port 8042"),
            ("Spark Worker", "Local In-Memory RDD/DataFrame Executor", "Executors")
        ]
        xs = [9.0, 10.75, 12.5]
        for (sn, sd, sp), xp in zip(servs, xs):
            sub = FancyBboxPatch((xp, y_bottom + 0.15), 1.6, 1.15, boxstyle="round,pad=0.05,rounding_size=0.08",
                                 facecolor='white', edgecolor='#BAE6FD', linewidth=1)
            ax.add_patch(sub)
            ax.text(xp + 0.8, y_bottom + 1.0, sn, ha='center', va='center', fontsize=8, fontweight='bold', color='#0369A1')
            ax.text(xp + 0.8, y_bottom + 0.65, sd, ha='center', va='center', fontsize=6.8, color='#475569')
            ax.text(xp + 0.8, y_bottom + 0.32, sp, ha='center', va='center', fontsize=7.5, fontweight='bold', color='#0284C7')

    # Central Network Bus & Communication Channels
    ax.plot([6.3, 7.5, 7.5, 8.8], [4.4, 4.4, 6.55, 8.8], color='#0284C7', lw=0) # dummy
    
    # Bus trunk
    bus_trunk = Rectangle((7.35, 2.15), 0.3, 4.4, facecolor='#E2E8F0', edgecolor='#94A3B8', linewidth=1)
    ax.add_patch(bus_trunk)
    ax.text(7.5, 4.35, "Docker Bridge Network (Cluster Interconnect)", rotation=90, 
            ha='center', va='center', fontsize=9, fontweight='bold', color='#475569')

    # Arrow from Master to Bus
    ax.annotate("", xy=(7.35, 4.35), xytext=(6.3, 4.35),
                arrowprops=dict(arrowstyle="<|-|>", mutation_scale=16, color='#0284C7', lw=2))

    # Arrows from Bus to Slaves
    for _, yb in slave_configs:
        target_y = yb + 0.95
        ax.annotate("", xy=(8.8, target_y), xytext=(7.65, target_y),
                    arrowprops=dict(arrowstyle="<|-|>", mutation_scale=16, color='#0284C7', lw=2))

    # Notes at bottom
    ax.text(7.5, 0.45, "Replication Policy: HDFS block replication factor = 3 across all three DataNodes. Failover resilient.",
            ha='center', va='center', fontsize=9.5, color='#334155', fontweight='bold')

    plt.tight_layout()
    for d in output_dirs:
        fig.savefig(os.path.join(d, "fig2_cluster_topology.png"), dpi=300, bbox_inches='tight')
    plt.close(fig)

# -----------------------------------------------------------------------------
# FIG 3: End-to-End Ingestion and Processing Pipeline
# -----------------------------------------------------------------------------
def generate_fig3(output_dirs):
    fig, ax = plt.subplots(figsize=(16, 7.5), dpi=300)
    ax.set_xlim(0, 16)
    ax.set_ylim(0, 7.5)
    ax.axis('off')

    # Title
    ax.text(8.0, 7.0, "Figure 3: End-to-End Ingestion and Processing Pipeline", 
            ha='center', va='center', fontsize=18, fontweight='bold', color='#1E293B')
    ax.text(8.0, 6.6, "Data Flow From Raw Collection Scripts to Analytical Dashboard",
            ha='center', va='center', fontsize=11, fontstyle='italic', color='#64748B')

    # Stage 1: Ingestion Scripts (Stacked vertically)
    s1_box = FancyBboxPatch((0.5, 1.2), 2.3, 4.8, boxstyle="round,pad=0.1,rounding_size=0.15",
                            facecolor='#F0FDF4', edgecolor='#16A34A', linewidth=1.8)
    ax.add_patch(s1_box)
    ax.text(1.65, 5.7, "Data Acquisition\n(src/ingestion/)", ha='center', va='center', fontsize=11, fontweight='bold', color='#15803D')
    
    scripts = [
        ("fetch_price.py", "Yahoo Finance\nOHLCV 30+ Tickers", 4.7),
        ("fetch_news.py", "Moneycontrol, ET,\nLiveMint, NDTV", 3.4),
        ("fetch_reddit.py", "r/IndianStreetBets,\nr/IndiaInvestments", 2.1)
    ]
    for name, desc, yp in scripts:
        inner = FancyBboxPatch((0.7, yp - 0.45), 1.9, 0.9, boxstyle="round,pad=0.05,rounding_size=0.08",
                               facecolor='white', edgecolor='#86EFAC', linewidth=1)
        ax.add_patch(inner)
        ax.text(1.65, yp + 0.15, name, ha='center', va='center', fontsize=9, fontweight='bold', color='#14532D')
        ax.text(1.65, yp - 0.2, desc, ha='center', va='center', fontsize=7.5, color='#475569')

    # Stage 2: HDFS Raw Zone
    s2_box = FancyBboxPatch((3.7, 2.0), 2.0, 3.2, boxstyle="round,pad=0.1,rounding_size=0.15",
                            facecolor='#EFF6FF', edgecolor='#2563EB', linewidth=1.8)
    ax.add_patch(s2_box)
    ax.text(4.7, 4.8, "HDFS Raw Zone", ha='center', va='center', fontsize=11, fontweight='bold', color='#1D4ED8')
    ax.text(4.7, 4.4, "(Replication = 3)", ha='center', va='center', fontsize=8.5, color='#3B82F6')
    
    paths = [
        ("/data/raw/prices/", "Daily OHLCV CSVs", 3.8),
        ("/data/raw/news/", "Raw News JSON", 3.0),
        ("/data/raw/reddit/", "Subreddit Posts", 2.2)
    ]
    for p, sub, yp in paths:
        sub_b = FancyBboxPatch((3.85, yp - 0.28), 1.7, 0.56, boxstyle="round,pad=0.05,rounding_size=0.06",
                               facecolor='white', edgecolor='#BFDBFE', linewidth=1)
        ax.add_patch(sub_b)
        ax.text(4.7, yp + 0.08, p, ha='center', va='center', fontsize=7.8, fontweight='bold', color='#1E40AF')
        ax.text(4.7, yp - 0.14, sub, ha='center', va='center', fontsize=6.8, color='#64748B')

    # Stage 3: Apache Pig ETL
    s3_box = FancyBboxPatch((6.6, 2.2), 2.1, 2.8, boxstyle="round,pad=0.1,rounding_size=0.15",
                            facecolor='#FFFBEB', edgecolor='#D97706', linewidth=1.8)
    ax.add_patch(s3_box)
    ax.text(7.65, 4.6, "Apache Pig ETL", ha='center', va='center', fontsize=11, fontweight='bold', color='#B45309')
    ax.text(7.65, 4.25, "etl_normalize_join.pig", ha='center', va='center', fontsize=8, color='#D97706')
    ax.text(7.65, 3.4, "* Timestamp Alignment\n* Schema Normalisation\n* Record Deduplication\n* Outer Equi-Joins",
            ha='center', va='center', fontsize=8, color='#78350F')

    # Stage 4: Hive & HBase
    s4_box = FancyBboxPatch((9.6, 2.0), 2.1, 3.2, boxstyle="round,pad=0.1,rounding_size=0.15",
                            facecolor='#FAF5FF', edgecolor='#9333EA', linewidth=1.8)
    ax.add_patch(s4_box)
    ax.text(10.65, 4.8, "Storage & Query", ha='center', va='center', fontsize=11, fontweight='bold', color='#7E22CE')
    
    sq_items = [
        ("Apache Hive", "clean_prices\nnews_feed\nsocial_sentiment", 3.8),
        ("Apache HBase", "Low-latency\nRowKey Lookup\nTicker Profiles", 2.6)
    ]
    for nm, desc, yp in sq_items:
        sub_b = FancyBboxPatch((9.75, yp - 0.42), 1.8, 0.85, boxstyle="round,pad=0.05,rounding_size=0.06",
                               facecolor='white', edgecolor='#E9D5FF', linewidth=1)
        ax.add_patch(sub_b)
        ax.text(10.65, yp + 0.12, nm, ha='center', va='center', fontsize=8.5, fontweight='bold', color='#6B21A8')
        ax.text(10.65, yp - 0.2, desc, ha='center', va='center', fontsize=7.2, color='#475569')

    # Stage 5: Spark / PySpark Analytics
    s5_box = FancyBboxPatch((12.6, 1.8), 2.8, 3.6, boxstyle="round,pad=0.1,rounding_size=0.15",
                            facecolor='#FFF1F2', edgecolor='#E11D48', linewidth=1.8)
    ax.add_patch(s5_box)
    ax.text(14.0, 5.0, "Spark / PySpark ML", ha='center', va='center', fontsize=11, fontweight='bold', color='#BE123C')
    
    an_items = [
        ("Sentiment Scoring", "Tokenizer, TF-IDF, TextBlob", 4.3),
        ("Isolation Forest", "Anomaly Scoring on Returns & Vol", 3.4),
        ("Sector Momentum", "MapReduce 30/60/90d Windows", 2.5)
    ]
    for nm, desc, yp in an_items:
        sub_b = FancyBboxPatch((12.75, yp - 0.32), 2.5, 0.65, boxstyle="round,pad=0.05,rounding_size=0.06",
                               facecolor='white', edgecolor='#FECDD3', linewidth=1)
        ax.add_patch(sub_b)
        ax.text(14.0, yp + 0.08, nm, ha='center', va='center', fontsize=8.2, fontweight='bold', color='#9F1239')
        ax.text(14.0, yp - 0.16, desc, ha='center', va='center', fontsize=7.2, color='#475569')

    # Arrows labeled with data forms
    # 1 -> 2
    ax.annotate("", xy=(3.7, 4.4), xytext=(2.8, 4.7), arrowprops=dict(arrowstyle="-|>", color='#059669', lw=1.5))
    ax.text(3.25, 4.8, "OHLCV files", ha='center', va='center', fontsize=7.5, fontweight='bold', color='#059669', rotation=15)

    ax.annotate("", xy=(3.7, 3.6), xytext=(2.8, 3.4), arrowprops=dict(arrowstyle="-|>", color='#059669', lw=1.5))
    ax.text(3.25, 3.75, "Headline text", ha='center', va='center', fontsize=7.5, fontweight='bold', color='#059669')

    ax.annotate("", xy=(3.7, 2.8), xytext=(2.8, 2.1), arrowprops=dict(arrowstyle="-|>", color='#059669', lw=1.5))
    ax.text(3.25, 2.65, "Post text", ha='center', va='center', fontsize=7.5, fontweight='bold', color='#059669', rotation=-15)

    # 2 -> 3
    ax.annotate("", xy=(6.6, 3.6), xytext=(5.7, 3.6), arrowprops=dict(arrowstyle="-|>", color='#2563EB', lw=1.8))
    ax.text(6.15, 3.9, "Raw records", ha='center', va='center', fontsize=8, fontweight='bold', color='#1D4ED8')

    # 3 -> 4
    ax.annotate("", xy=(9.6, 3.6), xytext=(8.7, 3.6), arrowprops=dict(arrowstyle="-|>", color='#D97706', lw=1.8))
    ax.text(9.15, 3.9, "Cleaned records", ha='center', va='center', fontsize=8, fontweight='bold', color='#B45309')

    # 4 -> 5
    ax.annotate("", xy=(12.6, 3.6), xytext=(11.7, 3.6), arrowprops=dict(arrowstyle="-|>", color='#9333EA', lw=1.8))
    ax.text(12.15, 3.9, "Tables / Views", ha='center', va='center', fontsize=8, fontweight='bold', color='#7E22CE')

    # Stage 6: Streamlit Banner Output at bottom/right
    st_box = FancyBboxPatch((5.0, 0.4), 6.0, 0.9, boxstyle="round,pad=0.08,rounding_size=0.1",
                            facecolor='#0F172A', edgecolor='#38BDF8', linewidth=1.5)
    ax.add_patch(st_box)
    ax.text(8.0, 0.85, "Streamlit Dashboard (Port 8501)", ha='center', va='center', fontsize=11, fontweight='bold', color='white')
    ax.text(8.0, 0.58, "Visualizes Anomaly Radar, Ticker Timelines, and Sector Momentum Heatmaps", 
            ha='center', va='center', fontsize=8, color='#94A3B8')

    # Connector from Spark down to Streamlit
    ax.annotate("", xy=(11.0, 0.85), xytext=(14.0, 1.8),
                arrowprops=dict(arrowstyle="-|>", connectionstyle="angle,angleA=-90,angleB=0,rad=10",
                                color='#E11D48', lw=1.8))
    ax.text(13.2, 1.0, "Scored records & Anomalies", ha='center', va='center', fontsize=8, fontweight='bold', color='#BE123C')

    plt.tight_layout()
    for d in output_dirs:
        fig.savefig(os.path.join(d, "fig3_ingestion_pipeline.png"), dpi=300, bbox_inches='tight')
    plt.close(fig)

# -----------------------------------------------------------------------------
# FIG 4: Sentiment Analysis Pipeline
# -----------------------------------------------------------------------------
def generate_fig4(output_dirs):
    fig, ax = plt.subplots(figsize=(15, 7.5), dpi=300)
    ax.set_xlim(0, 15)
    ax.set_ylim(0, 7.5)
    ax.axis('off')

    # Title
    ax.text(7.5, 7.0, "Figure 4: Sentiment-Analysis Pipeline", 
            ha='center', va='center', fontsize=18, fontweight='bold', color='#1E293B')
    ax.text(7.5, 6.6, "Dual-Branch Architecture: Spark MLlib TF-IDF Features and TextBlob Lexicon Polarity",
            ha='center', va='center', fontsize=11, fontstyle='italic', color='#64748B')

    # Step 1: Input Text
    b_input = FancyBboxPatch((0.5, 3.2), 2.2, 1.6, boxstyle="round,pad=0.1,rounding_size=0.12",
                             facecolor='#F1F5F9', edgecolor='#475569', linewidth=2)
    ax.add_patch(b_input)
    ax.text(1.6, 4.3, "Raw Unstructured Text", ha='center', va='center', fontsize=10.5, fontweight='bold', color='#0F172A')
    ax.text(1.6, 3.8, "* Financial News Headlines\n* Reddit Subreddit Posts\n(Moneycontrol, ET, ISB)", 
            ha='center', va='center', fontsize=8, color='#475569')

    # Step 2: Shared Spark MLlib Preprocessing
    b_prep = FancyBboxPatch((3.3, 3.2), 2.5, 1.6, boxstyle="round,pad=0.1,rounding_size=0.12",
                            facecolor='#EFF6FF', edgecolor='#2563EB', linewidth=2)
    ax.add_patch(b_prep)
    ax.text(4.55, 4.4, "Spark MLlib Preprocessing", ha='center', va='center', fontsize=10.5, fontweight='bold', color='#1D4ED8')
    ax.text(4.55, 3.9, "1. Tokenizer (Regex)\n2. StopWordsRemover\n(Noise Removal & Cleaning)",
            ha='center', va='center', fontsize=8, color='#1E40AF')

    # Arrow 1 -> 2
    ax.annotate("", xy=(3.3, 4.0), xytext=(2.7, 4.0),
                arrowprops=dict(arrowstyle="-|>", mutation_scale=14, color='#475569', lw=1.8))

    # --- TOP BRANCH: TF-IDF Extraction (Blue) ---
    b_tfidf = FancyBboxPatch((6.6, 4.6), 3.2, 1.6, boxstyle="round,pad=0.1,rounding_size=0.12",
                             facecolor='#EFF6FF', edgecolor='#3B82F6', linewidth=2)
    ax.add_patch(b_tfidf)
    ax.text(8.2, 5.75, "Branch A: Feature Representation", ha='center', va='center', fontsize=10, fontweight='bold', color='#1D4ED8')
    ax.text(8.2, 5.25, "Spark MLlib HashingTF + IDF", ha='center', va='center', fontsize=9, fontweight='bold', color='#2563EB')
    ax.text(8.2, 4.8, "TF-IDF(t, d) = TF(t, d) * IDF(t)\nProduces Sparse Feature Vectors", 
            ha='center', va='center', fontsize=8, color='#475569')

    # --- BOTTOM BRANCH: TextBlob Polarity & Subjectivity (Teal) ---
    b_tb = FancyBboxPatch((6.6, 1.8), 3.2, 1.6, boxstyle="round,pad=0.1,rounding_size=0.12",
                          facecolor='#F0FDF4', edgecolor='#10B981', linewidth=2)
    ax.add_patch(b_tb)
    ax.text(8.2, 2.95, "Branch B: Sentiment Scoring", ha='center', va='center', fontsize=10, fontweight='bold', color='#047857')
    ax.text(8.2, 2.45, "TextBlob NLP Polarity Engine", ha='center', va='center', fontsize=9, fontweight='bold', color='#059669')
    ax.text(8.2, 2.0, "Polarity p(x) in [-1.0, +1.0]\nSubjectivity in [0.0, 1.0]", 
            ha='center', va='center', fontsize=8, color='#475569')

    # Branching arrows from Preprocessing
    ax.annotate("", xy=(6.6, 5.4), xytext=(5.8, 4.4),
                arrowprops=dict(arrowstyle="-|>", mutation_scale=14, color='#3B82F6', lw=2))
    ax.text(6.0, 5.15, "Tokens", ha='center', va='center', fontsize=8.5, fontweight='bold', color='#2563EB')

    ax.annotate("", xy=(6.6, 2.6), xytext=(5.8, 3.6),
                arrowprops=dict(arrowstyle="-|>", mutation_scale=14, color='#10B981', lw=2))
    ax.text(6.0, 2.95, "Clean Text", ha='center', va='center', fontsize=8.5, fontweight='bold', color='#059669')

    # Step 3: Convergence into Ticker-Level Aggregation
    b_merge = FancyBboxPatch((10.7, 2.7), 3.8, 2.6, boxstyle="round,pad=0.1,rounding_size=0.15",
                             facecolor='#FAF5FF', edgecolor='#8B5CF6', linewidth=2.2)
    ax.add_patch(b_merge)
    ax.text(12.6, 4.9, "Ticker-Level Daily Aggregation", ha='center', va='center', fontsize=11, fontweight='bold', color='#6D28D9')
    ax.text(12.6, 4.3, "S(k, d) = (1 / |D(k, d)|) * sum( p(x) )", ha='center', va='center', fontsize=9.5, fontweight='bold', color='#4C1D95')
    ax.text(12.6, 3.6, "Produces Normalized Score in [-1.0, +1.0]\n-1.0 = Strong Bearish | +1.0 = Strong Bullish\nMaintains Separate Indices for News & Reddit", 
            ha='center', va='center', fontsize=8.2, color='#334155')
    ax.text(12.6, 3.0, "Exported to Hive Views & HBase Tables", ha='center', va='center', fontsize=8, fontstyle='italic', color='#7C3AED')

    # Converging arrows
    ax.annotate("", xy=(10.7, 4.5), xytext=(9.8, 5.4),
                arrowprops=dict(arrowstyle="-|>", mutation_scale=14, color='#3B82F6', lw=2))
    ax.annotate("", xy=(10.7, 3.5), xytext=(9.8, 2.6),
                arrowprops=dict(arrowstyle="-|>", mutation_scale=14, color='#10B981', lw=2))

    plt.tight_layout()
    for d in output_dirs:
        fig.savefig(os.path.join(d, "fig4_sentiment_pipeline.png"), dpi=300, bbox_inches='tight')
    plt.close(fig)

# -----------------------------------------------------------------------------
# FIG 5: Isolation Forest Anomaly Detection Process
# -----------------------------------------------------------------------------
def generate_fig5(output_dirs):
    fig, ax = plt.subplots(figsize=(15, 8.0), dpi=300)
    ax.set_xlim(0, 15)
    ax.set_ylim(0, 8.0)
    ax.axis('off')

    # Title
    ax.text(7.5, 7.5, "Figure 5: Isolation Forest Anomaly Detection Process", 
            ha='center', va='center', fontsize=18, fontweight='bold', color='#1E293B')
    ax.text(7.5, 7.1, "Unsupervised Multi-Feature Outlier Detection on Cleaned Equity Records",
            ha='center', va='center', fontsize=11, fontstyle='italic', color='#64748B')

    # Box 1: Input Clean Prices
    b1 = FancyBboxPatch((0.5, 2.8), 2.1, 3.4, boxstyle="round,pad=0.1,rounding_size=0.12",
                        facecolor='#F8FAFC', edgecolor='#64748B', linewidth=2)
    ax.add_patch(b1)
    ax.text(1.55, 5.7, "Input Data", ha='center', va='center', fontsize=11, fontweight='bold', color='#0F172A')
    ax.text(1.55, 5.3, "Hive Table: clean_prices", ha='center', va='center', fontsize=8.5, color='#475569')
    fields = ["Open", "High", "Low", "Close", "Volume", "Timestamp"]
    y_f = 4.6
    for f in fields:
        f_pill = FancyBboxPatch((0.75, y_f - 0.16), 1.6, 0.32, boxstyle="round,pad=0.03,rounding_size=0.05",
                                facecolor='white', edgecolor='#CBD5E1', linewidth=1)
        ax.add_patch(f_pill)
        ax.text(1.55, y_f, f, ha='center', va='center', fontsize=7.8, color='#334155')
        y_f -= 0.38

    # Box 2: Feature Engineering (4 Key Features)
    b2 = FancyBboxPatch((3.2, 1.8), 3.3, 4.4, boxstyle="round,pad=0.1,rounding_size=0.15",
                        facecolor='#EFF6FF', edgecolor='#2563EB', linewidth=2)
    ax.add_patch(b2)
    ax.text(4.85, 5.8, "Feature Calculation", ha='center', va='center', fontsize=11, fontweight='bold', color='#1D4ED8')
    ax.text(4.85, 5.4, "src/ml/anomaly_detector.py", ha='center', va='center', fontsize=8, color='#3B82F6')
    
    feats = [
        ("Daily Return R(t)", "R(t) = [P(t) - P(t-1)] / P(t-1)", 4.8),
        ("Normalized Volume NV(t)", "Z-score / Rolling scaled volume", 3.9),
        ("High-Low Spread S(t)", "S(t) = [High(t) - Low(t)] / Close(t)", 3.0),
        ("20-Day Volatility sigma(t)", "Sample std dev of returns over 20d", 2.1)
    ]
    for fn, fd, yp in feats:
        inner = FancyBboxPatch((3.4, yp - 0.35), 2.9, 0.75, boxstyle="round,pad=0.05,rounding_size=0.08",
                               facecolor='white', edgecolor='#93C5FD', linewidth=1)
        ax.add_patch(inner)
        ax.text(4.85, yp + 0.1, fn, ha='center', va='center', fontsize=8.5, fontweight='bold', color='#1E40AF')
        ax.text(4.85, yp - 0.16, fd, ha='center', va='center', fontsize=7.2, color='#475569')

    # Box 3: Isolation Forest Algorithm
    b3 = FancyBboxPatch((7.1, 2.0), 3.6, 4.2, boxstyle="round,pad=0.1,rounding_size=0.15",
                        facecolor='#FEF3C7', edgecolor='#D97706', linewidth=2)
    ax.add_patch(b3)
    ax.text(8.9, 5.8, "Isolation Forest Model", ha='center', va='center', fontsize=11, fontweight='bold', color='#B45309')
    ax.text(8.9, 5.4, "Scikit-Learn Implementation", ha='center', va='center', fontsize=8, color='#D97706')
    ax.text(8.9, 4.6, "* Ensemble of Isolation Trees (iTrees)\n* Recursive random feature partitioning\n* Outliers isolate in fewer splits (shorter path)",
            ha='center', va='center', fontsize=8.2, color='#78350F')
    
    formula_box = FancyBboxPatch((7.35, 2.3), 3.1, 1.4, boxstyle="round,pad=0.05,rounding_size=0.08",
                                 facecolor='white', edgecolor='#FCD34D', linewidth=1)
    ax.add_patch(formula_box)
    ax.text(8.9, 3.3, "Anomaly Score Formulation:", ha='center', va='center', fontsize=8.5, fontweight='bold', color='#92400E')
    ax.text(8.9, 2.85, "s(x, n) = 2^(-E[h(x)] / c(n))", ha='center', va='center', fontsize=9, fontweight='bold', color='#B45309')
    ax.text(8.9, 2.45, "c(n) = 2ln(n-1) + 0.5772 - 2(n-1)/n", ha='center', va='center', fontsize=7.2, color='#475569')

    # Box 4: Output Labels & Fusion Export
    b4 = FancyBboxPatch((11.3, 2.4), 3.2, 3.8, boxstyle="round,pad=0.1,rounding_size=0.15",
                        facecolor='#F0FDF4', edgecolor='#16A34A', linewidth=2)
    ax.add_patch(b4)
    ax.text(12.9, 5.8, "Outlier Classification", ha='center', va='center', fontsize=11, fontweight='bold', color='#15803D')
    
    # Label Pills
    p_norm = FancyBboxPatch((11.5, 4.6), 2.8, 0.7, boxstyle="round,pad=0.05,rounding_size=0.08",
                            facecolor='#DCFCE7', edgecolor='#22C55E', linewidth=1)
    ax.add_patch(p_norm)
    ax.text(12.9, 5.0, "Label: +1 (Normal Day)", ha='center', va='center', fontsize=9, fontweight='bold', color='#15803D')
    ax.text(12.9, 4.75, "High path length E[h(x)]", ha='center', va='center', fontsize=7.2, color='#166534')

    p_anom = FancyBboxPatch((11.5, 3.6), 2.8, 0.8, boxstyle="round,pad=0.05,rounding_size=0.08",
                            facecolor='#FEE2E2', edgecolor='#EF4444', linewidth=1.5)
    ax.add_patch(p_anom)
    ax.text(12.9, 4.1, "Label: -1 (Price / Vol Anomaly)", ha='center', va='center', fontsize=9, fontweight='bold', color='#B91C1C')
    ax.text(12.9, 3.8, "Short path length E[h(x)]", ha='center', va='center', fontsize=7.2, color='#991B1B')

    ax.text(12.9, 2.9, "Table of Anomalous Days\nForwarded to Fusion Stage (Fig. 6)",
            ha='center', va='center', fontsize=8.5, fontweight='bold', color='#047857')

    # Connectors
    ax.annotate("", xy=(3.2, 4.5), xytext=(2.6, 4.5), arrowprops=dict(arrowstyle="-|>", mutation_scale=14, color='#475569', lw=1.8))
    ax.annotate("", xy=(7.1, 4.5), xytext=(6.5, 4.5), arrowprops=dict(arrowstyle="-|>", mutation_scale=14, color='#2563EB', lw=1.8))
    ax.annotate("", xy=(11.3, 4.5), xytext=(10.7, 4.5), arrowprops=dict(arrowstyle="-|>", mutation_scale=14, color='#D97706', lw=1.8))

    plt.tight_layout()
    for d in output_dirs:
        fig.savefig(os.path.join(d, "fig5_anomaly_detection.png"), dpi=300, bbox_inches='tight')
    plt.close(fig)

# -----------------------------------------------------------------------------
# FIG 6: Sentiment-Anomaly Data Fusion Workflow
# -----------------------------------------------------------------------------
def generate_fig6(output_dirs):
    fig, ax = plt.subplots(figsize=(15, 8.5), dpi=300)
    ax.set_xlim(0, 15)
    ax.set_ylim(0, 8.5)
    ax.axis('off')

    # Title
    ax.text(7.5, 8.0, "Figure 6: Sentiment-Anomaly Data-Fusion Workflow", 
            ha='center', va='center', fontsize=18, fontweight='bold', color='#1E293B')
    ax.text(7.5, 7.6, "Temporal Alignment & Multi-Class Contextual Categorization",
            ha='center', va='center', fontsize=11, fontstyle='italic', color='#64748B')

    # Two Inputs on Left
    in1 = FancyBboxPatch((0.5, 5.0), 2.8, 1.8, boxstyle="round,pad=0.1,rounding_size=0.12",
                         facecolor='#FEE2E2', edgecolor='#DC2626', linewidth=2)
    ax.add_patch(in1)
    ax.text(1.9, 6.4, "Input A: Anomalies Table", ha='center', va='center', fontsize=10.5, fontweight='bold', color='#B91C1C')
    ax.text(1.9, 5.7, "Flagged days from Isolation Forest\nLabel = -1 (Price / Volume spikes)\nAttributes: Ticker k, Date t(a)",
            ha='center', va='center', fontsize=8, color='#7F1D1D')

    in2 = FancyBboxPatch((0.5, 1.8), 2.8, 2.2, boxstyle="round,pad=0.1,rounding_size=0.12",
                         facecolor='#EFF6FF', edgecolor='#2563EB', linewidth=2)
    ax.add_patch(in2)
    ax.text(1.9, 3.5, "Input B: Daily Sentiment", ha='center', va='center', fontsize=10.5, fontweight='bold', color='#1D4ED8')
    ax.text(1.9, 2.7, "Ticker-level polarity S(k, d)\n1. Financial News Index\n2. Reddit Retail Index\nRange: [-1.0, +1.0]",
            ha='center', va='center', fontsize=8, color='#1E3A8A')

    # Stage 2: Temporal Alignment (24-Hour Window)
    s2 = FancyBboxPatch((4.0, 3.0), 2.9, 3.0, boxstyle="round,pad=0.1,rounding_size=0.15",
                        facecolor='#FEF3C7', edgecolor='#D97706', linewidth=2)
    ax.add_patch(s2)
    ax.text(5.45, 5.5, "Temporal Alignment", ha='center', va='center', fontsize=11, fontweight='bold', color='#B45309')
    ax.text(5.45, 5.0, "24-Hour Window W(a)", ha='center', va='center', fontsize=9.5, fontweight='bold', color='#92400E')
    ax.text(5.45, 4.2, "Correlates timestamp of\nanomaly t(a) with publication\ntime of news / posts:\n[t(a) - 24h, t(a) + 24h]",
            ha='center', va='center', fontsize=8, color='#78350F')
    ax.text(5.45, 3.3, "Addresses market hours\nvs overnight news lag", ha='center', va='center', fontsize=7.2, fontstyle='italic', color='#B45309')

    # Arrows into Temporal Alignment
    ax.annotate("", xy=(4.0, 5.2), xytext=(3.3, 5.9),
                arrowprops=dict(arrowstyle="-|>", mutation_scale=14, color='#DC2626', lw=1.8))
    ax.annotate("", xy=(4.0, 3.8), xytext=(3.3, 2.9),
                arrowprops=dict(arrowstyle="-|>", mutation_scale=14, color='#2563EB', lw=1.8))

    # Stage 3: Decision Diamond / Threshold Test
    diamond = patches.RegularPolygon((8.1, 4.5), numVertices=4, radius=1.05,
                                     facecolor='#FAF5FF', edgecolor='#9333EA', linewidth=2)
    ax.add_patch(diamond)
    ax.text(8.1, 4.7, "Threshold Test", ha='center', va='center', fontsize=9, fontweight='bold', color='#7E22CE')
    ax.text(8.1, 4.3, "|Polarity| > 0.45 ?", ha='center', va='center', fontsize=8.5, fontweight='bold', color='#6B21A8')

    ax.annotate("", xy=(7.05, 4.5), xytext=(6.9, 4.5),
                arrowprops=dict(arrowstyle="-|>", mutation_scale=14, color='#D97706', lw=1.8))

    # Stage 4: Four-Way Classification
    c_y = [6.3, 5.0, 3.7, 2.4]
    classes = [
        ("News-Associated Anomaly", "High polarity shift (|p| > 0.45) in news within 24h\n(Observed in ~68% of large-cap anomalies)", "#EFF6FF", "#3B82F6", "#1E40AF"),
        ("Retail-Associated Anomaly", "High polarity shift (|p| > 0.45) in Reddit chatter\nRetail speculative momentum divergence", "#F0FDF4", "#10B981", "#065F46"),
        ("Structural / Liquidity Anomaly", "Extreme volume / price move without sentiment spike\nBlock trades, rebalancing, index adjustments", "#FEF3C7", "#F59E0B", "#92400E"),
        ("Unexplained Outlier", "No decisive news, retail chatter, or volume explanation\nRequires independent analyst verification", "#F1F5F9", "#64748B", "#334155")
    ]

    for (title, desc, bg, border, tc), yp in zip(classes, c_y):
        box_c = FancyBboxPatch((10.0, yp - 0.5), 4.5, 1.0, boxstyle="round,pad=0.06,rounding_size=0.1",
                               facecolor=bg, edgecolor=border, linewidth=1.5)
        ax.add_patch(box_c)
        ax.text(10.2, yp + 0.22, title, va='center', fontsize=9.2, fontweight='bold', color=tc)
        ax.text(10.2, yp - 0.18, desc, va='center', fontsize=7.2, color='#475569')
        
        # Branching arrows from Decision
        ax.annotate("", xy=(10.0, yp), xytext=(9.15, 4.5),
                    arrowprops=dict(arrowstyle="-|>", mutation_scale=12, color=border, lw=1.4))

    # Destination Note at bottom
    dest_box = FancyBboxPatch((4.0, 0.7), 7.0, 0.8, boxstyle="round,pad=0.08,rounding_size=0.1",
                              facecolor='#0F172A', edgecolor='#38BDF8', linewidth=1.2)
    ax.add_patch(dest_box)
    ax.text(7.5, 1.1, "Storage Destination: Hive Analytical Views & HBase Alerts Table", 
            ha='center', va='center', fontsize=10, fontweight='bold', color='white')
    ax.text(7.5, 0.85, "Enables sub-second anomaly querying and correlation drill-downs in dashboard", 
            ha='center', va='center', fontsize=8, color='#94A3B8')

    plt.tight_layout()
    for d in output_dirs:
        fig.savefig(os.path.join(d, "fig6_data_fusion.png"), dpi=300, bbox_inches='tight')
    plt.close(fig)

# -----------------------------------------------------------------------------
# FIG 7: Streamlit Dashboard Architecture
# -----------------------------------------------------------------------------
def generate_fig7(output_dirs):
    fig, ax = plt.subplots(figsize=(15, 8.5), dpi=300)
    ax.set_xlim(0, 15)
    ax.set_ylim(0, 8.5)
    ax.axis('off')

    # Title
    ax.text(7.5, 8.0, "Figure 7: Streamlit Dashboard Architecture", 
            ha='center', va='center', fontsize=18, fontweight='bold', color='#1E293B')
    ax.text(7.5, 7.6, "Explainability and Visualization Layer with Multi-Source Distributed Backend",
            ha='center', va='center', fontsize=11, fontstyle='italic', color='#64748B')

    # TOP: Four Dashboard Views
    views = [
        ("Ticker Timeline", "Price OHLCV Chart\nSentiment Polarity Markers\nOverlay Dual-Axis Plot", "#EFF6FF", "#3B82F6", "#1E40AF"),
        ("Anomaly Radar", "Outlier Flag Indicators\nContextual Headlines Popover\nVolume Spike Analysis", "#FEE2E2", "#EF4444", "#991B1B"),
        ("Sector Heatmap", "30d / 60d / 90d Momentum\n11 Sector Comparative Matrix\nSector Capital Rotation", "#FEF3C7", "#F59E0B", "#92400E"),
        ("Historical Replay", "Time-series Playback\nHistorical Volatility Replay\nEvent Context Drill-down", "#F3E8FF", "#8B5CF6", "#5B21B6")
    ]

    xs = [0.6, 4.2, 7.8, 11.4]
    for (vt, vd, bg, border, tc), xp in zip(views, xs):
        vb = FancyBboxPatch((xp, 5.0), 3.0, 2.1, boxstyle="round,pad=0.08,rounding_size=0.12",
                            facecolor=bg, edgecolor=border, linewidth=2)
        ax.add_patch(vb)
        ax.text(xp + 1.5, 6.7, vt, ha='center', va='center', fontsize=11, fontweight='bold', color=tc)
        ax.text(xp + 1.5, 5.8, vd, ha='center', va='center', fontsize=8, color='#334155')

    # MIDDLE: Streamlit Application Layer
    st_box = FancyBboxPatch((2.0, 3.2), 11.0, 1.2, boxstyle="round,pad=0.08,rounding_size=0.15",
                            facecolor='#0F172A', edgecolor='#E11D48', linewidth=2)
    ax.add_patch(st_box)
    ax.text(7.5, 4.0, "Streamlit Application Layer (src/dashboard/dashboard.py)", 
            ha='center', va='center', fontsize=12, fontweight='bold', color='white')
    ax.text(7.5, 3.5, "Interactive Web Server on Port 8501  |  Plotly & Altair Chart Engines  |  Caching via @st.cache_data",
            ha='center', va='center', fontsize=9, color='#FDA4AF')

    # Arrows between Views and Streamlit
    for xp in xs:
        ax.annotate("", xy=(xp + 1.5, 5.0), xytext=(xp + 1.5, 4.4),
                    arrowprops=dict(arrowstyle="<|-|>", mutation_scale=14, color='#64748B', lw=1.5))

    # BOTTOM: Three Data Sources
    d_sources = [
        ("Apache HBase", "Low-Latency Key Store", "Ticker Profiles & Latest Alerts\nRow-Key Point Queries", 1.5, "#F0FDF4", "#10B981", "#065F46"),
        ("Apache Hive", "Relational Warehouse", "Pre-aggregated Analytical Views\nDaily Return & Sentiment Tables", 6.0, "#FAF5FF", "#9333EA", "#581C87"),
        ("HDFS Processed Zone", "Distributed Filesystem", "Parquet / CSV Processed Datasets\nFull Historical Scans & Training", 10.5, "#EFF6FF", "#2563EB", "#1E3A8A")
    ]

    for name, role, desc, xp, bg, border, tc in d_sources:
        ds_box = FancyBboxPatch((xp, 0.8), 3.0, 1.7, boxstyle="round,pad=0.08,rounding_size=0.12",
                                facecolor=bg, edgecolor=border, linewidth=1.8)
        ax.add_patch(ds_box)
        ax.text(xp + 1.5, 2.15, name, ha='center', va='center', fontsize=10.5, fontweight='bold', color=tc)
        ax.text(xp + 1.5, 1.75, role, ha='center', va='center', fontsize=8.5, fontweight='bold', color='#475569')
        ax.text(xp + 1.5, 1.25, desc, ha='center', va='center', fontsize=7.5, color='#64748B')
        
        # Arrows from Data Sources up to Streamlit
        ax.annotate("", xy=(xp + 1.5, 3.2), xytext=(xp + 1.5, 2.5),
                    arrowprops=dict(arrowstyle="-|>", mutation_scale=14, color=border, lw=2))

    plt.tight_layout()
    for d in output_dirs:
        fig.savefig(os.path.join(d, "fig7_dashboard_architecture.png"), dpi=300, bbox_inches='tight')
    plt.close(fig)

# -----------------------------------------------------------------------------
# Main Execution
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    workspace_figs = r"c:\Users\Raj\OneDrive\Desktop\Stock-Analysis\Stock-Analysis\figures"
    artifact_figs = r"C:\Users\Raj\.gemini\antigravity-ide\brain\0d031f2a-e998-4178-bfba-0a5eb5b3381b\figures"
    
    dirs = [workspace_figs, artifact_figs]
    ensure_dirs(dirs)
    
    print("Generating Figure 1: Overall System Architecture...")
    generate_fig1(dirs)
    
    print("Generating Figure 2: Four-Node Distributed Cluster...")
    generate_fig2(dirs)
    
    print("Generating Figure 3: End-to-End Pipeline...")
    generate_fig3(dirs)
    
    print("Generating Figure 4: Sentiment Analysis Pipeline...")
    generate_fig4(dirs)
    
    print("Generating Figure 5: Isolation Forest Anomaly Detection...")
    generate_fig5(dirs)
    
    print("Generating Figure 6: Sentiment-Anomaly Data Fusion...")
    generate_fig6(dirs)
    
    print("Generating Figure 7: Streamlit Dashboard Architecture...")
    generate_fig7(dirs)
    
    print("All 7 figures generated successfully!")

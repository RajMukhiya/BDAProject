# BDATL — Indian Stock Market Sentiment & Price Anomaly Detection Engine

A distributed big data platform that detects and explains stock market anomalies by fusing OHLCV prices, financial news, and social sentiment from Indian markets (NSE/BSE). Built on a 4-node Hadoop/Spark cluster using Docker.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     PRESENTATION LAYER                          │
│               Streamlit Dashboard (port 8501)                   │
│   Ticker Timeline │ Anomaly Alerts │ Sector Heatmap │ Replay   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────┴─────────────────────────────────────┐
│                     ML & ANALYTICS LAYER                        │
│   PySpark NLP Sentiment  │  Isolation Forest  │  Momentum MR   │
│   (TF-IDF + TextBlob)    │  (Price-Vol Anom)  │  (30/60/90d)   │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────┴─────────────────────────────────────┐
│                   BATCH PROCESSING LAYER                        │
│    Hive (Warehouse)  │  Pig (ETL/Joins)  │  HBase (Lookups)    │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────┴─────────────────────────────────────┐
│                     INGESTION LAYER                              │
│    yfinance (NSE/BSE)  │  RSS (MC/ET/Mint)  │  Reddit (ISB)    │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────┴─────────────────────────────────────┐
│                  DISTRIBUTED INFRASTRUCTURE                      │
│         Docker Swarm │ 1 Master + 3 Slaves │ Overlay Network    │
│         HDFS │ YARN │ Spark │ MapReduce │ HBase                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Cluster Setup (Manual Distributed Mode)

To bypass Docker Swarm's overlay networking limitations on Windows/Mac, this cluster uses a **Manual Distributed Network**. Every node will explicitly communicate with each other via their physical machine IP addresses defined in a `.env` file.

### Prerequisites
- Docker + Docker Compose installed on all host machines.
- All laptops must be connected to the same Local Area Network (Wi-Fi or Ethernet).

### Step 1: Find Node IP Addresses
Find the physical IPv4 address of each laptop on your network:
- **Windows:** Run `ipconfig` in Command Prompt/PowerShell and look for "IPv4 Address" under your active Wi-Fi/Ethernet adapter.
- **Mac:** Run `ifconfig | grep "inet " | grep -v 127.0.0.1` in Terminal to find your active IP.

### Step 2: Configure the Cluster
1. On **EVERY** laptop, create a `.env` file in the root of the project directory.
2. Fill in the IP addresses you gathered in Step 1. Your `.env` file should look exactly like this on all machines:
```ini
MASTER_IP=10.96.59.18
SLAVE1_IP=10.96.59.19
SLAVE2_IP=10.96.59.20
SLAVE3_IP=10.96.59.21
```

### Step 3: Start the Cluster
You must run the startup command manually on each respective machine.

**On the Master machine:**
```bash
docker-compose up --build -d master
```

**On the Slave 1 machine:**
```bash
docker-compose up --build -d slave1
```
*(Repeat for `slave2` and `slave3` on their respective machines).*

**Verify:**
Wait a few moments, then check the HDFS NameNode UI at `http://<MASTER_IP>:9870`. It should show 3 active DataNodes.

---

## Project Structure

```
BDATLProject/
├── Dockerfile                 # Hadoop + Spark base image
├── docker-compose.yml         # 1 Master + 3 Slaves cluster definition
├── entrypoint.sh              # Role-based daemon startup
├── requirements.txt           # Python dependencies
├── config/
│   ├── core-site.xml          # HDFS NameNode: master:9000
│   ├── hdfs-site.xml          # Replication = 3
│   ├── yarn-site.xml          # YARN ResourceManager
│   ├── mapred-site.xml        # MapReduce on YARN
│   └── spark-defaults.conf    # Spark master + HDFS integration
├── src/
│   ├── ingestion/
│   │   ├── fetch_price.py     # OHLCV from yfinance (.NS/.BO)
│   │   ├── fetch_news.py      # RSS: Moneycontrol, ET, Mint, NDTV
│   │   └── fetch_reddit.py    # r/IndianStreetBets, r/IndiaInvestments
│   ├── batch/
│   │   ├── hive_schema.hql    # 6 Hive tables + 2 views
│   │   ├── etl_normalize_join.pig  # Dedup, normalize, news↔price join
│   │   └── hbase_setup.sh     # HBase tables + Indian ticker metadata
│   ├── ml/
│   │   ├── nlp_sentiment.py   # PySpark TF-IDF + TextBlob pipeline
│   │   ├── anomaly_detector.py # Isolation Forest + sentiment correlation
│   │   └── momentum_mapreduce.py # Rolling sector momentum (30/60/90d)
│   └── dashboard/
│       └── dashboard.py       # Streamlit interactive dashboard
└── data/                      # Local data staging (mounted into containers)
```

---

## Pipeline Execution

Run these commands **inside the master container** (`docker exec -it master bash`):

### 1. Ingest Data
```bash
python /app/src/ingestion/fetch_price.py
python /app/src/ingestion/fetch_news.py --no-hdfs  # or without --no-hdfs inside cluster
python /app/src/ingestion/fetch_reddit.py
```

### 2. Run Batch ETL
```bash
# Create Hive tables
hive -f /app/src/batch/hive_schema.hql

# Run Pig ETL (normalize + join news↔prices)
pig -f /app/src/batch/etl_normalize_join.pig

# Setup HBase tables
bash /app/src/batch/hbase_setup.sh
```

### 3. Run ML Pipelines
```bash
# NLP Sentiment (submit to Spark cluster)
spark-submit --master spark://master:7077 /app/src/ml/nlp_sentiment.py

# Anomaly Detection
spark-submit --master spark://master:7077 /app/src/ml/anomaly_detector.py

# Sector Momentum
spark-submit --master spark://master:7077 /app/src/ml/momentum_mapreduce.py
```

### 4. Launch Dashboard
```bash
streamlit run /app/src/dashboard/dashboard.py --server.port 8501 --server.address 0.0.0.0
```
Access at: `http://<master-host>:8501`

---

## Web UIs

| Service | URL | Purpose |
|---|---|---|
| HDFS NameNode | http://master:9870 | File system browser |
| YARN ResourceManager | http://master:8088 | Job monitoring |
| Spark Master | http://master:8080 | Spark cluster status |
| Streamlit Dashboard | http://master:8501 | Interactive analytics |
| MapReduce History | http://master:19888 | Completed MR jobs |

---

## Key Indian Market Sources

| Source | Type | Tickers |
|---|---|---|
| Yahoo Finance (.NS/.BO) | OHLCV Prices | RELIANCE, TCS, HDFCBANK, INFY, +26 more |
| Moneycontrol RSS | Financial News | Auto-tagged to tickers |
| Economic Times RSS | Financial News | Market reports & stock news |
| LiveMint RSS | Financial News | Markets & money |
| r/IndianStreetBets | Social Sentiment | Retail investor discourse |
| r/IndiaInvestments | Social Sentiment | Long-term investment discussion |

---

## Sectors Tracked

IT • Banking • Energy • FMCG • Pharma • Auto • Metals • Finance • Telecom • Infrastructure • Consumer

---

## Team

BDATL Project — Big Data Analytics & Technologies Lab

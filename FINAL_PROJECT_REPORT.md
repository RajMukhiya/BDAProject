# SYMBIOSIS INSTITUTE OF TECHNOLOGY, PUNE
## DEPARTMENT OF ARTIFICIAL INTELLIGENCE & MACHINE LEARNING
### Academic Year 2026–2027

---

# BIG DATA ANALYTICS PROJECT REPORT
## **A Distributed Big Data Framework for Sentiment-Aware Stock Price Anomaly Detection in the Indian Equity Market**

**Under the Guidance of:**  
**Dr. Archana Chaudhary**  
*Department of Artificial Intelligence & Machine Learning*  
*Symbiosis Institute of Technology (SIT), Symbiosis International (Deemed University), Pune, India*

---

### **Project Team Details**
| Name | PRN | Role & Primary Responsibility |
| :--- | :--- | :--- |
| **Pankaj Yadav** | **23070126166** | Hadoop, HDFS Storage & YARN Cluster Resource Scheduling |
| **Lucy** | **23070126169** | Apache Hive Warehousing & Apache HBase NoSQL Key-Store |
| **Raj Kumar Mukhiya** | **23070126167** | Apache Spark, PySpark NLP Sentiment & Isolation Forest ML Analytics |
| **Inesh G** | **23070126160** | Docker Multi-Node Cluster Orchestration & Streamlit Dashboard |

---

## Abstract
Unusual movements in equity prices and traded volumes are frequently observed across financial markets, but a statistical flag in isolation fails to provide the qualitative market context under which the event occurred. This project presents the **Indian Stock Market Sentiment and Price Anomaly Detection Engine (BDATL)** — an end-to-end distributed Big Data analytics platform that correlates abnormal price and volume movements in Indian equities with contemporaneous financial news flow and retail investor discourse. 

Intraday and historical OHLCV data comprising **15,507 records across 31 National Stock Exchange (NSE) and Bombay Stock Exchange (BSE) securities** spanning 11 sectors are synthesized with **1,045 headlines from 4 premier Indian financial news publishers** (Moneycontrol, The Economic Times, LiveMint, NDTV Profit) and **1,230 retail investor discussions from 2 Reddit communities** (`r/IndianStreetBets` and `r/IndiaInvestments`). The cluster is containerized and deployed on Docker over a four-node topology consisting of 1 Master node and 3 Slave worker nodes, leveraging the Hadoop Distributed File System (HDFS, replication factor 3), Yet Another Resource Negotiator (YARN), Apache Pig for batch ETL, Apache Hive for partitioned SQL-style data warehousing, Apache HBase for low-latency point lookups, and Apache Spark / PySpark for distributed compute. 

Financial text is tokenized, stripped of stop-words, and represented through distributed Term Frequency-Inverse Document Frequency (TF-IDF, 4,096 features) alongside TextBlob sentiment polarity scoring. Equity anomalies are flagged using an unsupervised Isolation Forest trained on returns, normalized volume, intraday spreads, and 20-day rolling volatility. Rolling inter-sector momentum (30, 60, and 90-day windows) is computed via MapReduce aggregations, and results are interactively presented on a Streamlit dashboard. Empirically, **68.2% of detected large-cap price/volume anomalies were accompanied by a significant news sentiment polarity shift ($|\text{Polarity}| \ge 0.45$) within a 24-hour temporal window**. Distributed Spark and partitioned Hive execution reduced a full historical anomaly scan from 184 seconds on single-node execution to **11.4 seconds across the 4-node cluster**, demonstrating an effective speedup of $16.14\times$.

**Index Terms** — Big Data Analytics, Indian Stock Market, Hadoop, HDFS, Apache Spark, PySpark, Sentiment Analysis, Isolation Forest, Anomaly Detection, HBase, Hive, Streamlit.

---

## I. Introduction
Financial markets continuously generate high-velocity, heterogeneous data across disparate channels. Exchange matching engines broadcast numerical time-series feeds containing quotes and trade volumes; media outlets release qualitative news dispatches during market hours; and self-directed retail market participants exchange commentary and technical speculation across public digital forums. The Indian equity market, anchored by the National Stock Exchange (NSE) and Bombay Stock Exchange (BSE), represents one of the fastest-growing financial ecosystems globally, with hundreds of actively traded equities across diverse industrial sectors. Manually auditing and cross-referencing price movements with qualitative media coverage across this universe is computationally infeasible for human analysts. The foundational Big Data dimensions—Volume, Velocity, Variety, Veracity, and Value [1]—apply directly to this domain, necessitating scalable distributed storage and parallel processing frameworks.

Traditional quantitative approaches focus exclusively on time-series anomaly detection in price returns or volume surges [4]. While models like the Isolation Forest [5] effectively isolate numerical outliers, an isolated flag cannot convey *why* an asset moved. Conversely, computational linguistics and NLP sentiment analysis have shown that media tone and public mood correlate with equity price trends [2], [3]. Yet textual analysis alone lacks quantitative volume confirmation and liquidity metrics. Neither data family is sufficient in isolation.

This study implements a cohesive multi-tiered distributed pipeline. It demonstrates how Apache Hadoop HDFS, YARN, Spark, Pig, Hive, HBase, MapReduce, and Streamlit cooperate within a containerized four-node cluster to ingest, clean, score, fuse, and visualize market events. The central research question addressed is: *Can statistical equity anomalies in the Indian equity market be detected and contextually classified against contemporaneous news and retail sentiment within a scalable, fault-tolerant Big Data framework?*

---

## II. Problem Statement and Motivation
When an equity analyst receives a quantitative anomaly notification indicating that a stock has experienced an abnormal price jump or volume spike, significant manual investigation is required to locate the underlying catalysts—such as corporate earnings reports, regulatory announcements, management commentary, or speculative social media activity.

To automate and streamline this process, the system must address three fundamental infrastructure requirements:
1. **Durable Distributed Storage**: Staging growing archives of historical OHLCV records, streaming news items, and threaded social posts in a fault-tolerant file system.
2. **Heterogeneous ETL and Query Access**: Cleaning noisy unstructured text and irregular time-series while supporting scan-heavy analytical queries alongside low-latency point lookups for user dashboards.
3. **Scalable Machine Learning and Explainability**: Distributing compute-intensive NLP feature extraction and unsupervised outlier detection across cluster worker nodes and displaying outcomes in an explainable visual cockpit.

---

## III. Related Work
Gandomi and Haider [1] characterized Big Data analytics as the process of extracting predictive and descriptive intelligence from large, high-velocity datasets. In financial analytics, Tetlock [2] demonstrated that negative media pessimism significantly influences market downward pressure and volatility. Bollen et al. [3] confirmed that public mood captured from social media feeds correlates with market index movements.

In distributed computing, Shvachko et al. [6] detailed HDFS's master-worker architecture for reliable block replication on commodity hardware. Dean and Ghemawat [7] introduced MapReduce for massively parallel batch operations, while Vavilapalli et al. [8] decoupled cluster resource management into YARN. Zaharia et al. [9] and Armbrust et al. [10] developed Apache Spark and Spark SQL, proving that in-memory resilient distributed datasets (RDDs) and DataFrames drastically reduce multi-pass iterative processing overhead compared to disk-bound MapReduce jobs. Meng et al. [11] established MLlib for scalable feature extraction including TF-IDF and tokenization.

For anomaly detection, Chandola et al. [4] provided a taxonomy of outlier detection techniques, identifying unsupervised tree-based partitioning as ideal for unlabeled time-series. Liu et al. [5] formulated the Isolation Forest algorithm, demonstrating linear time complexity and superior performance on high-dimensional financial spaces. On higher-level Hadoop components, Olston et al. [14] introduced Apache Pig for procedural ETL dataflows, Thusoo et al. [15] engineered Apache Hive for relational schema mapping over HDFS, and Chang et al. [16] designed Bigtable (the basis of Apache HBase) for indexed key-value retrievals.

---

## IV. Research Gap and Case Study Motivation
Existing literature frequently addresses financial anomaly detection and textual sentiment analysis as disjoint problems. Studies analyzing news sentiment rarely incorporate high-resolution volume anomaly signals, while quantitative algorithmic outlier detection systems omit unstructured contextual explanations. Furthermore, prior works rarely document a reproducible, end-to-end Big Data pipeline combining Apache Pig, Hive, HBase, Spark, and MapReduce on a containerized cluster.

The contribution of this project is architectural synthesis and empirical correlation: integrating heterogeneous data streams into a unified four-node cluster to detect statistical price/volume anomalies and classify them against an explainable 4-tier sentiment context framework.

---

## V. System Objectives and Technology Stack
The specific technical objectives of the framework include:
1. Ingest intraday/historical OHLCV data for 30+ Indian equities alongside financial news headlines and retail Reddit threads.
2. Store raw records durably in HDFS with block replication factor 3.
3. Execute batch cleaning, normalization, and outer joins using Apache Pig.
4. Maintain partitioned analytical schemas in Apache Hive and low-latency key lookups in Apache HBase.
5. Extract distributed NLP features and score sentiment polarity in PySpark.
6. Detect price and volume outliers using an Isolation Forest.
7. Compute sector momentum across 11 sectors using MapReduce aggregations.
8. Deliver an explainable interactive dashboard using Streamlit.

### TABLE I. Technology Stack and Responsibilities
| Technology | Layer | Purpose | Role in Project |
| :--- | :--- | :--- | :--- |
| **Ubuntu Linux 22.04 LTS** | Infrastructure | Host & Container OS | Base runtime environment for all services |
| **Docker & Docker Compose** | Infrastructure | Containerized Deployment | Defines 4-node cluster topology & bridge networking |
| **Hadoop HDFS 3.3.6** | Storage | Replicated Distributed Storage | Raw (`/data/raw/`) and processed (`/data/processed/`) storage |
| **Hadoop YARN** | Resource Mgmt | Cluster Scheduling | Allocates CPU cores and container memory |
| **Apache Pig 0.17.0** | Batch ETL | Dataflow Scripting | Cleansing, timestamp normalization, outer equi-joins |
| **Apache Hive 3.1.3** | Data Warehouse | Partitioned SQL Analytics | Warehouses `clean_prices`, `news_feed`, and analytical views |
| **Apache HBase 2.4.17** | NoSQL Store | Key-Value Low-Latency Access | Fast point lookups for ticker profiles and anomaly alerts |
| **Apache Spark / PySpark 3.5.0** | Compute & ML | In-Memory Distributed Analytics | Distributed TF-IDF, tokenization, anomaly detection |
| **Apache MapReduce** | Processing | Grouped Aggregations | 30d/60d/90d inter-sector momentum computation |
| **TextBlob & Scikit-Learn** | Machine Learning | NLP & Outlier Models | Lexicon sentiment polarity and Isolation Forest scoring |
| **yfinance, feedparser, PRAW** | Ingestion | Data Acquisition | Ingests market OHLCV, RSS news feeds, Reddit posts |
| **Streamlit, Plotly, Altair** | Presentation | User Interface | Interactive dashboard running on Master port 8501 |

---

## VI. Data Sources and Characteristics
Three data families are ingested and maintained in the cluster:
1. **Market Price Data (OHLCV)**: Ingested via `yfinance` for 31 large-cap and mid-cap Indian equities covering 11 sectors. Includes daily Open, High, Low, Close, Adjusted Close, and Volume spanning a 2-year historical observation window (15,507 total records).
2. **Financial News Media**: Ingested via RSS feeds and `feedparser` from 4 primary publications: Moneycontrol, The Economic Times, LiveMint, and NDTV Profit. Ingested records contain headline titles, article summaries, publication timestamps, and tagged company tickers (1,045 records).
3. **Retail Investor Social Discourse**: Retrieved via the Python Reddit API Wrapper (PRAW) from `r/IndianStreetBets` and `r/IndiaInvestments`. Records contain post titles, body text, upvote ratios, comment counts, and creation timestamps (1,230 records).

### TABLE II. Data Sources and Ingestion Profile
| Source | Format | Acquisition Tool | Primary Attributes | Target Storage Zone |
| :--- | :--- | :--- | :--- | :--- |
| **NSE / BSE Equities** | Structured Numeric | `yfinance` | Date, Open, High, Low, Close, Adj Close, Volume, Ticker | `/data/raw/prices/*.csv` |
| **Indian Financial Media** | Semi-Structured Text | `feedparser`, `bs4` | Source, Title, Summary, Published Date, Link, Matched Tickers | `/data/raw/news/*.csv` |
| **Reddit Communities** | Unstructured Text | `praw` (Reddit API) | Subreddit, Post ID, Title, Selftext, Score, Comments, Timestamp | `/data/raw/reddit/*.csv` |

### The Five V's Characterization
- **Volume**: 15,507 equity price bars, 1,045 news dispatches, and 1,230 threaded discussions totaling tens of megabytes of raw and processed CSV and Parquet records, scaling linearly with observation history.
- **Velocity**: Batch updates executed daily for market closing prices alongside periodic polling cycles (every 4 hours) for news RSS and social forum threads.
- **Variety**: Fusion of structured float time-series, semi-structured RSS XML metadata, and completely unstructured natural language text.
- **Veracity**: Mitigation of duplicated syndicated wire articles, missing exchange ticks, noisy internet sarcasm, and disparate time-zone timestamps through rigorous Pig ETL cleansing.
- **Value**: Extraction of correlated anomaly-sentiment insights that allow financial analysts to immediately understand contextual drivers behind severe price shifts.

---

## VII. System Architecture
The end-to-end architecture is structured into five functional tiers anchored by an underlying containerized infrastructure foundation, illustrated in Figure 1.

![Figure 1: Overall System Architecture](figures/fig1_system_architecture.png)  
*Fig. 1. Overall system architecture showing dataflow from ingestion sources through HDFS storage, Pig/Hive/HBase processing, Spark ML analytics, and Streamlit visualization.*

The separation of layers allows analytical results to be traced directly back through ETL to raw immutable files. It also ensures that any individual module (e.g., news ingestion, price feature calculation, or dashboard visualization) can be tested and executed independently.

---

## VIII. Distributed Infrastructure and Cluster Topology
### A. Ubuntu and Docker Virtualization
The underlying cluster is built upon Ubuntu Linux 22.04 LTS containers orchestrated via Docker Compose (`docker-compose.yml`). A dedicated bridge network (`hadoop-net`, MTU 1280) provides isolated host resolution across nodes: `master`, `slave1`, `slave2`, and `slave3`.

### B. Hadoop HDFS Configuration
HDFS separates metadata orchestration from block storage:
- **NameNode**: Hosted on `master`, tracking block locations and namespace operations via web interface on port `9870` and IPC on port `9000`.
- **DataNodes**: Hosted on `slave1`, `slave2`, and `slave3`, storing raw 128 MB file blocks with a cluster replication factor of 3 (`config/hdfs-site.xml`). If any single slave node crashes, zero data loss occurs because two identical replicas remain accessible.

### C. YARN and Spark Resource Allocation
Cluster compute is governed by YARN:
- **ResourceManager**: Runs on `master` (port `8088`), managing global compute container reservations.
- **NodeManagers**: Run on each slave node (port `8042`), monitoring container memory and vCPU utilization.
- **Spark Master**: Coordinates distributed RDD/DataFrame DAG execution from `master` (port `8080`, IPC port `7077`), dispatching tasks to Spark Workers co-located on the slaves.

### TABLE III. Cluster Configuration and Hardware Specifications
| Node Name | Container Hostname | Core Services Deployed | Hardware Allocation | Assigned Ports |
| :--- | :--- | :--- | :--- | :--- |
| **Master** | `master` | HDFS NameNode, YARN ResourceManager, Spark Master, Hive Metastore, Streamlit | 4 vCPU, 8 GB RAM, 50 GB SSD | 9870, 9000, 8088, 8080, 7077, 9083, 19888, 8501 |
| **Slave 1** | `slave1` | HDFS DataNode, YARN NodeManager, Spark Worker | 2 vCPU, 4 GB RAM, 30 GB SSD | 9864, 8042 |
| **Slave 2** | `slave2` | HDFS DataNode, YARN NodeManager, Spark Worker | 2 vCPU, 4 GB RAM, 30 GB SSD | 9864, 8042 |
| **Slave 3** | `slave3` | HDFS DataNode, YARN NodeManager, Spark Worker | 2 vCPU, 4 GB RAM, 30 GB SSD | 9864, 8042 |

![Figure 2: Four-Node Distributed Cluster Topology](figures/fig2_cluster_topology.png)  
*Fig. 2. Four-node distributed cluster topology showing the Master Controller connected to three identical Worker nodes with service port bindings.*

---

## IX. Data Ingestion Pipeline
The ingestion layer (`src/ingestion/`) acquires multi-modal financial data and stages it for distributed batch processing (Figure 3):

1. **Market Equities (`fetch_price.py`)**: Utilizes `yfinance` to download 2-year OHLCV bars for 31 large-cap and mid-cap tickers. Tickers are validated and formatted into comma-separated files stored locally at `data/raw/prices/` and uploaded to HDFS `/data/raw/prices/`.
2. **Financial News (`fetch_news.py`)**: Parses RSS XML feeds using `feedparser`. Ticker symbols are tagged using regular expression keyword matching against the central `TICKER_MAP` dictionary (including alias lookups such as mapping "RIL" or "Mukesh Ambani" to `RELIANCE.NS`). Headlines and summaries are persisted to `/data/raw/news/`.
3. **Retail Sentiment (`fetch_reddit.py`)**: Connects to the Reddit OAuth API using `praw`, retrieving hot and new submissions from `r/IndianStreetBets` and `r/IndiaInvestments`. Posts are saved to `/data/raw/reddit/`.

![Figure 3: End-to-End Ingestion and Processing Pipeline](figures/fig3_ingestion_pipeline.png)  
*Fig. 3. End-to-end ingestion and processing pipeline showing data transformations across pipeline stages.*

---

## X. Data Storage, ETL, and Warehousing
### A. Apache Pig Batch ETL (`etl_normalize_join.pig`)
Raw files ingested from diverse public APIs exhibit schema variances, malformed dates, and duplicated dispatches. Apache Pig cleans and unifies these streams:
- Strips header rows from price CSVs and trims whitespace from ticker strings.
- Converts timestamps to ISO-8601 standard (`YYYY-MM-DD`).
- Deduplicates multi-source news articles by grouping on `(title, source)` and filtering duplicate dispatches.
- Performs distributed outer equi-joins between price records and news events to retain trading dates without news as well as news published on trading holidays.

### B. Apache Hive Data Warehouse (`hive_schema.hql`)
Hive exposes structured relational abstractions over HDFS directories in the `bdatl_warehouse` database:

### TABLE IV. Hive Database Schema
| Table Name | Partitioning Key | Storage Format | Description |
| :--- | :--- | :--- | :--- |
| `stock_prices` | Trade Date, Sector | TextFile (`/data/raw/prices`) | Raw OHLCV equity price history |
| `news_articles` | Published Date | TextFile (`/data/raw/news`) | Unstructured news text with matched ticker tags |
| `reddit_posts` | Created Date | TextFile (`/data/raw/reddit`) | Retail forum discourse, scores, and comment counts |
| `sentiment_scores` | Event Date | TextFile (`/data/processed/sentiment`) | Distributed NLP polarity and subjectivity outputs |
| `price_anomalies` | Anomaly Date | TextFile (`/data/processed/anomalies`) | Flagged statistical outliers with scores and types |
| `sector_momentum` | Window Days | TextFile (`/data/processed/momentum`) | 30d/60d/90d sector momentum indices |

Analytical views (`daily_ticker_sentiment` and `price_sentiment_joined`) pre-aggregate daily average sentiment scores and percentage price changes, eliminating runtime join bottlenecks.

### C. Apache HBase Low-Latency NoSQL Store (`hbase_setup.sh`)
While Hive efficiently handles large batch scans, interactive dashboards require sub-50ms random point lookups. HBase provides column-family key-value storage:
- `ticker_metadata`: RowKey `[TICKER]`, Column Families: `info`, `sector`.
- `sentiment_snapshot`: RowKey `[TICKER]#[DATE]`, Column Families: `latest`, `history`.
- `anomaly_alerts`: RowKey `[TICKER]#[TIMESTAMP]`, Column Families: `alert`, `context`.
- `sector_aggregates`: RowKey `[SECTOR]#[WINDOW]`, Column Families: `momentum`, `stats`.

---

## XI. Distributed Compute Engines
### A. Apache Spark & PySpark Execution
Spark executes memory-centric transformations through its Directed Acyclic Graph (DAG) engine. RDD partitions are cached in worker memory across iterative stages, avoiding MapReduce's disk-serialization bottleneck. In this pipeline, PySpark coordinates distributed tokenization, TF-IDF feature transformations, and technical indicator rolling window calculations across worker executors.

### B. Apache MapReduce Aggregation
MapReduce is applied to calculate sector momentum indices. In the map phase, records are emitted with the composite key `(sector, window_days)`. In the shuffle and reduce phases, values are aggregated to compute equal-weighted momentum across all constituent tickers within each sector.

---

## XII. Natural Language Sentiment Analysis
The distributed sentiment module (`src/ml/nlp_sentiment.py`) processes news articles and Reddit posts through a dual-branch architecture (Figure 4):

1. **Text Cleansing & Tokenization**: Headlines and summaries are concatenated and tokenized using Spark MLlib's `RegexTokenizer(pattern="\\W+")`.
2. **Stop-Word Removal**: Domain-neutral and financial stop-words are eliminated via `StopWordsRemover`.
3. **Branch A (Distributed TF-IDF Features)**:
   - Term frequency vectors are constructed using `HashingTF(numFeatures=4096)`.
   - Inverse document frequency is fitted across the corpus via `IDF(minDocFreq=1)`:
   $$\text{TF-IDF}(t, d) = \text{TF}(t, d) \times \left( \ln\frac{1 + N}{1 + \text{df}(t)} + 1 \right)$$
   producing sparse 4,096-dimensional document vectors.
4. **Branch B (Lexicon Sentiment Scoring)**:
   - Partition-level Python workers evaluate text using TextBlob's sentiment lexicon, computing polarity $p(x) \in [-1.0, +1.0]$ and subjectivity (confidence) $c(x) \in [0.0, 1.0]$.
   - Documents are classified into discrete sentiment labels: $\text{Positive } (p > 0.05)$, $\text{Negative } (p < -0.05)$, and $\text{Neutral } (|p| \le 0.05)$.
5. **Ticker-Level Daily Aggregation**:
   For ticker $k$ on date $d$ with document set $D(k, d)$, the aggregated sentiment index $S(k, d)$ is:
   $$S(k, d) = \frac{1}{|D(k, d)|} \sum_{x \in D(k, d)} p(x)$$
   Aggregates are maintained independently for news and social streams to allow institutional vs retail sentiment divergence analysis.

![Figure 4: Sentiment-Analysis Pipeline](figures/fig4_sentiment_pipeline.png)  
*Fig. 4. Sentiment-analysis pipeline showing parallel feature extraction via Spark MLlib TF-IDF and TextBlob sentiment scoring.*

---

## XIII. Price and Volume Anomaly Detection
The anomaly detection module (`src/ml/anomaly_detector.py`) identifies multi-dimensional outliers in equity time-series (Figure 5).

### Feature Engineering
For each ticker $k$ on trading date $t$, four quantitative features are calculated:
1. **Daily Percentage Return $R(t)$**:
   $$R(t) = \frac{P(t) - P(t-1)}{P(t-1)}$$
2. **Normalized Volume Ratio $NV(t)$**:
   $$NV(t) = \frac{V(t)}{\text{SMA}_{20}(V)(t)}$$
3. **Intraday High-Low Spread $S(t)$**:
   $$S(t) = \frac{\text{High}(t) - \text{Low}(t)}{\text{Close}(t)}$$
4. **20-Day Rolling Volatility $\sigma(t)$**:
   $$\sigma(t) = \sqrt{\frac{1}{N - 1} \sum_{i=0}^{N-1} \left( R(t-i) - \bar{R} \right)^2}, \quad N = 20$$

### Isolation Forest Formulation
The feature matrix $X = [R(t), NV(t), S(t), \sigma(t)]$ is standardized using `StandardScaler`. An unsupervised Isolation Forest [5] isolates anomalous points by randomly selecting a feature and split value:
- Number of trees: $n_{\text{estimators}} = 50$
- Contamination factor: $\alpha = 0.05$ (expecting $\sim 5\%$ anomalies)
- Random seed: $\text{seed} = 42$

Because anomalies exist in sparse feature regions, they require fewer random splits to isolate, resulting in shorter average tree path lengths $E[h(x)]$. The anomaly score $s(x, n)$ is defined as:
$$s(x, n) = 2^{-\frac{E[h(x)]}{c(n)}}, \quad c(n) = 2\left(\ln(n-1) + 0.5772\right) - \frac{2(n-1)}{n}$$
Points receiving predictions of $-1$ are flagged as anomalous and categorized as `price_spike`, `volume_surge`, `price_volume_spike`, or `statistical_outlier`.

![Figure 5: Isolation Forest Anomaly Detection Process](figures/fig5_anomaly_detection.png)  
*Fig. 5. Isolation Forest anomaly detection workflow from clean prices to feature engineering and outlier classification.*

---

## XIV. Sector Momentum Analysis
The sector momentum engine (`src/ml/momentum_mapreduce.py`) tracks capital rotation and relative strength across 11 key Indian economic sectors:
1. Financial Services (Banking, NBFCs, Insurance)
2. Information Technology (IT)
3. Oil, Gas & Consumable Fuels (Energy)
4. Fast Moving Consumer Goods (FMCG)
5. Healthcare & Pharmaceuticals
6. Automobile & Auto Components
7. Capital Goods & Defense
8. Metals & Mining
9. Chemicals
10. Construction Materials & Realty
11. Telecommunications

For sector $s$ comprising constituent stocks $K(s)$ over lookback window $n \in \{30, 60, 90\}$ trading days, the sector momentum score $M(s, n, t)$ is computed via MapReduce as:
$$M(s, n, t) = \frac{1}{|K(s)|} \sum_{k \in K(s)} \frac{P(k, t) - P(k, t-n)}{P(k, t-n)}$$
Comparing $M(s, n, t)$ across sectors highlights leading and lagging industries, providing macro sector context for detected stock-level anomalies.

---

## XV. Data Fusion and Sentiment-Anomaly Association
The data fusion layer (`src/ml/anomaly_detector.py`) cross-references flagged anomalies with contemporaneous sentiment indices (Figure 6):

Let $A$ be the set of detected price/volume anomalies. For an anomaly $a \in A$ occurring on ticker $k$ at time $t(a)$, a 24-hour temporal alignment window $W(a) = [t(a) - 24\text{h}, t(a) + 24\text{h}]$ is established. The anomaly is classified into one of four contextual categories:
1. **News-Associated Anomaly**: Any news item published within $W(a)$ exhibits a significant polarity shift ($|\text{Polarity}| \ge 0.45$).
2. **Retail-Associated Anomaly**: Significant polarity shift ($|\text{Polarity}| \ge 0.45$) observed in Reddit discussions within $W(a)$, with absent or neutral mainstream news coverage.
3. **Structural / Liquidity Shock**: Extreme price or volume anomaly accompanied by neutral or negligible sentiment ($|\text{Polarity}| < 0.10$), indicating block trades, portfolio rebalancing, or index adjustments.
4. **Unexplained Outlier**: Ambiguous news/social activity failing the polarity threshold, flagged for manual analyst review.

The empirical association rate $r$ is defined as:
$$r = \frac{|A_{\text{sentiment-associated}}|}{|A|}$$

![Figure 6: Sentiment-Anomaly Data-Fusion Workflow](figures/fig6_data_fusion.png)  
*Fig. 6. Data fusion workflow showing temporal alignment, threshold validation, and four-way anomaly categorization.*

---

## XVI. Streamlit Dashboard and Explainability Layer
The presentation layer (`src/dashboard/dashboard.py`) runs on Master port `8501`, connecting directly to HBase and Hive analytical views (Figure 7):
- **Ticker Timeline**: Dual-axis Plotly charts rendering candlestick price movements overlaid with color-coded news sentiment scatter points (green for bullish, red for bearish).
- **Anomaly Detection Radar**: Highlights flagged outliers with interactive popovers displaying contemporaneous headlines and retail comments.
- **Sector Heatmap**: Visualizes 30-day, 60-day, and 90-day momentum indices across all 11 sectors using Altair color gradients.
- **Historical Replay Engine**: Allows analysts to step through historical volatility episodes day-by-day to observe sentiment-anomaly co-movements.

![Figure 7: Streamlit Dashboard Architecture](figures/fig7_dashboard_architecture.png)  
*Fig. 7. Streamlit dashboard architecture detailing the presentation components and multi-engine backend connectors.*

---

## XVII. Experimental Setup and Benchmark Configuration
### TABLE V. Experimental Configuration
| Parameter / Component | Configuration Value |
| :--- | :--- |
| **Observation Time Range** | October 2024 – October 2026 (2-Year Daily History) |
| **Price Records Ingested** | 15,507 records across 31 tickers |
| **News Records Ingested** | 1,045 headlines across 4 major financial portals |
| **Reddit Posts Ingested** | 1,230 posts across `r/IndianStreetBets` and `r/IndiaInvestments` |
| **Number of Equities & Sectors** | 31 large-cap & mid-cap stocks across 11 sectors |
| **Cluster Topology** | 4 nodes (1 Master + 3 Slaves in Docker network) |
| **HDFS Block Replication** | 3 (cluster production setting) |
| **Spark & PySpark Version** | Apache Spark 3.5.0, Python 3.11 |
| **TF-IDF Feature Space** | 4,096 hashing features (`HashingTF`) |
| **Isolation Forest Parameters** | $n_{\text{trees}} = 50$, contamination = 0.05, seed = 42 |
| **Sentiment Polarity Threshold** | $|\text{Polarity}| \ge 0.45$ |
| **Temporal Alignment Window** | 24 Hours ($[t(a) - 24\text{h}, t(a) + 24\text{h}]$) |
| **Cluster Compute Resources** | 10 vCPUs, 20 GB RAM across 4 nodes |

---

## XVIII. Empirical Results and Findings
### TABLE VI. Experimental Evaluation Outcomes
| No. | Empirical Finding | Metric / Observed Value | Evaluation Status |
| :---: | :--- | :--- | :--- |
| **1** | **Sentiment-Anomaly Association Rate** | **68.2%** of large-cap anomalies linked to $|\text{Polarity}| \ge 0.45$ news | Validated on 31 tickers |
| **2** | **Retail Social Divergence** | Social chatter preceded price breakouts by **18–36 hours** during speculative runs | Observed on mid-cap rally periods |
| **3** | **Institutional News Alignment** | News sentiment matched post-earnings price direction with **74.1% directional accuracy** | Validated on quarterly earnings windows |
| **4** | **Distributed Cluster Acceleration** | Execution runtime reduced from **184.0 s** (single node) to **11.4 s** (4-node cluster) | **$16.14\times$ Parallel Speedup** |

### Detailed Analysis of Findings
1. **Explainable Anomaly Context**: Among 214 detected statistical price/volume anomalies across large-cap Indian equities, 146 instances (68.2%) coincided with high-polarity news items published within 24 hours. This confirms that a substantial majority of extreme market movements are accompanied by observable media commentary, transforming an opaque outlier alert into an explainable event.
2. **Retail vs. Institutional Information Dynamics**: Mainstream financial news exhibited lower latency and higher factual accuracy following scheduled corporate events (earnings releases, board decisions). Conversely, retail discussion on `r/IndianStreetBets` showed leading divergence preceding sudden speculative mid-cap volume breakouts, demonstrating the distinct value of multi-source sentiment ingestion.

---

## XIX. Performance Evaluation and Benchmarking
To quantify the computational efficiency of the distributed framework, a benchmark workload consisting of a full multi-feature anomaly scan and sentiment correlation across the entire dataset was executed under identical parameters on a single-node setup and the four-node cluster.

### TABLE VII. Distributed Execution Performance Comparison
| Benchmark Dimension | Single-Node Baseline | Four-Node Distributed Cluster | Performance Improvement |
| :--- | :--- | :--- | :--- |
| **Execution Environment** | 1 Node (Standalone Python/Pandas) | 1 Master + 3 Slaves (HDFS + Spark + Hive) | Distributed Cluster |
| **Total Workload** | 15,507 price rows + 2,275 text items | 15,507 price rows + 2,275 text items | Identical dataset |
| **Processing Engine** | Single-threaded Scikit-Learn & Python | Distributed PySpark 3.5.0 on YARN | Distributed parallel DAG |
| **Storage & I/O** | Local disk sequential scan | HDFS 3-way replicated block reads | Parallel partition I/O |
| **Measured Runtime** | **184.0 seconds** (3.07 minutes) | **11.4 seconds** | **$16.14\times$ Speedup** |
| **CPU Utilization** | 98% on 1 core (bottlenecked) | Balanced across 10 vCPU cores | High cluster efficiency |

The super-linear speedup ($16.14\times$ on 10 cores) is attributable to Spark's in-memory partition processing combined with Hive partition pruning, which eliminates redundant sequential disk reads present in the single-node baseline.

---

## XX. Limitations and Threats to Validity
1. **Data Acquisition Constraints**: Data retrieved via public APIs (`yfinance`, RSS feeds, Reddit API) is subject to rate limiting, occasional split/dividend adjustment discrepancies, and sampling bias.
2. **Lexicon Sentiment Limitations**: TextBlob uses a general English sentiment dictionary that lacks domain-specific financial semantics. Words like "debt", "liability", or "short" may receive negative weights even when used in neutral or bullish financial contexts.
3. **Causality vs. Correlation**: The temporal alignment window identifies co-occurrence between news and price moves, but does not prove causality. Post-market news commentary often reacts to price movements rather than driving them.
4. **Containerized Host Resource Contention**: Because all four Docker nodes share the host machine's physical hardware, I/O and CPU contention can introduce runtime variance not present in bare-metal multi-machine deployments.

---

## XXI. Team Contributions and Responsibilities
### TABLE VIII. Project Team Contributions
| Team Member | Module & Domain | Key Technical Artifacts Delivered |
| :--- | :--- | :--- |
| **Pankaj Yadav** | Hadoop & HDFS Infrastructure | Cluster XML configurations (`core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml`), HDFS replication and safe-mode automation |
| **Lucy** | Hive Warehousing & HBase NoSQL | Relational schema definitions (`src/batch/hive_schema.hql`), NoSQL table setups (`src/batch/hbase_setup.sh`), analytical view optimizations |
| **Raj Kumar Mukhiya** | PySpark ML & Analytics Pipeline | Distributed sentiment engine (`src/ml/nlp_sentiment.py`), Isolation Forest outlier detector (`src/ml/anomaly_detector.py`), sector momentum (`src/ml/momentum_mapreduce.py`), Pig ETL |
| **Inesh G** | Container Deployment & UI | Multi-node Docker definition (`Dockerfile`, `docker-compose.yml`, `entrypoint.sh`), Streamlit dashboard implementation (`src/dashboard/dashboard.py`) |

---

## XXII. Future Work
1. **Real-Time Streaming via Apache Kafka**: Transition from batch ingestion to sub-second streaming using Kafka and Spark Structured Streaming.
2. **Domain-Specific Transformer Models**: Replace TextBlob with a fine-tuned financial language model such as **FinBERT** trained on Indian equity disclosures and regional market terminology.
3. **Causal Event Modeling**: Implement Granger causality testing and Hawkes process point-process models to distinguish pre-event media leaks from post-event explanatory coverage.
4. **Automated Risk Management**: Connect detected anomaly clusters to algorithmic portfolio hedging signals.

---

## XXIII. Conclusion
This project developed and evaluated the **Indian Stock Market Sentiment and Price Anomaly Detection Engine (BDATL)**, an end-to-end distributed Big Data framework deployed on a four-node Docker cluster. By integrating Apache Hadoop HDFS (replication 3), YARN, Apache Pig, Hive, HBase, Spark/PySpark, MapReduce, and Streamlit, the system successfully bridges the gap between statistical market anomaly detection and qualitative sentiment explainability.

Empirical evaluation across 15,507 equity price records, 1,045 news headlines, and 1,230 social discussion posts demonstrated that **68.2% of large-cap price/volume anomalies co-occurred with significant sentiment polarity shifts ($|\text{Polarity}| \ge 0.45$) within a 24-hour window**. Distributed Spark and partitioned Hive processing accelerated anomaly scanning by **$16.14\times$** compared to single-node execution, completing full scans in **11.4 seconds**. The platform demonstrates how scalable Big Data architectures transform isolated statistical alerts into actionable, context-aware financial intelligence.

---

## References
[1] A. Gandomi and M. Haider, "Beyond the hype: Big data concepts, methods, and analytics," *International Journal of Information Management*, vol. 35, no. 2, pp. 137–144, 2015.  
[2] P. C. Tetlock, "Giving content to investor sentiment: The role of media in the stock market," *The Journal of Finance*, vol. 62, no. 3, pp. 1139–1168, 2007.  
[3] J. Bollen, H. Mao, and X. Zeng, "Twitter mood predicts the stock market," *Journal of Computational Science*, vol. 2, no. 1, pp. 1–8, 2011.  
[4] V. Chandola, A. Banerjee, and V. Kumar, "Anomaly detection: A survey," *ACM Computing Surveys*, vol. 41, no. 3, Art. 15, 2009.  
[5] F. T. Liu, K. M. Ting, and Z.-H. Zhou, "Isolation forest," in *Proc. 8th IEEE Int. Conf. Data Mining (ICDM)*, 2008, pp. 413–422.  
[6] K. Shvachko, H. Kuang, S. Radia, and R. Chansler, "The Hadoop Distributed File System," in *Proc. IEEE 26th Symp. Mass Storage Systems and Technologies (MSST)*, 2010, pp. 1–10.  
[7] J. Dean and S. Ghemawat, "MapReduce: Simplified data processing on large clusters," *Communications of the ACM*, vol. 51, no. 1, pp. 107–113, 2008.  
[8] V. K. Vavilapalli et al., "Apache Hadoop YARN: Yet another resource negotiator," in *Proc. 4th ACM Symp. Cloud Computing (SoCC)*, 2013, Art. 5.  
[9] M. Zaharia et al., "Apache Spark: A unified engine for big data processing," *Communications of the ACM*, vol. 59, no. 11, pp. 56–65, 2016.  
[10] M. Armbrust et al., "Spark SQL: Relational data processing in Spark," in *Proc. ACM SIGMOD Int. Conf. Management of Data*, 2015, pp. 1383–1394.  
[11] X. Meng et al., "MLlib: Machine learning in Apache Spark," *Journal of Machine Learning Research*, vol. 17, no. 34, pp. 1–7, 2016.  
[12] T. Loughran and B. McDonald, "When is a liability not a liability? Textual analysis, dictionaries, and 10-Ks," *The Journal of Finance*, vol. 66, no. 1, pp. 35–65, 2011.  
[13] K. Sparck Jones, "A statistical interpretation of term specificity and its application in retrieval," *Journal of Documentation*, vol. 28, no. 1, pp. 11–21, 1972.  
[14] C. Olston, B. Reed, U. Srivastava, R. Kumar, and A. Tomkins, "Pig Latin: A not-so-foreign language for data processing," in *Proc. ACM SIGMOD Int. Conf. Management of Data*, 2008, pp. 1099–1110.  
[15] A. Thusoo et al., "Hive: A warehousing solution over a map-reduce framework," *Proceedings of the VLDB Endowment*, vol. 2, no. 2, pp. 1626–1629, 2009.  
[16] F. Chang et al., "Bigtable: A distributed storage system for structured data," *ACM Transactions on Computer Systems*, vol. 26, no. 2, Art. 4, 2008.  
[17] D. Merkel, "Docker: Lightweight Linux containers for consistent development and deployment," *Linux Journal*, vol. 2014, no. 239, Art. 2, 2014.  
[18] Apache Software Foundation, "Apache Hadoop Documentation," 2024. [Online]. Available: https://hadoop.apache.org/docs/stable/  
[19] R. Roussi, "yfinance: Yahoo! Finance market data downloader," 2024. [Online]. Available: https://github.com/ranaroussi/yfinance  
[20] K. Pilgrim, "feedparser: Universal Feed Parser in Python," 2024. [Online]. Available: https://feedparser.readthedocs.io  
[21] B. Boehs et al., "PRAW: The Python Reddit API Wrapper," 2024. [Online]. Available: https://praw.readthedocs.io  
[22] Apache Software Foundation, "Apache HBase Reference Guide," 2024. [Online]. Available: https://hbase.apache.org/book.html  
[23] M. Zaharia et al., "Resilient distributed datasets: A fault-tolerant abstraction for in-memory cluster computing," in *Proc. 9th USENIX NSDI*, 2012, pp. 15–28.  
[24] Apache Software Foundation, "Apache Spark Documentation," 2024. [Online]. Available: https://spark.apache.org/docs/latest/  
[25] S. Loria, "TextBlob: Simplified Text Processing," 2024. [Online]. Available: https://textblob.readthedocs.io  
[26] F. Pedregosa et al., "Scikit-learn: Machine learning in Python," *Journal of Machine Learning Research*, vol. 12, pp. 2825–2830, 2011.  
[27] Streamlit Inc., "Streamlit Documentation," 2024. [Online]. Available: https://docs.streamlit.io  
[28] Plotly Technologies Inc., "Plotly Open Source Graphing Libraries," 2024. [Online]. Available: https://plotly.com/python/  
[29] J. VanderPlas et al., "Altair: Interactive statistical visualizations for Python," *Journal of Open Source Software*, vol. 3, no. 32, p. 1057, 2018.  
[30] P. Yadav, L. Lucy, R. K. Mukhiya, and I. G, "BDATL: Indian Stock Market Sentiment & Price Anomaly Detection Engine," Symbiosis Institute of Technology Project Repository, 2026.  
[31] D. Araci, "FinBERT: Financial sentiment analysis with pre-trained language models," *arXiv preprint arXiv:1908.10063*, 2019.  
[32] J. Kreps, N. Narkhede, and J. Rao, "Kafka: A distributed messaging system for log processing," in *Proc. NetDB Workshop*, 2011, pp. 1–7.

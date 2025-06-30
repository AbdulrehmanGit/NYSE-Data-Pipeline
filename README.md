# 📈 NYSE Data Engineering Pipeline
A comprehensive end-to-end batch data pipeline for ingesting, transforming, and visualizing New York Stock Exchange (NYSE) data using a modern data engineering stack including Polygon.io, PySpark, PostgreSQL, Parquet, HDFS, Hive, and Power BI.

---

## 📂 Project Structure
```text
NYSE-Data-Pipeline/
│
├── ingestion/
│   ├── extract_tickers_json.py         # Calls Polygon API for ticker data
│   ├── loading_ticker_json.py          # Loads JSON to PostgreSQL (staging)
│   ├── extract_dailyprice_json.py      # Calls Polygon API for daily OHLCV data
│   ├── loading_daily_price_json.py     # Loads daily price JSON to PostgreSQL
│   └── utility.py                      # Shared helpers for ingestion
│
├── transformation/
│   ├── transform.py                    # Transforms and loads data warehouse tables
│   └── utility.py                      # Shared transformation utilities
│
├── export/
│   └── move_to_hdfs.py                 # Transfers Parquet files to HDFS
│
├── data/
│   └── prepared/                       # Parquet files ready for export
│       ├── dim_ticker/
│       ├── dim_date/
│       └── fact_daily_price/
│
├── logs/
│   └── pipeline.log                    # Pipeline execution log
│
├── .env                                # Environment config (excluded in .gitignore)
├── run_pipeline.py                     # Master orchestration script
└── README.md
```
---

## ⚙️ Technologies Used
- Python & PySpark – For extraction, transformation, and orchestration

- Polygon.io API – Real-time and historical financial market data

- PostgreSQL – Used as both staging and warehouse storage

- Parquet – Optimized file format for export and analysis

- HDFS (Cloudera) – Distributed file system for storage

- Hive – External tables built over HDFS for query access

- Power BI – Business intelligence and visualization

---

## 🔁 Project Workflow
### 1. Ingestion Layer
- extract_tickers_json.py and extract_dailyprice_json.py request data from the Polygon.io API.

- JSON files are saved locally under data/raw/.

### 2. Staging Layer
- loading_ticker_json.py and loading_daily_price_json.py load data into staging tables in PostgreSQL using psycopg2 or SQLAlchemy.

### 3. Transformation Layer
- transform.py reads staging data using PySpark, applies business rules and deduplication, and loads dimension and fact tables (dim_ticker, dim_date, fact_daily_price) into the data warehouse (PostgreSQL schema: nyse_dw).

- Data is also written to Parquet files for each table and saved under data/prepared/.

### 4. Export to HDFS
move_to_hdfs.py uses SCP and SSH to:

- Transfer Parquet files from local machine to Cloudera VM.

- Move those files into HDFS using hdfs dfs -put.

Paths:
/home/cloudera/NYSE/... → /user/cloudera/nyse/...

### 5. Hive External Tables
- External Hive tables are created pointing to HDFS directories. These tables are used by Power BI for analysis.

### 6. Power BI Dashboard
The final dataset is visualized in Power BI:

- Time-series trends (OHLCV)

- Price comparisons (Max Date vs. Prior Date)

- Volume trends

- Derived measures using DAX

---

## 🚀 How to Run
Set up your .env file with database and SSH configs.

Run the full pipeline using:

- bash
- Copy
- Edit
- python run_pipeline.py
- All steps will execute in sequence and log to logs/pipeline.log.

---

## 📊 Dashboard Highlights (Power BI)
- Daily Close Trends

- High vs. Low Price Ranges

- Close Price Comparison: Max Date vs. Prior Date

- Volume Movement Patterns

- Rolling averages (7-day, 30-day)

- Trend KPIs and DAX Measures


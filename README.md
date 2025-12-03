Real-Time Serverless Data Ingestion Pipeline
============================================

A fully automated **real-time market data ingestion pipeline** built using **AWS, Python, S3, Lambda, SNS, SQS, EventBridge, SQL Server, and Snowflake**.

* * * * *

 1. Problem --- Why This Pipeline Matters
-----------------------------------------

Modern financial, fintech, and analytics teams need **fresh, reliable, real-time market data**---updated every minute---to power decisions such as pricing, risk analysis, trading signals, and economic monitoring.

Manually downloading CSVs, calling APIs, or updating dashboards is slow and error-prone.\
**This pipeline removes all manual work** and delivers always-on, production-grade automation.

### Who needs this system?

-   **Trading & Quant Research Teams**\
    Require minute-level OHLCV for intraday and algorithmic strategies.

-   **Crypto Analysts & Web3 Teams**\
    Need continuous tracking of top cryptocurrency movements.

-   **Fintech & Payment Providers**\
    Depend on live FX rates for pricing, conversion, and settlement.

-   **Risk & Treasury Teams**\
    Monitor market volatility, currency exposure, and asset changes in real time.

-   **Data Engineering Teams**\
    Need a scalable blueprint for multi-source, event-driven ingestion.

This system solves the universal problem:\
 **How to ingest, clean, organize, route, and store high-frequency market data without any human effort?**

* * * * *

2. Architecture
-------------------

![Architecture Diagram](https://raw.githubusercontent.com/hameed980/Real-Time-Serverless-Data-Ingestion-Pipeline/main/architecture.png)

### **Data Flow Summary**

1.  **EventBridge** triggers ingestion every minute

2.  Lambdas fetch real-time market data

3.  Raw files are saved in S3

4.  S3 → SNS → SQS distributes notifications to processors

5.  Processing Lambdas transform + route to:

    -   **Snowflake** (stocks)

    -   **SQL Server** (FX)

    -   **Processed S3 zone** (crypto)

* * * * *

3. Data Sources
------------------

### **1\. Yahoo Finance (yfinance)**

-   Data: 1-minute OHLCV

-   Universe: S&P 500 symbols

-   Domain Use: Intraday trading, volatility analysis, market monitoring

### **2\. CoinMarketCap (Web Scraping)**

-   Data: Top 10 cryptocurrencies

-   Domain Use: Crypto market monitoring, pricing insights, sentiment indicators

### **3\. Open Exchange Rates API**

-   Data: USD-based FX rates

-   Domain Use: Payments, treasury, risk, cross-border pricing

* * * * *

4. Tech Stack
----------------

### **Programming**

-   Python

-   yfinance

-   BeautifulSoup

-   requests

### **AWS Services**

-   Lambda

-   S3

-   EventBridge

-   SNS

-   SQS

### **Data Storage**

-   Snowflake

-   SQL Server

-   S3 (Raw + Processed zones)

* * * * *

5. Pipeline Breakdown
------------------------

* * * * *

### **STEP 1 --- Ingestion Lambdas (Triggered Every Minute)**

| Lambda | Source | Trigger | Output |
| --- | --- | --- | --- |
| `lambda_yahoofinance` | Yahoo Finance | EventBridge | S3/raw/yahoofinance |
| `lambda_coinmarketcap` | CoinMarketCap | EventBridge | S3/raw/coinmarketcap |
| `lambda_openexchangerates` | OXR API | EventBridge | S3/raw/openexchangerates |

Each ingestion stores:

-   timestamp

-   metadata

-   symbols

-   status

* * * * *

### **STEP 2 --- S3 → SNS → SQS Routing**

1.  New raw data arrives in S3

2.  S3 sends event to SNS

3.  SNS fans out to 3 SQS queues

4.  Each SQS triggers its processing Lambda

Queues:

-   `yfinance-queue`

-   `cmc-queue`

-   `fxrates-queue`

* * * * *

### **STEP 3 --- Processing Lambdas (ETL/ELT)**

#### **Stock Data → Snowflake**

-   Cleans OHLCV

-   Validates types

-   Loads into Snowflake fact table

-   Supports analytical queries & storage optimization

#### **Crypto Data → Processed S3 Zone**

-   Cleans scraped data

-   Converts to JSON/Parquet

-   Stores in `processed/coinmarketcap/`

#### **FX Rates → SQL Server**

-   Inserts hourly/minute-level FX rates

-   Maintains audit + historical records

* * * * *

6. Final Outcomes
--------------------

-   **Real-time ingestion from 3 production-grade data sources**

-   **Fully automated workflow every 60 seconds**

-   **Event-driven, serverless architecture with zero manual operations**

-   **Clean RAW + PROCESSED data layers**

-   **Multi-database storage (Snowflake + SQL Server)**

-   **Reusable blueprint for any real-time data ingestion system**

* * * * *

7. Skills Demonstrated
-------------------------

### **Data Engineering**

-   Data lake design (raw → processed)

-   Multi-source ingestion

-   Real-time architecture

### **AWS Cloud**

-   Lambda orchestration

-   S3 event-driven workflows

-   SNS fan-out

-   SQS queue processing

### **Backend / Python**

-   API automation

-   Web scraping

-   Data cleaning + transformations

### **Databases**

-   Snowflake ELT

-   SQL Server ingestion

* * * * *

Author
---------

**Abdul Hameed**\
Cloud Data Engineer

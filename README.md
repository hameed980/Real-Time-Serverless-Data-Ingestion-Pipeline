Real-Time Serverless Data Ingestion Pipeline
============================================

A fully automated **real-time market data ingestion pipeline** built using **AWS, Python, S3, Lambda, SNS, SQS, EventBridge, SQL Server, and Snowflake**.

* * * * *

 1. Problem---Why This Pipeline Matters
---------------------------------------

Many data-driven teams require **fresh, clean, and automated market data** every minute to make fast and accurate decisions. Without an automated pipeline, analysts waste hours manually downloading files, cleaning data, and maintaining scripts.

This system solves that exact problem.

### Who needs this?

-   **Trading & Quant Teams:** Need minute-level OHLCV for intraday strategies.

-   **Portfolio Managers:** Require updated stock and FX rates for portfolio balancing and risk.

-   **Crypto Analysts & DeFi Teams:** Track top crypto movements and liquidity in real time.

-   **Fintech/Payments:** Use FX rates for pricing, cross-border payments, and hedging.

-   **BI & Analytics Teams:** Need standardized raw + processed data for dashboards and KPIs.

-   **Data Engineers:** Want a scalable, serverless pattern for multi-source real-time ingestion.

This pipeline provides an **always-on**, **zero-maintenance**, **serverless** solution to deliver clean, trusted market data every minute to any storage or analytics layer.

* * * * *

 2. Architecture
-------------------

![Architecture Diagram](https://raw.githubusercontent.com/hameed980/Real-Time-Serverless-Data-Ingestion-Pipeline/main/architecture.png)

### **Data Flow Overview**

1.  **EventBridge** triggers ingestion Lambdas every minute

2.  Lambdas fetch real-time data (stocks, crypto, FX)

3.  Raw files land in **S3** with metadata

4.  S3 events notify **SNS → SQS** subscribers

5.  Processing Lambdas transform and load into:

    -   **Snowflake** (stocks)

    -   **SQL Server** (FX rates)

    -   **Processed S3** (crypto top-10)

* * * * *

 3. Data Sources
------------------

### 1\. **Yahoo Finance (yfinance)**

-   Symbols: S&P 500

-   Granularity: **1-minute OHLCV**

-   Use cases: trading, risk, intraday dashboards

### 2\. **CoinMarketCap (Web Scraping)**

-   Top 10 cryptocurrencies (updated every minute)

-   Use cases: crypto research, DeFi, pricing insights

### 3\. **Open Exchange Rates API**

-   Live global FX rates (USD-based)

-   Use cases: fintech, cross-border transactions, pricing engines

* * * * *

 4. Tech Stack
----------------

### **Languages & Libraries**

-   Python

-   yfinance

-   BeautifulSoup + requests

### **AWS Services**

-   Lambda

-   S3

-   SNS

-   SQS

-   EventBridge

### **Databases**

-   Snowflake

-   SQL Server

* * * * *

 5. Pipeline Breakdown
------------------------

### **Step 1 --- Ingestion Lambda (runs every minute)**

| Function | Source | Trigger | Output |
| --- | --- | --- | --- |
| `lambda_yahoofinance` | Yahoo Finance | EventBridge | S3/raw/yahoofinance |
| `lambda_coinmarketcap` | CoinMarketCap | EventBridge | S3/raw/coinmarketcap |
| `lambda_openexchangerates` | OpenExchangeRates | EventBridge | S3/raw/openexchangerates |

Each file contains:\
✔ timestamps\
✔ source name\
✔ metadata (status, symbols, etc.)

* * * * *

### **Step 2 --- S3 → SNS → SQS Routing**

1.  Raw file lands in S3

2.  S3 event → SNS notification

3.  SNS routes to dedicated SQS queues

4.  SQS triggers processing Lambdas

Queues:

-   `yfinance-queue`

-   `cmc-queue`

-   `fxrates-queue`

* * * * *

### **Step 3 --- Processing Lambdas (ETL/ELT)**

####  **Yahoo Finance → Snowflake**

-   Validates schema

-   Normalizes OHLCV

-   Loads into Snowflake fact table

####  **CoinMarketCap → Processed S3**

-   Cleans scraped crypto data

-   Stores in `/processed/coinmarketcap/` as JSON

####  **OpenExchangeRates → SQL Server**

-   Inserts currency rates

-   Maintains audit history

-   Used for BI dashboards & reporting

* * * * *

6. Final Outputs
-------------------

Your pipeline delivers:

-   **Real-time data every minute**

-   **Raw + processed zones** (analytics-ready)

-   **Reliable, event-driven architecture**

-   **Zero maintenance (fully serverless)**

-   **Scalable pattern to ingest ANY real-time source**

-   **Business-ready data** for trading, crypto, payments, and BI teams

* * * * *

 7. Skills Demonstrated
-------------------------

### **Data Engineering**

-   Real-time ingestion

-   ETL/ELT design

-   Distributed message processing

### **Cloud Engineering**

-   Serverless compute

-   Pub/sub and Queue patterns

-   Infrastructure automation logic

### **Backend/Data**

-   Python automation

-   API integration + Web scraping

-   Working with Snowflake + SQL Server

* * * * *

 Author
---------

**Abdul Hameed**\
Cloud Data Engineer

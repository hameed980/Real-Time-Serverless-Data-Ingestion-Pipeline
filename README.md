**Real-Time Serverless Data Ingestion Pipeline**
================================================

An end-to-end automated data engineering project using **AWS, Python, S3, Lambda, EventBridge, SNS, SQS, Snowflake, SQL Server, and Power BI**.

* * * * *

 **1\. Project Overview**
===========================

Modern businesses need **live, accurate, and automatically refreshed market data** for decision-making.\
The problem:\
**How can we continuously collect real-time stock, cryptocurrency, and currency exchange data from multiple APIs, process it, store it properly, and make it ready for analytics---without manual work?**

To solve this, I built a **serverless real-time data ingestion and analytics pipeline** that:

-   Ingests live data every minute

-   Processes and routes it intelligently

-   Stores it in Snowflake, SQL Server, and S3

-   Feeds Power BI dashboards for insights

This system handles three external sources:

-   **Yahoo Finance** -- S&P 500 OHLCV

-   **CoinMarketCap** -- Top 10 cryptocurrencies

-   **OpenExchangeRates** -- Live currency FX rates

The final result is a **fully automated, scalable, production-style pipeline** suitable for real analytics, reporting, and monitoring.

* * * * *

 **2\. Architecture Diagram**
===============================

`[Yahoo Finance]   [CoinMarketCap]   [OpenExchangeRates]
       |                 |                 |
       |   AWS Lambda Ingestion Functions  |
       |         (Trigger: Every Minute)   |
       |                 |                 |
                   S3 RAW DATA LAKE
                      (JSON Files)

            S3 Event → SNS Notification
            SNS → SQS Standard Queues
       (yfinance-queue | cmc-queue | fxrates-queue)

               SQS → Lambda Processing

       -----------------------------------------------
       |                   |                         |
Transform + Load      Transform + Store      Transform + Load
   to Snowflake        to S3 Processed          to SQL Server
       -----------------------------------------------

        Power BI Dashboards (SQL Server + Snowflake)`

* * * * *

 **3\. Data Sources**
========================

### **1\. Yahoo Finance**

-   Uses `yfinance` Python library

-   Fetches **minute-level OHLCV** for all S&P 500 symbols

### **2\. CoinMarketCap**

-   Web scraped with **BeautifulSoup + requests**

-   Extracts Top 10 cryptocurrencies

### **3\. Open Exchange Rates**

-   REST API call using `requests`

-   Provides **live USD-based FX rates**

* * * * *

**4\. Tech Stack**
=====================

### **Languages & Libraries**

-   Python

-   yfinance

-   BeautifulSoup

-   requests

### **AWS Services**

-   Lambda

-   S3

-   SNS

-   SQS (Standard)

-   EventBridge

### **Other Tools**

-   Snowflake

-   SQL Server

-   Power BI

* * * * *

**5\. Pipeline Breakdown**
=============================

* * * * *

**STEP 1 --- Data Ingestion (AWS Lambda + EventBridge)**
------------------------------------------------------

Each source has a dedicated Lambda function:

| Function Name | Source | Trigger | Output |
| --- | --- | --- | --- |
| lambda_yahoofinance | Yahoo Finance | Every minute | S3/raw/yahoofinance |
| lambda_coinmarketcap | CoinMarketCap | Every minute | S3/raw/coinmarketcap |
| lambda_openexchangerates | OXR API | Every minute | S3/raw/openexchangerates |

Each file includes metadata:

-   timestamp

-   source

-   status

-   symbol (if available)

* * * * *

**STEP 2 --- S3 Event → SNS → SQS Routing**
-----------------------------------------

Whenever new data lands in S3:

1.  **S3 triggers SNS**

2.  **SNS routes message to 3 SQS queues (by metadata)**

3.  Processing Lambdas read SQS messages

### **Why Standard SQS Instead of FIFO?**

SNS does **not** allow direct publishing to FIFO queues without additional configurations (MessageGroupId, deduplication, FIFO SNS topic).\
To keep the pipeline simple and fully compatible:

**I used Standard SQS queues for all three pipelines.**

This ensures reliable, fast delivery without restrictions.

Queues used:

-   **yfinance-queue**

-   **cmc-queue**

-   **fxrates-queue**

* * * * *

**STEP 3 --- Data Processing (Lambda)**
-------------------------------------

### **Yahoo Finance → Snowflake**

-   Reads OHLCV messages from SQS

-   Cleans + prepares data

-   Loads into Snowflake table

-   Supports BI-ready stock analysis

### **CoinMarketCap → Processed S3 Zone**

-   Cleans crypto data

-   Stores in Parquet/JSON

-   Output folder:

    `s3://data-pipeline/processed/coinmarketcap/`

### **OpenExchangeRates → SQL Server**

-   Extracts FX metrics

-   Loads data into SQL Server

-   Used directly for Power BI dashboards

* * * * *

 **6\. Business Intelligence Layer (Power BI)**
=================================================

Connected to both:

-   **SQL Server**

-   **Snowflake**

Dashboards include:

-   Stock price movements

-   Crypto market changes

-   FX rate patterns

-   Real-time trends

-   Technical indicators

Updates every minute.

* * * * *

 **7\. Final Outcomes**
=========================

-   Fully automated real-time ETL/ELT pipeline

-   No manual intervention required

-   Scalable AWS serverless architecture

-   Clean storage zones: raw → processed → analytics

-   Ready for dashboards and insights

-   Demonstrates end-to-end production-style data engineering

* * * * *

**8\. Skills Demonstrated**
==============================

### **Data Engineering**

-   Real-time ingestion

-   Serverless ETL design

-   S3 data lake layout

-   Event-driven architecture

### **AWS**

-   Lambda

-   EventBridge

-   SNS + SQS

-   S3

### **Programming**

-   Python automation

-   API calling

-   Web scraping

-   Data transformations

### **Analytics**

-   Snowflake modeling

-   SQL Server transformations

-   Power BI visualizations

* * * * *

🙌 **Author**
=============

**Abdul Hameed**\
Cloud Data Engineer

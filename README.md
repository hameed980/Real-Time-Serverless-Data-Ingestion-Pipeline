Real-Time Serverless Data Ingestion Pipeline
============================================

[](https://github.com/hameed980/Real-Time-Serverless-Data-Ingestion-Pipeline#real-time-serverless-data-ingestion-pipeline)

**An end-to-end automated data engineering project using AWS, Python, S3, Lambda, EventBridge, SNS, SQS, Snowflake, SQL Server, and Power BI.**

* * * * *

📌 **1\. Project Overview**
---------------------------

This project solves a common real-world problem:

> **How can we automatically collect live stock, cryptocurrency, and foreign exchange data every minute from multiple sources, process it in real-time, store it properly, and make it ready for analytics --- without manual effort?**

To address this, I built a **fully automated, serverless real-time data pipeline** that ingests → processes → stores → and visualizes market data from three different external sources:

-   **Yahoo Finance** (S&P 500 OHLCV data)

-   **CoinMarketCap** (Top 10 cryptocurrencies)

-   **Open Exchange Rates** (Live currency rates)

The outcome is a production-style data system suitable for analytics, reporting, and insights.

* * * * *

🧱 **2\. Architecture Diagram**
===============================

 `[Yahoo Finance]       [CoinMarketCap]        [OpenExchangeRates]
          |                       |                        |
   [AWS Lambda - Ingestion]  [AWS Lambda]           [AWS Lambda]
          |                       |                        |
     triggered every minute using Amazon EventBridge (CRON rule)
          |                       |                        |
                  ====>  S3 RAW DATA LAKE  <====
                            (with metadata)

                     S3 Event → SNS Topic
                   SNS → SQS FIFO Queues
              (yfinance.fifo | cmc.fifo | fxrates.fifo)

                     SQS FIFO → Lambda Processing
            -------------------------------------------------
            |                   |                          |
      Transform + Load     Transform + Store         Transform + Load
          to Snowflake        to S3 Processed             to SQL Server
            -------------------------------------------------

                Power BI Dashboards (SQL Server + Snowflake)`

* * * * *

🗂️ **3\. Data Sources**
========================

### **1\. Yahoo Finance (yfinance library)**

-   Collects **minute-level OHLCV** for all **S&P 500 symbols**

-   Ideal for stock trend analysis

### **2\. CoinMarketCap (Web Scraping)**

-   Retrieves Top 10 cryptocurrencies by market cap

-   Data extracted with **BeautifulSoup + requests**

### **3\. Open Exchange Rates (REST API)**

-   Live USD-based currency FX rates

-   Requires App ID from OpenExchangeRates

* * * * *

⚙️ **4\. Tech Stack**
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

-   SQS FIFO

-   EventBridge

### **Other Tools**

-   Snowflake

-   SQL Server

-   Power BI

* * * * *

🚀 **5\. Pipeline Breakdown**
=============================

**STEP 1 --- Data Ingestion (AWS Lambda + EventBridge)**
------------------------------------------------------

Each data source has its own Lambda:

| Lambda Function | Source | Trigger | Output |
| --- | --- | --- | --- |
| lambda_yahoofinance | Yahoo Finance | Every minute | OHLCV JSON → S3/raw/yahoofinance |
| lambda_coinmarketcap | CoinMarketCap | Every minute | Crypto JSON → S3/raw/coinmarketcap |
| lambda_openexchangerates | OXR API | Every minute | FX JSON → S3/raw/openexchangerates |

All raw files include metadata:

-   timestamp

-   symbol (if applicable)

-   source name

-   API response status

* * * * *

**STEP 2 --- S3 Event → SNS → SQS Routing**
-----------------------------------------

1.  S3 triggers SNS whenever a new file arrives

2.  SNS uses metadata filtering to route events to one of the 3 FIFO queues

3.  Ensures **ordered**, **exactly-once** delivery

Queues:

-   yahoo-finance-queue.fifo

-   coinmarketcap-queue.fifo

-   openexchangerates-queue.fifo

* * * * *

**STEP 3 --- Processing & Loading (Lambda)**
------------------------------------------

### **Yahoo Finance → Snowflake**

-   Reads SQS messages

-   Parses OHLCV

-   Loads into Snowflake table

-   Enables BI-ready stock analysis

* * * * *

### **CoinMarketCap → Processed S3 Zone**

-   Cleans data

-   Stores transformed JSON/Parquet in:

`s3://data-hackathon-smit-yourname/processed/coinmarketcap/`

* * * * *

### **Open Exchange Rates → SQL Server**

-   Extracts currency FX details

-   Loads into SQL Server table

-   Used for Power BI dashboards

* * * * *

📊 **6\. Business Intelligence Layer (Power BI)**
=================================================

Connected Power BI to:

-   **SQL Server**

-   **Snowflake**

Created dashboards showing:

-   S&P 500 stock movements

-   Crypto market trends

-   FX rate fluctuations

-   Trend lines and indicators

-   Minute-level updating visuals

* * * * *

🎯 **7\. Final Outcomes**
=========================

-   **End-to-end automated ETL/ELT system**

-   **Real-time pipeline updating every minute**

-   **Scalable, serverless cloud architecture**

-   **Clean data ready for BI and analytics**

-   **Professional-level data engineering workflow**

This project simulates how data teams ingest, transform, and visualize data at scale.

* * * * *

🧪 **8\. Skills Demonstrated**
==============================

### **Data Engineering**

-   Serverless ETL pipelines

-   Real-time processing

-   S3 data lake design

-   Metadata-driven routing

### **Cloud**

-   Lambda

-   EventBridge

-   SNS / SQS FIFO

-   S3

### **Programming**

-   Python

-   API consumption

-   Web scraping

-   Data transformation

### **Analytics**

-   Snowflake & SQL Server modeling

-   Power BI dashboards

* * * * *

🙌 **Author**
=============

**Abdul Hameed**\
Cloud Data Engineer\
---

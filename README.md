#  Real-Time Serverless Data Ingestion Pipeline

An end-to-end automated data engineering project using AWS, Python, S3, Lambda, EventBridge, SNS, SQS, Snowflake, SQL Server, and Power BI.
---

## 📚 Table of Contents

- [Overview](#overview)
- [Data Sources](#data-sources)
- [AWS Services Used](#aws-services-used)
- [S3 Storage Format](#s3-storage-format)
- [Project Structure](#project-structure)
- [Setup & Deployment](#setup--deployment)

---

## 1.  Project Overview

This project solves a common real-world problem:

-- How can we automatically collect live stock, cryptocurrency, and foreign exchange data every minute from multiple sources, process it in real-time, store it properly, and make it ready for analytics — without manual effort?

-- To address this, I built a fully automated, serverless real-time data pipeline that ingests → processes → stores → and visualizes market data from three different external sources:

- Yahoo Finance (S&P 500 OHLCV data)
- CoinMarketCap (Top 10 cryptocurrencies)
- Open Exchange Rates (Live currency rates)
- The outcome is a production-style data system suitable for analytics, reporting, and insights.

---

## 2. Architecture Diagram

## 3. Data Sources

### 1. Yahoo Finance (yfinance library)
-Collects minute-level OHLCV((Open, High, Low, Close, Volume)) for all S&P 500 symbols
-Ideal for stock trend analysis

### 2. CoinMarketCap
- Retrieves Top 10 cryptocurrencies by market cap
- Data extracted with BeautifulSoup + requests

### 3. Open Exchange Rates (REST API)
- Live USD-based currency FX rates
- Requires App ID from OpenExchangeRates
---

## 4. Tech Stack

**Languages & Libraries**
- Python
- yfinance
- BeautifulSoup
- requests

**AWS Services**
- Lambda
- S3
- SNS
- SQS FIFO
- EventBridge

**Other Tools**
- Snowflake
- SQL Server
- Power BI

---

## 5 . Pipeline Breakdown

**STEP 1 — Data Ingestion (AWS Lambda + EventBridge)**
- Each data source has its own Lambda:
| Lambda Function          | Source        | Trigger      | Output                             |
| ------------------------ | ------------- | ------------ | ---------------------------------- |
| lambda_yahoofinance      | Yahoo Finance | Every minute | OHLCV JSON → S3/raw/yahoofinance   |
| lambda_coinmarketcap     | CoinMarketCap | Every minute | Crypto JSON → S3/raw/coinmarketcap |
| lambda_openexchangerates | OXR API       | Every minute | FX JSON → S3/raw/openexchangerates |

All raw files include metadata:

- timestamp
- symbol (if applicable)
- ource name
- API response status

**STEP 2 — S3 Event → SNS → SQS Routing**

1- S3 triggers SNS whenever a new file arrives
2- SNS uses metadata filtering to route events to one of the 3 FIFO queues
3- Ensures ordered, exactly-once delivery

Queues:
- yahoo-finance-queue.fifo
- coinmarketcap-queue.fifo
- openexchangerates-queue.fifo

**STEP 3 — Processing & Loading (Lambda)**

Yahoo Finance → Snowflake:
- Reads SQS messages
- Parses OHLCV
- Loads into Snowflake table
- Enables BI-ready stock analysis
---

---


## 5 . 

---

---

## 5 . 

---

---

## 5 . 

---

---

## 5 . 

---

---

## 5 . 

---

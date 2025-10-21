import yfinance as yf
import pandas as pd
import boto3
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pytz
import io
import time
import logging  #  Added logging module

#  Setup logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
BUCKET = 'data-hackathon-smit-ah'
SOURCE = 'yahoofinance'

# Step 1: Get S&P 500 symbols from Wikipedia using BeautifulSoup
def get_sp500_symbols():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", {"id": "constituents"})
    symbols = []
    for row in table.tbody.find_all("tr")[1:]:
        cols = row.find_all("td")
        symbols.append(cols[0].text.strip())
    return symbols

# Lambda Handler
def lambda_handler(event, context):
    logger.info("Lambda triggered at: %s", datetime.utcnow().isoformat())
    
    now = datetime.now(pytz.UTC)
    date_folder = now.strftime(f"raw/{SOURCE}/%Y/%m/%d")
    file_name = now.strftime("%H%M") + ".csv"
    s3_key = f"{date_folder}/{file_name}"

    logger.info("Preparing to fetch data for S&P 500 symbols")
    symbols = get_sp500_symbols()[:6]  # Limit to avoid rate limits
    logger.info("Symbols selected: %s", symbols)

    all_data = []

    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period="1d", interval="1m")
            df.reset_index(inplace=True)

            df["symbol"] = symbol
            df["source"] = SOURCE
            df["ingest_timestamp"] = now.isoformat()
            df["status"] = "success"
            all_data.append(df)

            logger.info("Fetched data for symbol: %s", symbol)
        except Exception as e:
            logger.error("Failed to fetch data for %s: %s", symbol, str(e))
            all_data.append(pd.DataFrame([{
                "Datetime": now,
                "Open": None,
                "High": None,
                "Low": None,
                "Close": None,
                "Volume": None,
                "symbol": symbol,
                "source": SOURCE,
                "ingest_timestamp": now.isoformat(),
                "status": f"error: {e}"
            }]))
        time.sleep(1)  # Respect Yahoo Finance rate limits

    result_df = pd.concat(all_data)
    buffer = io.StringIO()
    result_df.to_csv(buffer, index=False)

    logger.info("Uploading CSV to S3: s3://%s/%s", BUCKET, s3_key)
    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=buffer.getvalue())

    logger.info("Upload completed successfully.")

    return {
        "statusCode": 200,
        "body": f"Data uploaded to s3://{BUCKET}/{s3_key}"
    }

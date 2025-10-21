import requests
from bs4 import BeautifulSoup
import boto3
from io import StringIO
import datetime
import csv
import logging

#  Set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event=None, context=None):
    logger.info(" CoinMarketCap Lambda triggered.")

    url = "https://coinmarketcap.com/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    try:
        logger.info(" Requesting CoinMarketCap page...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except Exception as e:
        logger.error(f" Request failed: {e}")
        raise

    soup = BeautifulSoup(response.text, 'html.parser')
    tbody = soup.find('tbody')
    if not tbody:
        logger.error(" Table body not found on the page.")
        raise Exception("Table body not found")

    rows = tbody.find_all('tr')
    if not rows:
        logger.error(" No rows found in the crypto table.")
        raise Exception("No rows found in the crypto table")

    data = []
    timestamp = datetime.datetime.utcnow().isoformat()
    logger.info(" Parsing top 10 cryptocurrency rows...")

    for row in rows[:10]:
        cols = row.find_all('td')
        if len(cols) < 10:
            continue

        name_col = cols[2]
        name_tag = name_col.find('p')
        name = name_tag.text.strip() if name_tag else ""
        symbol_tag = name_col.find_all('p')
        symbol = symbol_tag[1].text.strip() if len(symbol_tag) > 1 else ""

        price = cols[3].text.strip()
        change_1h = cols[4].text.strip()
        change_24h = cols[5].text.strip()
        change_7d = cols[6].text.strip()
        market_cap = cols[7].text.strip()
        volume_24h = cols[8].text.strip()
        circulating_supply = cols[9].text.strip()

        data.append({
            "timestamp": timestamp,
            "source": "coinmarketcap",
            "name": name,
            "symbol": symbol,
            "price": price,
            "1h %": change_1h,
            "24h %": change_24h,
            "7d %": change_7d,
            "market_cap": market_cap,
            "volume_24h": volume_24h,
            "circulating_supply": circulating_supply,
            "status": "success"
        })

    if not data:
        logger.warning(" No data parsed. Exiting.")
        return {"message": "No data to write."}

    # 🔁 Convert to CSV
    csv_buffer = StringIO()
    fieldnames = list(data[0].keys())
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)

    now = datetime.datetime.utcnow()
    bucket = "data-hackathon-smit-ah"  # Replace with your actual bucket name
    time_prefix = f"{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}{now.minute:02d}"
    key = f"raw/coinmarketcap/{time_prefix}.csv"

    s3 = boto3.client("s3")
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        logger.info(f" File uploaded to: s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f" Failed to upload CSV to S3: {e}")
        raise

    return {
        "statusCode": 200,
        "body": f"Top 10 coins saved to s3://{bucket}/{key}"
    }

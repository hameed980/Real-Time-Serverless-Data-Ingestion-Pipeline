import requests
import boto3
from io import StringIO
import datetime
import csv
import logging  #  Logging added

# Configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event=None, context=None):
    logger.info("Open Exchange Rates Lambda triggered.")

    # Fetch from Open Exchange Rates JSON API
    API_KEY = "f2944e17e65f449cac4c97f3e5c4932e"  #  Add your key here
    url = f"https://openexchangerates.org/api/latest.json?app_id={API_KEY}"

    logger.info("Requesting data from Open Exchange Rates...")
    response = requests.get(url)
    if response.status_code != 200:
        logger.error("Failed to fetch data. Status code: %s, body: %s", response.status_code, response.text)
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}, body: {response.text}")

    json_data = response.json()
    rates = json_data.get("rates", {})
    timestamp = datetime.datetime.utcnow().isoformat()

    logger.info("Fetched %d currency rates.", len(rates))

    # Prepare CSV rows
    data = []
    for currency, value in rates.items():
        data.append({
            "timestamp": timestamp,
            "source": "openexchangerates",
            "currency": currency,
            "rate_to_usd": value,
            "status": "success"
        })

    # Write CSV to memory
    csv_buffer = StringIO()
    fieldnames = ["timestamp", "source", "currency", "rate_to_usd", "status"]
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)

    # Save to S3
    now = datetime.datetime.utcnow()
    bucket = "data-hackathon-smit-ah"  # your bucket name
    s3_key = f"raw/openexchangerates/{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}{now.minute:02d}.csv"

    logger.info("Uploading file to S3: s3://%s/%s", bucket, s3_key)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )

    logger.info("Upload complete.")

    return {
        "statusCode": 200,
        "body": f"Exchange rates saved to s3://{bucket}/{s3_key}"
    }

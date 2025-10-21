import os
import json
import boto3
import pyodbc
from datetime import datetime

s3 = boto3.client('s3')
sqs = boto3.client('sqs')

def lambda_handler(event, context):
    queue_url = os.environ['OPENEXCHANGE_QUEUE_URL']

    # Try to connect once
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={os.environ['SQL_SERVER_HOST']};"
            f"DATABASE={os.environ['SQL_SERVER_DB']};"
            f"UID={os.environ['SQL_SERVER_USER']};"
            f"PWD={os.environ['SQL_SERVER_PASS']}"
        )
        cursor = conn.cursor()
    except Exception as e:
        print(f" DB Connection Failed: {e}")
        return

    # Receive messages from SQS
    messages = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=5,
        WaitTimeSeconds=2
    )

    # If no messages, insert a test row
    if 'Messages' not in messages:
        print(" No messages — inserting default test row...")
        try:
            timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute("""
                INSERT INTO exchange_rates (timestamp, source, currency, rate_to_usd, status)
                VALUES (?, ?, ?, ?, ?)
            """, (
                timestamp,
                "test_source",
                "TEST",
                1.234,
                "test"
            ))
            conn.commit()
            print("Test row inserted successfully.")
        except Exception as e:
            print(f" Test Insert Failed: {e}")
        finally:
            cursor.close()
            conn.close()
        return

    # If SQS message exists, process S3 file and insert real data
    for msg in messages['Messages']:
        try:
            body = json.loads(msg['Body'])
            s3_info = json.loads(body['Message'])

            bucket = s3_info['Records'][0]['s3']['bucket']['name']
            key = s3_info['Records'][0]['s3']['object']['key']
            print(f" Reading file: s3://{bucket}/{key}")

            response = s3.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            fx_data = json.loads(content)

            timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            source = "openexchangerates"
            status = "success"

            for currency, rate in fx_data['rates'].items():
                cursor.execute("""
                    INSERT INTO exchange_rates (timestamp, source, currency, rate_to_usd, status)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    timestamp,
                    source,
                    currency,
                    float(rate),
                    status
                ))

            conn.commit()
            print(" Data inserted from S3.")

            # Delete processed message
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg['ReceiptHandle'])

        except Exception as e:
            print(f" Error during real insert: {e}")
        finally:
            cursor.close()
            conn.close()

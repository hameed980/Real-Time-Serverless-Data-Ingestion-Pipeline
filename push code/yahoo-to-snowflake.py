import os
import json
import boto3
import csv
import io
import snowflake.connector

# AWS clients
sqs = boto3.client('sqs')
s3 = boto3.client('s3')

# Environment variable
queue_url = os.environ['YAHOO_QUEUE_URL']

def lambda_handler(event, context):
    processed_files = []
    total_inserted_rows = 0

    try:
        # Get messages from SQS
        messages = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=5,
            WaitTimeSeconds=2
        )

        if 'Messages' not in messages:
            print("No messages found.")
            return {
                "statusCode": 200,
                "message": "No SQS messages received."
            }

        for msg in messages['Messages']:
            try:
                # Parse S3 event info from SQS message
                body = json.loads(msg['Body'])
                s3_info = json.loads(body['Message'])
                bucket = s3_info['Records'][0]['s3']['bucket']['name']
                key = s3_info['Records'][0]['s3']['object']['key']

                print(f"Processing file: s3://{bucket}/{key}")

                # Read CSV from S3
                response = s3.get_object(Bucket=bucket, Key=key)
                content = response['Body'].read().decode('utf-8')

                # Parse CSV content
                csv_reader = csv.DictReader(io.StringIO(content))
                insert_data = []
                for row in csv_reader:
                    try:
                        insert_data.append((
                            row['symbol'],
                            row['Datetime'].split(' ')[0],  # Extract date only
                            float(row['Open']),
                            float(row['High']),
                            float(row['Low']),
                            float(row['Close']),
                            int(float(row['Volume']))
                        ))
                    except Exception as row_error:
                        print(f"Skipping row due to error: {row_error}")
                        continue

                # Connect to Snowflake
                conn = snowflake.connector.connect(
                    user=os.environ['SNOWFLAKE_USER'],
                    password=os.environ['SNOWFLAKE_PASSWORD'],
                    account=os.environ['SNOWFLAKE_ACCOUNT'],
                    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
                    database=os.environ['SNOWFLAKE_DATABASE'],
                    schema=os.environ['SNOWFLAKE_SCHEMA']
                )
                cursor = conn.cursor()

                # Bulk insert using executemany
                if insert_data:
                    cursor.executemany("""
                        INSERT INTO ohlcv_data (symbol, trade_date, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, insert_data)
                    print(f"Inserted {len(insert_data)} rows into Snowflake.")
                    total_inserted_rows += len(insert_data)
                else:
                    print("No valid rows to insert.")

                cursor.close()
                conn.close()

                # Delete message from SQS
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg['ReceiptHandle'])
                processed_files.append(f"s3://{bucket}/{key}")

            except Exception as file_error:
                print(f"Error processing message: {file_error}")
                continue

        return {
            "statusCode": 200,
            "message": f"Lambda processed {len(processed_files)} file(s), inserted {total_inserted_rows} row(s) into Snowflake.",
            "files": processed_files
        }

    except Exception as e:
        print("Lambda failed:", e)
        raise

import os
import json
import logging
import uuid
import boto3
import tempfile
import math
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

from TableExtract import processOutputData  # Assumes this is available via Lambda layer
from OutputFile_pb2 import OutputFile       # Assumes this is available via Lambda layer
import botocore.exceptions
from google.protobuf.json_format import Parse

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Global clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Config
SCHEMA_PREFIX = os.getenv('SCHEMA_PREFIX', 'my_schema')
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '5'))

def lambda_handler(event, context):
    logger.info("🚀 Lambda execution started")
    for record in event['Records']:
        try:
            body = json.loads(record['body'])
            bucket = body['bucket']
            key = body['key']

            logger.info(f"📥 Downloading file from s3://{bucket}/{key}")
            with tempfile.NamedTemporaryFile() as tmp_file:
                s3.download_file(bucket, key, tmp_file.name)
                output_file = OutputFile()
                with open(tmp_file.name, 'rb') as f:
                    output_file.ParseFromString(f.read())

            logger.info("🧠 Parsing and processing OutputData")
            table_data: Dict[str, List[Dict]] = processOutputData(output_file)

            results = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                for table_name, rows in table_data.items():
                    results.append(executor.submit(handle_table_insert, table_name, rows))

            for result in results:
                result.result()  # Raise any exceptions

        except Exception as e:
            logger.exception(f"❌ Error processing record: {e}")

    logger.info("✅ Lambda execution completed")

def handle_table_insert(table_name: str, rows: List[Dict]):
    if not rows:
        logger.warning(f"⚠️ No rows to insert for table {table_name}")
        return

    full_table_name = f"{SCHEMA_PREFIX}_{table_name}"
    logger.info(f"🧾 Processing table {full_table_name} with {len(rows)} rows")

    # Create table if it doesn't exist
    ensure_table_exists(full_table_name, rows)

    table = dynamodb.Table(full_table_name)
    success_count = 0
    skipped_count = 0
    batch_size = 25

    def sanitize_value(value):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        if isinstance(value, float):
            return Decimal(str(value))
        return value

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        with table.batch_writer(overwrite_by_pkeys=['id']) as batch_writer:
            for item in batch:
                sanitized = {k: sanitize_value(v) for k, v in item.items()}
                if 'id' not in sanitized:
                    sanitized['id'] = str(uuid.uuid4())
                try:
                    batch_writer.put_item(Item=sanitized)
                    success_count += 1
                except Exception as e:
                    skipped_count += 1
                    logger.warning(f"⚠️ Failed to insert item: {e}")

    logger.info(f"📊 Inserted {success_count} rows (skipped {skipped_count}) into {full_table_name}")

def ensure_table_exists(table_name: str, sample_rows: List[Dict]):
    try:
        dynamodb.meta.client.describe_table(TableName=table_name)
        return
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        logger.info(f"🛠️ Table {table_name} does not exist, creating...")

    # Determine key schema
    if 'id' in sample_rows[0]:
        key_schema = [{'AttributeName': 'id', 'KeyType': 'HASH'}]
        attr_def = [{'AttributeName': 'id', 'AttributeType': 'S'}]
    else:
        key_schema = [{'AttributeName': 'uuid', 'KeyType': 'HASH'}]
        attr_def = [{'AttributeName': 'uuid', 'AttributeType': 'S'}]

    try:
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attr_def,
            BillingMode='PAY_PER_REQUEST'
        )
        logger.info(f"✅ Created table {table_name}")
        dynamodb.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    except Exception as e:
        logger.error(f"❌ Failed to create table {table_name}: {e}")

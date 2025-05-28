import boto3
import os
import json
import io
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from collections import defaultdict

BUCKET = os.environ.get("DEST_BUCKET")
PREFIX = os.environ.get("DEST_PREFIX", "dynamodb")
MAX_ROWS_PER_FILE = int(os.environ.get("MAX_ROWS", "5000"))

s3 = boto3.client("s3")

def extract_table_name(event_arn):
    return event_arn.split(":table/")[1].split("/")[0]

def parse_ddb_value(value):
    if 'S' in value:
        return value['S']
    elif 'N' in value:
        num = value['N']
        return int(num) if num.isdigit() else float(num)
    elif 'BOOL' in value:
        return value['BOOL']
    elif 'NULL' in value:
        return None
    elif 'M' in value:
        return {k: parse_ddb_value(v) for k, v in value['M'].items()}
    elif 'L' in value:
        return [parse_ddb_value(v) for v in value['L']]
    else:
        return str(value)

def flatten_ddb_json(ddb_item):
    return {k: parse_ddb_value(v) for k, v in ddb_item.items()}

def lambda_handler(event, context):
    partitioned_rows = defaultdict(list)

    for record in event['Records']:
        if record['eventName'] not in ('INSERT', 'MODIFY'):
            continue

        new_image = record['dynamodb'].get('NewImage')
        if not new_image or 'calc_dt' not in new_image:
            continue

        row = flatten_ddb_json(new_image)
        table = extract_table_name(record['eventSourceARN'])
        calc_dt = row.get('calc_dt')
        if not calc_dt:
            continue

        key = f"{table}|{calc_dt}"
        partitioned_rows[key].append(row)

    for key, rows in partitioned_rows.items():
        table, calc_dt = key.split('|')
        for i in range(0, len(rows), MAX_ROWS_PER_FILE):
            batch = rows[i:i + MAX_ROWS_PER_FILE]
            table_arrow = pa.Table.from_pylist(batch)
            buf = io.BytesIO()
            pq.write_table(table_arrow, buf)
            buf.seek(0)

            file_name = f"{uuid.uuid4()}.parquet"
            s3_key = f"{PREFIX}/{table}/calc_dt={calc_dt}/{file_name}"
            s3.upload_fileobj(buf, BUCKET, s3_key)
            print(f"✅ Wrote {len(batch)} rows to s3://{BUCKET}/{s3_key}")

# DynamoDB Stream to Parquet on S3 (Lambda Deployment)

This project sets up a serverless pipeline that streams DynamoDB changes to S3 in Parquet format, partitioned by `calc_dt`.

## ✅ Components
- AWS Lambda (Python) that writes Parquet files
- PyArrow Layer (deployed separately)
- Bash script to enable DynamoDB Streams and link to Lambda

## 🚀 Setup Instructions

### 1. Install Dependencies for Layer (in Docker)
```bash
mkdir -p layer/python
docker run --rm -v "$PWD/layer/python":/mnt amazonlinux   bash -c "yum install -y gcc python3 python3-devel && \
           pip3 install pyarrow boto3 --target /mnt"

cd layer && zip -r layer.zip python && cd ..
```

### 2. Deploy Lambda Layer
```bash
aws lambda publish-layer-version \
  --layer-name ddb-pyarrow-layer \
  --zip-file fileb://layer/layer.zip \
  --compatible-runtimes python3.11
```

### 3. Deploy Lambda Function
```bash
zip lambda.zip lambda_function.py

aws lambda create-function \
  --function-name ddb-stream-to-parquet \
  --zip-file fileb://lambda.zip \
  --handler lambda_function.lambda_handler \
  --runtime python3.11 \
  --timeout 900 \
  --memory-size 512 \
  --role arn:aws:iam::<ACCOUNT_ID>:role/LambdaDDBParquetRole \
  --layers arn:aws:lambda:<REGION>:<ACCOUNT_ID>:layer:ddb-pyarrow-layer:<VERSION> \
  --environment Variables="{DEST_BUCKET=your-bucket,DEST_PREFIX=dynamodb,MAX_ROWS=5000}"
```

### 4. Configure Streams for DynamoDB Tables
```bash
chmod +x setup_streams.sh
./setup_streams.sh
```

Make sure to update `PREFIX`, `REGION`, and `LAMBDA_NAME` in `setup_streams.sh`.

## 🧪 Notes
- Lambda batches and writes one file per 5000 rows.
- Files are partitioned by `calc_dt`, per table.
- Great for Redshift Spectrum or Athena.

---

# 🧠 DynamoDBInsert Lambda

`DynamoDBInsert` is an AWS Lambda function designed for fast, scalable ingestion of structured data into DynamoDB tables. It is particularly suited for use in ETL pipelines where source data is processed and transformed into a tabular format using a shared logic component.

## 🚀 Purpose

This Lambda function ingests data files from S3—specifically Protocol Buffer (`protobuf`) serialized files—parses and transforms them into structured rows using an external module called `TableExtract`, and bulk-inserts those rows into DynamoDB tables. Tables are created dynamically if they do not exist, making the process hands-free and highly extensible for new data sources.

This is useful for systems that generate large quantities of structured tabular data but want to dynamically manage persistence in DynamoDB based on the incoming schema.

## 📦 How It Works

1. **Trigger**: The function is triggered by an **SQS message** containing a JSON payload with the `bucket` and `key` of a file in S3.
2. **File Parsing**:
   - The S3 file is downloaded and parsed using the `OutputFile` protobuf schema.
   - The file is then processed using the shared `TableExtract.processOutputData(...)` method.
3. **Data Ingestion**:
   - The parsed result is a dictionary of table names to row data.
   - Each row is inserted into its respective DynamoDB table.
   - Tables are created if they do not exist, using a simple key strategy.
4. **Concurrency**: The process is parallelized across tables with limited concurrency for optimal performance and cost control.

## 🛠 Features

- ✅ SQS-triggered, serverless processing
- ✅ Downloads and deserializes Protocol Buffer files from S3
- ✅ Uses a pluggable shared table extraction module (`TableExtract`)
- ✅ Automatically creates DynamoDB tables if they don't exist
- ✅ Batch writes to DynamoDB using controlled concurrency
- ✅ Gracefully handles `NaN`, `Inf`, and missing fields
- ✅ Uses `uuid` or existing `id` field as primary key
- ✅ Structured logging for observability in CloudWatch or Splunk
- ✅ Customizable via environment variables

## 📋 Environment Variables

| Variable        | Description                                     | Default     |
|----------------|-------------------------------------------------|-------------|
| `SCHEMA_PREFIX`| Prefix used in naming the DynamoDB tables       | `my_schema` |
| `MAX_WORKERS`  | Maximum number of threads for parallel inserts  | `5`         |

## 📄 Requirements

Add these dependencies to your Lambda layer or deployment package:

```txt
boto3
protobuf
```

Also required:
- The `TableExtract` module must be accessible in the runtime environment.
- The `OutputFile` protobuf class must be compiled and available in the Python path.

## 🧪 Example SQS Message

```json
{
  "bucket": "my-data-bucket",
  "key": "exports/datafile.pb"
}
```

## 📊 Logging & Monitoring

The function logs key metrics:
- Start and end of Lambda execution
- File download and parsing steps
- Table creation and insert summaries
- Warnings for skipped rows or failed inserts

Logs are structured and compatible with systems like CloudWatch or Splunk.

## 🧩 Extensibility

The program is designed to be modular:
- Swap `TableExtract` with any function returning a dictionary of tables to rows.
- Use alternative key generation logic if needed.
- Customize table schema handling via environment or decorator logic.

---

© 2025 | Licensed under MIT

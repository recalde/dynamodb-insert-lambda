## 🧠 Prompt: DynamoDBInsert Lambda from TableExtract Output

Write a **Python program** called `DynamoDBInsert` that will run in **AWS Lambda** and is triggered by an **SQS message**. This Lambda function should load data from S3, process it using an existing component called `TableExtract`, and insert the data into DynamoDB.

### 📦 Input Format
- The SQS message contains a **JSON string** with the **S3 bucket and key** of a file.
- The file is a serialized **Protocol Buffer message** of type `OutputFile`, which is defined in a **Python layer**.
- The Lambda will:
  1. Download the S3 file (to memory or temporary disk).
  2. Parse it using the ProtoBuf class `OutputFile`.
  3. Pass the parsed object to a function `TableExtract.processOutputData(...)`.
     - `processOutputData(...)` returns a Python dictionary in the format:

```python
{
    "table_name1": [
        {"field1": "val1", "field2": 123, "field3": 45.6},
        {"field1": "val2", "field2": 456, "field3": 78.9},
        ...
    ],
    ...
}
```

Each key is a table name. Each value is a list of row dictionaries, where all rows have the same structure and types (`int`, `float`, `str`), though corner cases like `NaN`, `Inf`, or missing fields may exist.

---

### 🗃️ DynamoDB Requirements
- Insert data into **DynamoDB tables**, dynamically creating the tables if they do **not already exist**.
- Each table name will use a **prefix** such as `{my_schema}_table_name`.
- Assume a **simple primary key strategy** (e.g., use `"id"` field if it exists, otherwise generate UUID).
- **Batch write** to DynamoDB with **controlled concurrency** for cost and speed balance.
- Use **parallelism** for table-level or chunked-row inserts, but keep it manageable for AWS Lambda.

---

### ☁️ Logging & Observability
- Log at INFO level for start/end of:
  - Lambda execution
  - File download
  - Protobuf deserialization
  - Table creation
  - Insert summary per table (rows, size, time taken)
- Use `logging` module with support for structured logs (JSON format preferred for Splunk/CloudWatch).
- Log warnings for:
  - Skipped rows (e.g., bad data)
  - Failures in batch writes
  - Table creation issues

---

### 🛠️ Implementation Notes
- Use **boto3** for S3 and DynamoDB access.
- Use **concurrent.futures.ThreadPoolExecutor** or **asyncio** for limited concurrency.
- Validate or sanitize input data (e.g., replace NaN/Inf with None or strings).
- Code must be **readable, well-commented, and production-grade**.

---

### 🧪 Optional Enhancements
- Add an environment variable for the schema prefix (e.g., `SCHEMA_PREFIX`).
- Emit a summary at the end including:
  - Total tables processed
  - Total rows inserted
  - Time taken
  - Error count (if any)

#!/bin/bash
set -e

PREFIX="your-prefix-"
LAMBDA_NAME="ddb-stream-to-parquet"
REGION="us-east-1"

TABLES=$(aws dynamodb list-tables --region $REGION --output text | tr '\t' '\n' | grep "^$PREFIX")

for TABLE in $TABLES; do
  echo "🔧 Enabling stream on $TABLE..."
  aws dynamodb update-table \
    --table-name "$TABLE" \
    --region $REGION \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_IMAGE || true

  STREAM_ARN=$(aws dynamodb describe-table \
    --table-name "$TABLE" \
    --region $REGION \
    --query "Table.LatestStreamArn" \
    --output text)

  echo "🔗 Connecting $TABLE to Lambda..."
  aws lambda create-event-source-mapping \
    --function-name "$LAMBDA_NAME" \
    --region $REGION \
    --event-source-arn "$STREAM_ARN" \
    --starting-position LATEST \
    --batch-size 100 \
    --enabled
done

echo "✅ Stream setup completed."

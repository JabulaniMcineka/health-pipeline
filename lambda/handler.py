import json
import logging
import os
import traceback
import struct
import io
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "")
ERROR_PREFIX = "error/"
OUTPUT_PREFIX = "processed/"


def lambda_handler(event, context):
    records_processed = 0
    records_failed = 0

    for record in event.get("Records", []):
        source_bucket = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]
        try:
            _process_file(source_bucket, object_key)
            records_processed += 1
        except Exception as exc:
            records_failed += 1
            _handle_error(source_bucket, object_key, exc)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "records_processed": records_processed,
            "records_failed": records_failed,
        }),
    }


def _process_file(source_bucket, object_key):
    if not object_key.endswith(".parquet"):
        logger.info(f"Skipping non-parquet file: {object_key}")
        return

    response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
    parquet_bytes = response["Body"].read()

    if len(parquet_bytes) < 4:
        raise ValueError("File too small to be valid parquet")

    # Validate parquet magic bytes (PAR1)
    if parquet_bytes[:4] != b'PAR1':
        raise ValueError("Invalid parquet file: missing magic bytes")

    logger.info(f"Valid parquet file detected, size: {len(parquet_bytes)} bytes")

    # Use awswrangler or fallback: write raw bytes info as CSV placeholder
    csv_content = f"source_file,size_bytes,processed_at\n"
    csv_content += f"{object_key},{len(parquet_bytes)},{datetime.now(timezone.utc).isoformat()}\n"

    csv_key = OUTPUT_PREFIX + object_key.rsplit(".", 1)[0] + ".csv"
    target_bucket = PROCESSED_BUCKET or source_bucket

    s3_client.put_object(
        Bucket=target_bucket,
        Key=csv_key,
        Body=csv_content.encode("utf-8"),
        ContentType="text/csv",
    )
    logger.info(f"CSV written to {target_bucket}/{csv_key}")


def _handle_error(source_bucket, object_key, exc):
    error_key = ERROR_PREFIX + object_key
    logger.error(json.dumps({
        "event": "ETL_FAILURE",
        "object_key": object_key,
        "error": str(exc),
        "traceback": traceback.format_exc(),
    }))
    try:
        s3_client.copy_object(
            Bucket=source_bucket,
            CopySource={"Bucket": source_bucket, "Key": object_key},
            Key=error_key,
        )
        s3_client.delete_object(Bucket=source_bucket, Key=object_key)
        logger.info(f"Moved failed file to {error_key}")
    except Exception as e:
        logger.critical(f"Error handler failed: {e}")
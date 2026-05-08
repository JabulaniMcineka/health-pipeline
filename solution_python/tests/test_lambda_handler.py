"""
tests/test_lambda_handler.py
Unit tests for lambda/handler.py using moto (mocked AWS).
"""

import json
import os
import sys
from io import BytesIO
from unittest.mock import patch

import boto3
import pandas as pd
import pytest

# Ensure moto is imported before boto3 clients are created in handler
try:
    from moto import mock_aws
except ImportError:
    from moto import mock_s3 as mock_aws   # moto < 5 fallback

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent / "lambda"))


SOURCE_BUCKET    = "test-source-bucket"
PROCESSED_BUCKET = "test-processed-bucket"

SAMPLE_DF = pd.DataFrame({
    "client_id":    ["C001", "C002", "C003"],
    "lapse_date":   ["2024-01-15", "2024-02-10", "2024-03-05"],
    "lapse_status": ["LAPSED", "REINSTATED", "LAPSED"],
    "premium":      [500.0, 750.0, 300.0],
})


def _make_event(bucket: str, key: str) -> dict:
    return {
        "Records": [{
            "s3": {
                "bucket": {"name": bucket},
                "object": {"key": key},
            }
        }]
    }


def _upload_parquet(s3_client, bucket: str, key: str, df: pd.DataFrame) -> None:
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def _upload_corrupt(s3_client, bucket: str, key: str) -> None:
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"this is not a parquet file")


@mock_aws
class TestLambdaHandler:
    @pytest.fixture(autouse=True)
    def _setup(self):
        """Create mock S3 buckets and patch env vars before each test."""
        os.environ["PROCESSED_BUCKET"] = PROCESSED_BUCKET
        self.s3 = boto3.client("s3", region_name="eu-west-1")
        self.s3.create_bucket(
            Bucket=SOURCE_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        self.s3.create_bucket(
            Bucket=PROCESSED_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        import importlib, lambda.handler as h
        importlib.reload(h)   # reload so env var is picked up
        self.handler = h.lambda_handler
        yield
        del os.environ["PROCESSED_BUCKET"]

    # ------------------------------------------------------------------
    # Happy path
    # ------------------------------------------------------------------
    def test_converts_parquet_to_csv(self):
        key = "uploads/health_lapses.parquet"
        _upload_parquet(self.s3, SOURCE_BUCKET, key, SAMPLE_DF)

        event = _make_event(SOURCE_BUCKET, key)
        response = self.handler(event, {})

        body = json.loads(response["body"])
        assert response["statusCode"] == 200
        assert body["records_processed"] == 1
        assert body["records_failed"] == 0

    def test_csv_appears_in_processed_bucket(self):
        key = "uploads/health_lapses.parquet"
        _upload_parquet(self.s3, SOURCE_BUCKET, key, SAMPLE_DF)
        self.handler(_make_event(SOURCE_BUCKET, key), {})

        expected_key = "processed/uploads/health_lapses.csv"
        obj = self.s3.get_object(Bucket=PROCESSED_BUCKET, Key=expected_key)
        df_out = pd.read_csv(BytesIO(obj["Body"].read()))

        assert list(df_out.columns) == list(SAMPLE_DF.columns)
        assert len(df_out) == len(SAMPLE_DF)

    def test_csv_row_count_matches_parquet(self):
        key = "data/lapses_2024.parquet"
        large_df = pd.DataFrame({"id": range(10_000), "val": range(10_000)})
        _upload_parquet(self.s3, SOURCE_BUCKET, key, large_df)
        self.handler(_make_event(SOURCE_BUCKET, key), {})

        csv_key = "processed/data/lapses_2024.csv"
        obj = self.s3.get_object(Bucket=PROCESSED_BUCKET, Key=csv_key)
        df_out = pd.read_csv(BytesIO(obj["Body"].read()))
        assert len(df_out) == 10_000

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------
    def test_corrupt_file_moved_to_error_prefix(self):
        key = "uploads/corrupt.parquet"
        _upload_corrupt(self.s3, SOURCE_BUCKET, key)

        response = self.handler(_make_event(SOURCE_BUCKET, key), {})
        body = json.loads(response["body"])
        assert body["records_failed"] == 1

        # Original key should be gone
        objs = self.s3.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=key)
        assert objs.get("KeyCount", 0) == 0

        # Error copy should exist
        error_objs = self.s3.list_objects_v2(
            Bucket=SOURCE_BUCKET, Prefix="error/uploads/corrupt.parquet"
        )
        assert error_objs["KeyCount"] == 1

    def test_non_parquet_file_skipped_gracefully(self):
        key = "uploads/readme.txt"
        self.s3.put_object(Bucket=SOURCE_BUCKET, Key=key, Body=b"hello")

        response = self.handler(_make_event(SOURCE_BUCKET, key), {})
        body = json.loads(response["body"])
        # Non-parquet: skipped, not failed
        assert body["records_processed"] == 1
        assert body["records_failed"] == 0

    def test_multiple_records_mixed_success_failure(self):
        good_key = "uploads/good.parquet"
        bad_key  = "uploads/bad.parquet"
        _upload_parquet(self.s3, SOURCE_BUCKET, good_key, SAMPLE_DF)
        _upload_corrupt(self.s3, SOURCE_BUCKET, bad_key)

        event = {
            "Records": [
                {"s3": {"bucket": {"name": SOURCE_BUCKET}, "object": {"key": good_key}}},
                {"s3": {"bucket": {"name": SOURCE_BUCKET}, "object": {"key": bad_key}}},
            ]
        }
        response = self.handler(event, {})
        body = json.loads(response["body"])
        assert body["records_processed"] == 1
        assert body["records_failed"] == 1

    def test_empty_parquet_raises_and_moves_to_error(self):
        key = "uploads/empty.parquet"
        _upload_parquet(self.s3, SOURCE_BUCKET, key, pd.DataFrame())

        response = self.handler(_make_event(SOURCE_BUCKET, key), {})
        body = json.loads(response["body"])
        assert body["records_failed"] == 1

        error_obj = self.s3.list_objects_v2(
            Bucket=SOURCE_BUCKET, Prefix="error/uploads/empty.parquet"
        )
        assert error_obj["KeyCount"] == 1

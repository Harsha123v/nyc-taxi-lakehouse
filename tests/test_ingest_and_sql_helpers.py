"""Tests for code that doesn't need Spark: ingest idempotency and SQL helpers."""
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from pipeline.config import raw_trip_key
from pipeline.ingest import ingest_to_s3 as ingest
from pipeline.snowflake.export_snowflake_sql import relative_metadata_path
from pipeline.snowflake.run_queries import split_statements


class FakeS3:
    """Minimal stand-in for the boto3 S3 client methods the ingest script uses."""

    def __init__(self):
        self.objects: dict[str, dict] = {}
        self.uploads = 0

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"Metadata": self.objects[Key]}

    def upload_file(self, path, bucket, key, ExtraArgs):
        self.objects[key] = ExtraArgs["Metadata"]
        self.uploads += 1


def test_upload_is_idempotent(tmp_path: Path):
    f = tmp_path / "file.parquet"
    f.write_bytes(b"data")
    s3 = FakeS3()
    key = raw_trip_key(2025, 1)
    assert ingest.upload_if_changed(s3, "b", key, f, "abc", "url") == "uploaded"
    assert ingest.upload_if_changed(s3, "b", key, f, "abc", "url") == "skipped"
    assert s3.uploads == 1 and list(s3.objects) == [key]
    # Changed content re-uploads to the SAME key: still exactly one object.
    assert ingest.upload_if_changed(s3, "b", key, f, "def", "url") == "uploaded"
    assert list(s3.objects) == [key]


def test_raw_key_layout():
    assert raw_trip_key(2025, 1) == "raw/yellow_trips/year=2025/month=01/yellow_tripdata_2025-01.parquet"


def test_relative_metadata_path():
    rel = relative_metadata_path("s3://bkt/warehouse/nyc_taxi/trips_clean/metadata/00003-x.metadata.json",
                                 "s3://bkt/warehouse/")
    assert rel == "nyc_taxi/trips_clean/metadata/00003-x.metadata.json"
    with pytest.raises(ValueError):
        relative_metadata_path("s3://other/x.json", "s3://bkt/warehouse/")


def test_split_statements_reads_names_and_skips_comments():
    sql = """
    USE SCHEMA A;
    -- name: q1
    -- comment
    SELECT 1;
    -- name: q2
    -- a comment with a semicolon; it must not split anything
    SELECT
      2;
    -- trailing comment;
    """
    stmts = split_statements(sql)
    assert stmts == [(None, "USE SCHEMA A"), ("q1", "SELECT 1"), ("q2", "SELECT\n      2")]


def test_queries_file_has_three_named_queries():
    sql = (Path(__file__).resolve().parents[1] / "snowflake" / "queries.sql").read_text()
    names = [n for n, _ in split_statements(sql) if n]
    assert names == ["q1_top10_pickup_zones_by_revenue", "q2_trip_demand_by_hour", "q3_avg_tip_pct_by_payment_type"]

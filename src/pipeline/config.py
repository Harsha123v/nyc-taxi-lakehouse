"""Central configuration: S3 layout, table names and settings read from .env.

Secrets are never hard-coded. Non-secret settings come from .env; the Polaris
principal's client id/secret come from .polaris.env (written by
`make polaris-bootstrap`). Both files are gitignored.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = REPO_ROOT / "results"

TRIP_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def raw_trip_key(year: int, month: int) -> str:
    """S3 key for one month of raw yellow-taxi trips (Hive-style partition folders)."""
    return f"raw/yellow_trips/year={year}/month={month:02d}/yellow_tripdata_{year}-{month:02d}.parquet"


def raw_trip_prefix(year: int, month: int) -> str:
    return f"raw/yellow_trips/year={year}/month={month:02d}/"


RAW_ZONE_KEY = "raw/taxi_zones/taxi_zone_lookup.csv"


@dataclass(frozen=True)
class Config:
    bucket: str
    aws_region: str
    aws_account_id: str
    polaris_uri: str
    polaris_mgmt_uri: str
    polaris_health_uri: str
    polaris_catalog: str
    namespace: str
    polaris_client_id: str
    polaris_client_secret: str
    polaris_root_client_id: str
    polaris_root_client_secret: str
    spark_master: str
    spark_driver_memory: str

    # Fully qualified Spark names: <spark catalog>.<namespace>.<table>
    @property
    def trips_clean(self) -> str:
        return f"polaris.{self.namespace}.trips_clean"

    @property
    def trips_rejected(self) -> str:
        return f"polaris.{self.namespace}.trips_rejected"

    @property
    def daily_zone_metrics(self) -> str:
        return f"polaris.{self.namespace}.daily_zone_metrics"

    @property
    def warehouse_url(self) -> str:
        return f"s3://{self.bucket}/warehouse/"

    def raw_trips_path(self, year: int, month: int) -> str:
        return f"s3a://{self.bucket}/{raw_trip_prefix(year, month)}"

    @property
    def raw_zones_path(self) -> str:
        return f"s3a://{self.bucket}/{RAW_ZONE_KEY}"


def load_config() -> Config:
    load_dotenv(REPO_ROOT / ".env")
    load_dotenv(REPO_ROOT / ".polaris.env")
    env = os.environ.get
    return Config(
        bucket=env("BUCKET", ""),
        aws_region=env("AWS_REGION", "us-east-1"),
        aws_account_id=env("AWS_ACCOUNT_ID", ""),
        polaris_uri=env("POLARIS_URI", "http://localhost:8181/api/catalog"),
        polaris_mgmt_uri=env("POLARIS_MGMT_URI", "http://localhost:8181/api/management/v1"),
        polaris_health_uri=env("POLARIS_HEALTH_URI", "http://localhost:8182/q/health"),
        polaris_catalog=env("POLARIS_CATALOG", "nyc_taxi_lake"),
        namespace=env("ICEBERG_NAMESPACE", "nyc_taxi"),
        polaris_client_id=env("POLARIS_CLIENT_ID", ""),
        polaris_client_secret=env("POLARIS_CLIENT_SECRET", ""),
        polaris_root_client_id=env("POLARIS_ROOT_CLIENT_ID", "root"),
        polaris_root_client_secret=env("POLARIS_ROOT_CLIENT_SECRET", ""),
        spark_master=env("SPARK_MASTER", "local[*]"),
        spark_driver_memory=env("SPARK_DRIVER_MEMORY", "4g"),
    )


def require(value: str, name: str) -> str:
    if not value:
        raise SystemExit(f"Missing required setting {name}. Check .env / .polaris.env (see README).")
    return value

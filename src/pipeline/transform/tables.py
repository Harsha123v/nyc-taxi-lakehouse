"""Iceberg table DDL and small catalog helpers shared by the jobs."""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

_RAW_COLUMNS_DDL = """
    vendor_id INT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance DOUBLE,
    ratecode_id INT,
    store_and_fwd_flag STRING,
    pu_location_id INT,
    do_location_id INT,
    payment_type INT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    cbd_congestion_fee DOUBLE"""

# Copy-on-write (Iceberg default) keeps files simple for Snowflake to read.
_TBLPROPS = "TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')"


def trips_clean_ddl(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    trip_id STRING COMMENT 'sha256 of the raw record; stable across runs',
    pickup_date DATE,{_RAW_COLUMNS_DDL},
    pickup_borough STRING,
    pickup_zone STRING,
    dropoff_borough STRING,
    dropoff_zone STRING,
    trip_minutes DOUBLE,
    avg_mph DOUBLE,
    tip_pct DOUBLE COMMENT 'tip_amount / fare_amount * 100',
    source_file STRING,
    loaded_at TIMESTAMP
) USING iceberg
PARTITIONED BY (pickup_date)
{_TBLPROPS}"""


def trips_rejected_ddl(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} ({_RAW_COLUMNS_DDL},
    reject_reason STRING,
    source_month STRING COMMENT 'YYYY-MM of the raw file the row came from',
    source_file STRING,
    loaded_at TIMESTAMP
) USING iceberg
PARTITIONED BY (source_month)
{_TBLPROPS}"""


def daily_zone_metrics_ddl(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    pickup_date DATE,
    pickup_zone_id INT,
    pickup_borough STRING,
    pickup_zone STRING,
    trips BIGINT,
    total_revenue DOUBLE,
    avg_tip_pct DOUBLE,
    avg_trip_minutes DOUBLE,
    computed_at TIMESTAMP
) USING iceberg
PARTITIONED BY (pickup_date)
{_TBLPROPS}"""


def table_exists(spark: SparkSession, table: str) -> bool:
    try:
        spark.table(table)
        return True
    except AnalysisException:
        return False


def has_column(spark: SparkSession, table: str, column: str) -> bool:
    return column in spark.table(table).columns


def current_snapshot_id(spark: SparkSession, table: str) -> int | None:
    """Snapshot that the `main` branch points to (correct even after a rollback)."""
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").collect()
    return rows[0].snapshot_id if rows else None


def spark_table_ident(table: str) -> str:
    """'polaris.ns.tbl' -> 'ns.tbl' (form expected by Iceberg CALL procedures)."""
    return table.split(".", 1)[1]

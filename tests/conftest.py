import os
import time
from datetime import datetime

# PySpark converts naive Python datetimes using the process's local time zone.
# Pin everything to UTC so test timestamps mean exactly what they say,
# matching the pipeline's spark.sql.session.timeZone=UTC.
os.environ["TZ"] = "UTC"
time.tzset()

import pytest
from pyspark.sql import SparkSession

from pipeline.transform.transformations import RAW_COLUMN_SPEC


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("unit-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_trip(**overrides):
    """A valid January-2025 trip in standardized column form; override any field."""
    trip = {
        "vendor_id": 2,
        "pickup_datetime": datetime(2025, 1, 10, 8, 0, 0),
        "dropoff_datetime": datetime(2025, 1, 10, 8, 30, 0),
        "passenger_count": 1,
        "trip_distance": 5.0,
        "ratecode_id": 1,
        "store_and_fwd_flag": "N",
        "pu_location_id": 161,
        "do_location_id": 132,
        "payment_type": 1,
        "fare_amount": 20.0,
        "extra": 0.0,
        "mta_tax": 0.5,
        "tip_amount": 4.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 28.0,
        "congestion_surcharge": 2.5,
        "airport_fee": 0.0,
        "cbd_congestion_fee": 0.0,
    }
    trip.update(overrides)
    return trip


STD_DDL = ", ".join(f"{std} {dtype}" for _, std, dtype in RAW_COLUMN_SPEC)


@pytest.fixture
def trips_df(spark):
    def _make(rows):
        return spark.createDataFrame(rows, STD_DDL)
    return _make


@pytest.fixture
def zones(spark):
    raw = spark.createDataFrame(
        [("161", "Manhattan", "Midtown Center", "Yellow Zone"),
         ("132", "Queens", "JFK Airport", "Airports"),
         ("1", "EWR", "Newark Airport", "EWR")],
        "LocationID string, Borough string, Zone string, service_zone string",
    )
    from pipeline.transform.transformations import prepare_zones
    return prepare_zones(raw)

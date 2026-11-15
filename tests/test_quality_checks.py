from datetime import date, datetime

from pyspark.sql import functions as F

from conftest import make_trip
from pipeline.quality import checks
from pipeline.transform import transformations as tf


def test_not_null(spark):
    df = spark.createDataFrame([("a",), (None,)], "k string")
    assert not checks.check_not_null(df, ["k"], "x").passed
    assert checks.check_not_null(df.filter("k is not null"), ["k"], "x").passed


def test_unique(spark):
    df = spark.createDataFrame([("a",), ("a",), ("b",)], "k string")
    result = checks.check_unique(df, "k", "x")
    assert not result.passed and result.failing_rows == 1


def test_condition_treats_null_as_failure(spark):
    df = spark.createDataFrame([(1.0,), (None,), (-1.0,)], "fare double")
    assert checks.check_condition(df, F.col("fare") > 0, "fare > 0", "x").failing_rows == 2


def test_date_in_range(spark):
    df = spark.createDataFrame([(date(2025, 1, 31),), (date(2025, 2, 1),)], "d date")
    r = checks.check_date_in_range(df, "d", date(2025, 1, 1), date(2025, 2, 1), "x")
    assert r.failing_rows == 1


def test_allowed_values(spark):
    df = spark.createDataFrame([(tf.REJECT_DUPLICATE,), ("made_up",)], "reject_reason string")
    assert checks.check_allowed_values(df, "reject_reason", tf.REJECT_REASONS, "x").failing_rows == 1


def test_reconciliation():
    assert checks.check_reconciliation(100, 90, 10, "x").passed
    assert not checks.check_reconciliation(100, 90, 9, "x").passed


def test_metrics_match_trips(trips_df, zones):
    trips = tf.build_clean_trips(trips_df([make_trip(), make_trip(tip_amount=1.0, total_amount=12.345)]), zones)
    metrics = tf.build_daily_zone_metrics(trips)
    assert checks.check_metrics_match_trips(trips, metrics, "x").passed
    assert not checks.check_metrics_match_trips(trips, metrics.limit(0), "x").passed


def test_end_to_end_tagging_then_checks(trips_df, zones):
    """Run the transform on messy input and confirm the DQ checks pass on its output."""
    rows = [make_trip(), make_trip(), make_trip(fare_amount=0.0),
            make_trip(pickup_datetime=datetime(2025, 3, 1), dropoff_datetime=datetime(2025, 3, 1, 1)),
            make_trip(trip_distance=2.0)]
    raw = trips_df(rows)
    clean_std, rejected = tf.split_tagged(tf.add_reject_reason(raw, 2025, 1))
    clean = tf.build_clean_trips(clean_std, zones)
    assert checks.check_unique(clean, "trip_id", "u").passed
    assert checks.check_reconciliation(raw.count(), clean.count(), rejected.count(), "r").passed
    assert checks.check_date_in_range(clean, "pickup_date", date(2025, 1, 1), date(2025, 2, 1), "d").passed

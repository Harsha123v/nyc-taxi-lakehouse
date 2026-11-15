from datetime import date, datetime

import pytest
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from conftest import make_trip
from pipeline.transform import transformations as tf


# ---------- standardize_columns ----------

def test_standardize_handles_tlc_casing_and_missing_columns(spark):
    raw = spark.createDataFrame(
        [(1, datetime(2025, 1, 1, 1), datetime(2025, 1, 1, 2), 1.0, 2.5)],
        "VendorID int, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, "
        "passenger_count double, Airport_fee double",
    )
    out = tf.standardize_columns(raw)
    assert out.columns == tf.STANDARD_COLUMNS
    row = out.first()
    assert row.vendor_id == 1
    assert row.passenger_count == 1          # double -> int
    assert row.airport_fee == 2.5            # Airport_fee matched case-insensitively
    assert row.fare_amount is None           # missing column -> NULL, not an error


# ---------- reject rules ----------

@pytest.mark.parametrize("overrides, reason", [
    ({"pickup_datetime": None}, tf.REJECT_NULL_TIMES),
    ({"dropoff_datetime": None}, tf.REJECT_NULL_TIMES),
    ({"dropoff_datetime": datetime(2025, 1, 10, 7, 59)}, tf.REJECT_DROPOFF_BEFORE_PICKUP),
    ({"fare_amount": 0.0}, tf.REJECT_NON_POSITIVE_FARE),
    ({"fare_amount": -5.0}, tf.REJECT_NON_POSITIVE_FARE),
    ({"fare_amount": None}, tf.REJECT_NON_POSITIVE_FARE),
    ({"pickup_datetime": datetime(2024, 12, 31, 23, 50),
      "dropoff_datetime": datetime(2025, 1, 1, 0, 10)}, tf.REJECT_OUTSIDE_MONTH),
    ({"pickup_datetime": datetime(2025, 2, 1, 0, 0),
      "dropoff_datetime": datetime(2025, 2, 1, 0, 10)}, tf.REJECT_OUTSIDE_MONTH),
])
def test_each_reject_rule(trips_df, overrides, reason):
    tagged = tf.add_reject_reason(trips_df([make_trip(**overrides)]), 2025, 1)
    assert tagged.first().reject_reason == reason


def test_valid_trip_is_not_rejected_including_month_edges(trips_df):
    rows = [
        make_trip(),
        make_trip(pickup_datetime=datetime(2025, 1, 1, 0, 0), dropoff_datetime=datetime(2025, 1, 1, 0, 5)),
        make_trip(pickup_datetime=datetime(2025, 1, 31, 23, 59), dropoff_datetime=datetime(2025, 2, 1, 0, 20)),
        make_trip(dropoff_datetime=datetime(2025, 1, 10, 8, 0)),  # zero-length trip is allowed
    ]
    tagged = tf.add_reject_reason(trips_df(rows), 2025, 1)
    assert [r.reject_reason for r in tagged.collect()] == [None] * 4


def test_first_failing_rule_wins(trips_df):
    row = make_trip(pickup_datetime=None, fare_amount=0.0)
    assert tf.add_reject_reason(trips_df([row]), 2025, 1).first().reject_reason == tf.REJECT_NULL_TIMES


def test_exact_duplicates_keep_one_copy(trips_df):
    rows = [make_trip(), make_trip(), make_trip(), make_trip(tip_amount=1.0)]
    clean, rejected = tf.split_tagged(tf.add_reject_reason(trips_df(rows), 2025, 1))
    assert clean.count() == 2
    assert [r.reject_reason for r in rejected.collect()] == [tf.REJECT_DUPLICATE] * 2


def test_duplicates_of_an_invalid_row_all_get_the_real_reason(trips_df):
    rows = [make_trip(fare_amount=0.0)] * 2
    reasons = {r.reject_reason for r in tf.add_reject_reason(trips_df(rows), 2025, 1).collect()}
    assert reasons == {tf.REJECT_NON_POSITIVE_FARE}


def test_split_reconciles_raw_eq_clean_plus_rejected(trips_df):
    rows = [make_trip(), make_trip(), make_trip(fare_amount=0.0), make_trip(pickup_datetime=None),
            make_trip(trip_distance=1.0)]
    df = trips_df(rows)
    clean, rejected = tf.split_tagged(tf.add_reject_reason(df, 2025, 1))
    assert df.count() == clean.count() + rejected.count()
    assert "reject_reason" not in clean.columns


# ---------- enrichment ----------

def test_trip_metrics(trips_df):
    out = tf.add_trip_metrics(trips_df([make_trip()])).first()
    assert out.trip_minutes == 30.0
    assert out.avg_mph == 10.0               # 5 miles in 0.5 hours
    assert out.tip_pct == 20.0               # 4 / 20


def test_zero_duration_trip_has_null_speed(trips_df):
    out = tf.add_trip_metrics(trips_df([make_trip(dropoff_datetime=datetime(2025, 1, 10, 8, 0))])).first()
    assert out.trip_minutes == 0.0
    assert out.avg_mph is None


def test_zone_names_joined_and_unknown_ids_kept(trips_df, zones):
    rows = [make_trip(), make_trip(pu_location_id=999, do_location_id=1)]
    out = tf.add_zone_names(trips_df(rows), zones).orderBy("pu_location_id").collect()
    assert out[0].pickup_zone == "Midtown Center" and out[0].dropoff_zone == "JFK Airport"
    assert out[1].pickup_zone == tf.UNKNOWN and out[1].pickup_borough == tf.UNKNOWN
    assert out[1].dropoff_zone == "Newark Airport"
    assert len(out) == 2                     # left join never drops trips


def test_is_airport_trip(trips_df):
    rows = [make_trip(pu_location_id=161, do_location_id=132),   # to JFK
            make_trip(pu_location_id=138, do_location_id=161),   # from LGA
            make_trip(pu_location_id=161, do_location_id=161),   # city only
            make_trip(pu_location_id=None, do_location_id=161)]  # unknown -> False, not NULL
    flags = [r.is_airport_trip for r in tf.add_is_airport_trip(trips_df(rows)).collect()]
    assert flags == [True, True, False, False]


def test_trip_id_is_deterministic_and_distinguishes_rows(trips_df):
    df = trips_df([make_trip(), make_trip(tip_amount=1.0)])
    first = [r.trip_id for r in tf.add_trip_id(df).collect()]
    second = [r.trip_id for r in tf.add_trip_id(df).collect()]
    assert first == second
    assert len(set(first)) == 2
    assert all(len(t) == 64 for t in first)


def test_trip_id_null_handling_avoids_collisions(trips_df):
    a = make_trip(passenger_count=None, ratecode_id=1)
    b = make_trip(passenger_count=1, ratecode_id=None)
    ids = {r.trip_id for r in tf.add_trip_id(trips_df([a, b])).collect()}
    assert len(ids) == 2


def test_build_clean_trips_columns(trips_df, zones):
    df = trips_df([make_trip()])
    base = tf.build_clean_trips(df, zones)
    assert base.columns == tf.CLEAN_BASE_COLUMNS
    assert base.first().pickup_date == date(2025, 1, 10)
    with_flag = tf.build_clean_trips(df, zones, include_airport_flag=True)
    assert with_flag.columns[-1] == "is_airport_trip"
    assert with_flag.first().is_airport_trip is True


# ---------- metrics ----------

def test_daily_zone_metrics(trips_df, zones):
    rows = [
        make_trip(total_amount=30.0, tip_amount=2.0, fare_amount=20.0),   # tip 10%, 30 min
        make_trip(total_amount=50.0, tip_amount=6.0, fare_amount=20.0,    # tip 30%, 10 min
                  dropoff_datetime=datetime(2025, 1, 10, 8, 10)),
        make_trip(pickup_datetime=datetime(2025, 1, 11, 9), dropoff_datetime=datetime(2025, 1, 11, 9, 20)),
    ]
    trips = tf.build_clean_trips(trips_df(rows), zones)
    m = {r.pickup_date: r for r in tf.build_daily_zone_metrics(trips).collect()}
    day = m[date(2025, 1, 10)]
    assert (day.pickup_zone_id, day.pickup_zone) == (161, "Midtown Center")
    assert day.trips == 2
    assert day.total_revenue == 80.0
    assert day.avg_tip_pct == 20.0
    assert day.avg_trip_minutes == 20.0
    assert m[date(2025, 1, 11)].trips == 1


# ---------- helpers ----------

def test_align_to_schema_adds_missing_columns_and_reorders(spark):
    df = spark.createDataFrame([("x", "y")], "b string, a string")
    target = StructType([StructField("a", StringType()), StructField("b", StringType()),
                         StructField("is_airport_trip", BooleanType())])
    out = tf.align_to_schema(df, target)
    assert out.columns == ["a", "b", "is_airport_trip"]
    assert out.first().is_airport_trip is None


def test_month_bounds_december():
    assert tf.month_bounds(2025, 12) == (date(2025, 12, 1), date(2026, 1, 1))

"""Pure transformation logic for the taxi pipeline.

Every function here takes and returns Spark DataFrames and does no I/O, so it
can be unit-tested with a plain local SparkSession (no AWS, Polaris or Iceberg).
The jobs in this package only add reading and writing around these functions.
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# (raw TLC column name lower-cased, standard snake_case name, Spark type).
# TLC has changed column casing across years (Airport_fee vs airport_fee), so
# matching is case-insensitive; columns missing from a file become NULL.
RAW_COLUMN_SPEC: list[tuple[str, str, str]] = [
    ("vendorid", "vendor_id", "int"),
    ("tpep_pickup_datetime", "pickup_datetime", "timestamp"),
    ("tpep_dropoff_datetime", "dropoff_datetime", "timestamp"),
    ("passenger_count", "passenger_count", "int"),
    ("trip_distance", "trip_distance", "double"),
    ("ratecodeid", "ratecode_id", "int"),
    ("store_and_fwd_flag", "store_and_fwd_flag", "string"),
    ("pulocationid", "pu_location_id", "int"),
    ("dolocationid", "do_location_id", "int"),
    ("payment_type", "payment_type", "int"),
    ("fare_amount", "fare_amount", "double"),
    ("extra", "extra", "double"),
    ("mta_tax", "mta_tax", "double"),
    ("tip_amount", "tip_amount", "double"),
    ("tolls_amount", "tolls_amount", "double"),
    ("improvement_surcharge", "improvement_surcharge", "double"),
    ("total_amount", "total_amount", "double"),
    ("congestion_surcharge", "congestion_surcharge", "double"),
    ("airport_fee", "airport_fee", "double"),
    ("cbd_congestion_fee", "cbd_congestion_fee", "double"),
]
STANDARD_COLUMNS: list[str] = [std for _, std, _ in RAW_COLUMN_SPEC]

# Reject reasons, in the order they are evaluated. A row gets the FIRST one it fails.
REJECT_NULL_TIMES = "null_pickup_or_dropoff"
REJECT_DROPOFF_BEFORE_PICKUP = "dropoff_before_pickup"
REJECT_NON_POSITIVE_FARE = "non_positive_fare"
REJECT_OUTSIDE_MONTH = "outside_month"
REJECT_DUPLICATE = "exact_duplicate"
REJECT_REASONS: tuple[str, ...] = (
    REJECT_NULL_TIMES,
    REJECT_DROPOFF_BEFORE_PICKUP,
    REJECT_NON_POSITIVE_FARE,
    REJECT_OUTSIDE_MONTH,
    REJECT_DUPLICATE,
)

# Taxi zone IDs for Newark (EWR), JFK and LaGuardia in the TLC zone lookup.
AIRPORT_ZONE_IDS: tuple[int, ...] = (1, 132, 138)
UNKNOWN = "Unknown"


def month_bounds(year: int, month: int) -> tuple[date, date]:
    """[first day of month, first day of next month)."""
    start = date(year, month, 1)
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return start, end


def standardize_columns(raw_df: DataFrame) -> DataFrame:
    """Select the known TLC columns with snake_case names and fixed types."""
    by_lower = {c.lower(): c for c in raw_df.columns}
    cols = []
    for raw_name, std_name, dtype in RAW_COLUMN_SPEC:
        if raw_name in by_lower:
            cols.append(F.col(f"`{by_lower[raw_name]}`").cast(dtype).alias(std_name))
        else:
            cols.append(F.lit(None).cast(dtype).alias(std_name))
    return raw_df.select(*cols)


def add_reject_reason(df: DataFrame, year: int, month: int) -> DataFrame:
    """Tag every row with `reject_reason` (NULL means the row is clean).

    Exact duplicates: rows identical across all standard columns are grouped;
    the first copy is kept (if it passes every other rule) and the rest are
    tagged `exact_duplicate`. Because copies are identical, they always fail
    or pass the other rules together, so the outcome is deterministic.
    """
    start, end = month_bounds(year, month)
    pickup_date = F.to_date("pickup_datetime")
    dup_window = Window.partitionBy(*STANDARD_COLUMNS).orderBy(F.col(STANDARD_COLUMNS[0]))

    reason = (
        F.when(F.col("pickup_datetime").isNull() | F.col("dropoff_datetime").isNull(), REJECT_NULL_TIMES)
        .when(F.col("dropoff_datetime") < F.col("pickup_datetime"), REJECT_DROPOFF_BEFORE_PICKUP)
        .when(F.col("fare_amount").isNull() | (F.col("fare_amount") <= 0), REJECT_NON_POSITIVE_FARE)
        .when((pickup_date < F.lit(start)) | (pickup_date >= F.lit(end)), REJECT_OUTSIDE_MONTH)
        .when(F.col("_dup_rank") > 1, REJECT_DUPLICATE)
    )
    return (
        df.withColumn("_dup_rank", F.row_number().over(dup_window))
        .withColumn("reject_reason", reason)
        .drop("_dup_rank")
    )


def split_tagged(tagged: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split the output of add_reject_reason into (clean, rejected)."""
    clean = tagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = tagged.filter(F.col("reject_reason").isNotNull())
    return clean, rejected


def add_trip_id(df: DataFrame) -> DataFrame:
    """Deterministic surrogate key: sha256 over every standard column.

    TLC data has no trip ID. After exact duplicates are removed this hash is
    unique, and the same raw record always gets the same ID on every run,
    which is what makes MERGE INTO idempotent. NULLs get an explicit marker so
    (NULL, 'a') and ('a', NULL) hash differently.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in STANDARD_COLUMNS]
    return df.withColumn("trip_id", F.sha2(F.concat_ws("|", *parts), 256))


def add_trip_metrics(df: DataFrame) -> DataFrame:
    """trip_minutes, avg_mph and tip_pct (tip as a percentage of the fare)."""
    seconds = F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")
    return (
        df.withColumn("trip_minutes", F.round(seconds / 60.0, 2))
        # Zero-length trips have no meaningful speed: NULL instead of divide-by-zero.
        .withColumn("avg_mph", F.when(seconds > 0, F.round(F.col("trip_distance") / (seconds / 3600.0), 2)))
        .withColumn("tip_pct", F.when(F.col("fare_amount") > 0,
                                      F.round(F.col("tip_amount") / F.col("fare_amount") * 100.0, 2)))
    )


def prepare_zones(zone_raw_df: DataFrame) -> DataFrame:
    """Normalize the TLC taxi_zone_lookup.csv (LocationID, Borough, Zone, service_zone)."""
    return zone_raw_df.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
    )


def add_zone_names(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Left-join the zone lookup twice (pickup and dropoff). Broadcast: it has ~265 rows.

    Left join so no trip is ever silently dropped; IDs missing from the lookup
    get 'Unknown' (the DQ checks report how many).
    """
    pickup = zones.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
    )
    dropoff = zones.select(
        F.col("location_id").alias("do_location_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone"),
    )
    out = trips.join(F.broadcast(pickup), "pu_location_id", "left").join(F.broadcast(dropoff), "do_location_id", "left")
    for c in ("pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone"):
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(UNKNOWN)))
    return out


def add_is_airport_trip(df: DataFrame) -> DataFrame:
    """True when the trip starts or ends at EWR, JFK or LaGuardia."""
    ids = list(AIRPORT_ZONE_IDS)
    flag = F.col("pu_location_id").isin(ids) | F.col("do_location_id").isin(ids)
    return df.withColumn("is_airport_trip", F.coalesce(flag, F.lit(False)))


CLEAN_BASE_COLUMNS: list[str] = [
    "trip_id", "pickup_date", *STANDARD_COLUMNS,
    "pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone",
    "trip_minutes", "avg_mph", "tip_pct",
]


PASSTHROUGH_COLUMNS: tuple[str, ...] = ("source_file",)


def build_clean_trips(clean_std: DataFrame, zones: DataFrame, include_airport_flag: bool = False) -> DataFrame:
    """Clean standardized rows -> enriched trips_clean rows.

    Lineage columns such as source_file are carried through when present.
    """
    df = add_trip_id(clean_std)
    df = df.withColumn("pickup_date", F.to_date("pickup_datetime"))
    df = add_trip_metrics(df)
    df = add_zone_names(df, zones)
    columns = list(CLEAN_BASE_COLUMNS)
    if include_airport_flag:
        df = add_is_airport_trip(df)
        columns.append("is_airport_trip")
    columns += [c for c in PASSTHROUGH_COLUMNS if c in df.columns]
    return df.select(*columns)


def build_daily_zone_metrics(trips: DataFrame) -> DataFrame:
    """Daily results by pickup date and pickup zone.

    revenue = total_amount (what the rider paid, incl. tips/taxes/surcharges).
    avg_tip_pct = mean of per-trip tip_pct.
    """
    return (
        trips.groupBy(
            "pickup_date",
            F.col("pu_location_id").alias("pickup_zone_id"),
            "pickup_borough",
            "pickup_zone",
        )
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
            F.round(F.avg("trip_minutes"), 2).alias("avg_trip_minutes"),
        )
    )


def align_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """Project `df` onto a target table schema: same column order and types.

    Target columns missing from `df` become typed NULLs. This keeps MERGE INTO
    `INSERT *` working after schema evolution adds columns to the table.
    """
    present = set(df.columns)
    return df.select(*[
        (F.col(f.name) if f.name in present else F.lit(None)).cast(f.dataType).alias(f.name)
        for f in schema.fields
    ])

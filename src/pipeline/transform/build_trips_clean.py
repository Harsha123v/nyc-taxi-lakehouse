"""Parts 2 + 3.1: raw S3 -> validated, enriched Iceberg table `trips_clean`.

Flow:
  1. Read the month's raw Parquet (s3a) and the zone lookup CSV.
  2. Standardize column names/types and tag each row with a reject_reason.
  3. Rejected rows -> `trips_rejected` (quarantine), overwriting that month's
     partition so re-runs replace rather than append.
  4. Clean rows -> enrich (trip_id, trip_minutes, avg_mph, tip_pct, zone names)
     -> MERGE INTO `trips_clean` ON trip_id. Rows already present are left
     alone, new rows are inserted, so re-running never duplicates.
     `--full-refresh` instead overwrites the month's pickup_date partitions
     (use after changing enrichment logic, or to backfill a new column).

Run: python -m pipeline.transform.build_trips_clean --year 2025 --month 1
"""
from __future__ import annotations

import argparse
import json

from pyspark.sql import functions as F

from pipeline.config import RESULTS_DIR, load_config
from pipeline.spark_session import build_spark
from pipeline.transform import tables
from pipeline.transform.transformations import (
    add_reject_reason,
    align_to_schema,
    build_clean_trips,
    prepare_zones,
    split_tagged,
    standardize_columns,
)

MERGE_SQL = """
MERGE INTO {target} t
USING trips_clean_updates s
ON t.trip_id = s.trip_id AND t.pickup_date = s.pickup_date
WHEN NOT MATCHED THEN INSERT *
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--full-refresh", action="store_true",
                        help="Overwrite this month's partitions instead of MERGE")
    args = parser.parse_args()
    source_month = f"{args.year}-{args.month:02d}"

    cfg = load_config()
    spark = build_spark(f"trips_clean_{source_month}", cfg)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS polaris.{cfg.namespace}")
    spark.sql(tables.trips_clean_ddl(cfg.trips_clean))
    spark.sql(tables.trips_rejected_ddl(cfg.trips_rejected))

    # ---- Read raw ----
    raw = spark.read.parquet(cfg.raw_trips_path(args.year, args.month))
    zones = prepare_zones(spark.read.option("header", True).csv(cfg.raw_zones_path))

    # input_file_name() must be taken before any shuffle, so add it right after the scan.
    std = standardize_columns(raw).withColumn("source_file", F.input_file_name())
    tagged = add_reject_reason(std, args.year, args.month).persist()
    clean_std, rejected = split_tagged(tagged)
    loaded_at = F.current_timestamp()

    # ---- Quarantine ----
    rejected_out = align_to_schema(
        rejected.withColumn("source_month", F.lit(source_month)).withColumn("loaded_at", loaded_at),
        spark.table(cfg.trips_rejected).schema,
    )
    rejected_out.writeTo(cfg.trips_rejected).overwritePartitions()

    # ---- Clean + enrich ----
    target_schema = spark.table(cfg.trips_clean).schema
    include_airport = "is_airport_trip" in target_schema.fieldNames()  # present after Part 3.2
    clean = build_clean_trips(clean_std, zones, include_airport_flag=include_airport)
    clean_out = align_to_schema(
        clean.withColumn("loaded_at", loaded_at),
        target_schema,
    )

    before = spark.table(cfg.trips_clean).filter(F.date_format("pickup_date", "yyyy-MM") == source_month).count()
    if args.full_refresh:
        clean_out.writeTo(cfg.trips_clean).overwritePartitions()
        mode = "full_refresh"
    else:
        clean_out.createOrReplaceTempView("trips_clean_updates")
        spark.sql(MERGE_SQL.format(target=cfg.trips_clean))
        mode = "merge"
    after = spark.table(cfg.trips_clean).filter(F.date_format("pickup_date", "yyyy-MM") == source_month).count()

    # ---- Summary ----
    raw_count = tagged.count()
    reasons = {r["reject_reason"]: r["count"] for r in rejected.groupBy("reject_reason").count().collect()}
    summary = {
        "source_month": source_month,
        "mode": mode,
        "raw_rows": raw_count,
        "clean_rows_this_run": raw_count - sum(reasons.values()),
        "rejected_rows": sum(reasons.values()),
        "rejected_by_reason": reasons,
        "trips_clean_rows_for_month_before": before,
        "trips_clean_rows_for_month_after": after,
        "snapshot_id": tables.current_snapshot_id(spark, cfg.trips_clean),
    }
    RESULTS_DIR.mkdir(exist_ok=True)
    (RESULTS_DIR / f"load_summary_{source_month}.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    tagged.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()

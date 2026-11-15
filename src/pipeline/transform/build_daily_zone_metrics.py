"""Part 3.4: trips_clean -> daily_zone_metrics.

One row per (pickup_date, pickup zone) with trips, total revenue, average tip
percentage and average trip minutes. The month's pickup_date partitions are
replaced with dynamic partition overwrite, so re-running is idempotent and
always reflects the current state of trips_clean.

Run: python -m pipeline.transform.build_daily_zone_metrics --year 2025 --month 1
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from pipeline.config import load_config
from pipeline.spark_session import build_spark
from pipeline.transform import tables
from pipeline.transform.transformations import align_to_schema, build_daily_zone_metrics, month_bounds


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    cfg = load_config()
    spark = build_spark("daily_zone_metrics", cfg)
    spark.sql(tables.daily_zone_metrics_ddl(cfg.daily_zone_metrics))

    start, end = month_bounds(args.year, args.month)
    trips = spark.table(cfg.trips_clean).filter((F.col("pickup_date") >= F.lit(start)) & (F.col("pickup_date") < F.lit(end)))

    metrics = build_daily_zone_metrics(trips).withColumn("computed_at", F.current_timestamp())
    metrics = align_to_schema(metrics, spark.table(cfg.daily_zone_metrics).schema)
    metrics.writeTo(cfg.daily_zone_metrics).overwritePartitions()

    written = spark.table(cfg.daily_zone_metrics).filter(
        (F.col("pickup_date") >= F.lit(start)) & (F.col("pickup_date") < F.lit(end))
    )
    totals = written.agg(F.count(F.lit(1)).alias("rows"), F.sum("trips").alias("trips")).first()
    print(f"daily_zone_metrics: {totals.rows:,} rows covering {totals.trips:,} trips for {start:%Y-%m}")
    written.orderBy(F.desc("total_revenue")).show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

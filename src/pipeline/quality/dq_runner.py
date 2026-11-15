"""Part 5: run the data quality checks for one month against the real tables.

Exits non-zero if any check fails, so it can gate a scheduler or CI step.
Writes results/dq_report.json.

Run: python -m pipeline.quality.dq_runner --year 2025 --month 1
"""
from __future__ import annotations

import argparse
import json

from pyspark.sql import functions as F

from pipeline.config import RESULTS_DIR, load_config
from pipeline.quality import checks
from pipeline.spark_session import build_spark
from pipeline.transform.transformations import REJECT_REASONS, UNKNOWN, month_bounds


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()
    source_month = f"{args.year}-{args.month:02d}"
    start, end = month_bounds(args.year, args.month)

    cfg = load_config()
    spark = build_spark("data_quality", cfg)

    raw = spark.read.parquet(cfg.raw_trips_path(args.year, args.month))
    clean = spark.table(cfg.trips_clean).filter(
        (F.col("pickup_date") >= F.lit(start)) & (F.col("pickup_date") < F.lit(end))
    ).cache()
    rejected = spark.table(cfg.trips_rejected).filter(F.col("source_month") == source_month).cache()
    metrics = spark.table(cfg.daily_zone_metrics).filter(
        (F.col("pickup_date") >= F.lit(start)) & (F.col("pickup_date") < F.lit(end))
    )

    results = [
        checks.check_not_null(clean, ["trip_id"], "trips_clean.trip_id_not_null"),
        checks.check_unique(clean, "trip_id", "trips_clean.trip_id_unique"),
        checks.check_not_null(
            clean,
            ["pickup_datetime", "dropoff_datetime", "pickup_date", "pu_location_id", "do_location_id", "fare_amount"],
            "trips_clean.required_columns_not_null",
        ),
        checks.check_condition(
            clean,
            (F.col("fare_amount") > 0) & (F.col("dropoff_datetime") >= F.col("pickup_datetime")) & (F.col("trip_minutes") >= 0),
            "fare_amount > 0 AND dropoff >= pickup AND trip_minutes >= 0",
            "trips_clean.valid_value_ranges",
        ),
        checks.check_date_in_range(clean, "pickup_date", start, end, "trips_clean.pickup_date_in_month"),
        checks.check_condition(
            clean, F.col("pickup_zone") != F.lit(UNKNOWN), "pickup zone found in lookup",
            "trips_clean.pickup_zone_enriched",
        ),
        checks.check_reconciliation(raw.count(), clean.count(), rejected.count(), "reconciliation.raw_eq_clean_plus_rejected"),
        checks.check_allowed_values(rejected, "reject_reason", REJECT_REASONS, "trips_rejected.reject_reason_valid"),
        checks.check_metrics_match_trips(clean, metrics, "daily_zone_metrics.matches_trips_clean"),
    ]

    width = max(len(r.name) for r in results)
    for r in results:
        print(f"{'PASS' if r.passed else 'FAIL'}  {r.name:<{width}}  {r.detail}")
    RESULTS_DIR.mkdir(exist_ok=True)
    report = {"source_month": source_month, "passed": all(r.passed for r in results),
              "checks": [r.as_dict() for r in results]}
    (RESULTS_DIR / "dq_report.json").write_text(json.dumps(report, indent=2, default=str))
    spark.stop()
    if not report["passed"]:
        raise SystemExit("Data quality checks failed (see results/dq_report.json)")
    print(f"All {len(results)} checks passed")


if __name__ == "__main__":
    main()

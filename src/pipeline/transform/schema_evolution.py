"""Part 3.2: add `is_airport_trip` to trips_clean WITHOUT rewriting data.

Iceberg tracks columns by ID in table metadata, so ADD COLUMN is a
metadata-only commit: no data file is touched and no new snapshot is created.
Existing rows read the new column as NULL. Future loads populate it
(build_trips_clean detects the column), and `--full-refresh` can backfill a
month if you decide the rewrite is worth it.

This script proves the "no rewrite" claim by comparing the set of data files
and the current snapshot ID before and after the ALTER.

Run: python -m pipeline.transform.schema_evolution
"""
from __future__ import annotations

import json

from pipeline.config import RESULTS_DIR, load_config
from pipeline.spark_session import build_spark
from pipeline.transform import tables

COLUMN = "is_airport_trip"


def data_files(spark, table: str) -> set[str]:
    return {r.file_path for r in spark.sql(f"SELECT file_path FROM {table}.files").collect()}


def main() -> None:
    cfg = load_config()
    spark = build_spark("schema_evolution", cfg)
    t = cfg.trips_clean

    files_before = data_files(spark, t)
    snapshot_before = tables.current_snapshot_id(spark, t)
    rows = spark.table(t).count()

    if tables.has_column(spark, t, COLUMN):
        print(f"{COLUMN} already exists; nothing to do (safe to re-run).")
    else:
        spark.sql(
            f"ALTER TABLE {t} ADD COLUMN {COLUMN} BOOLEAN "
            "COMMENT 'pickup or dropoff zone is EWR (1), JFK (132) or LaGuardia (138)'"
        )
        print(f"Added column {COLUMN}")

    files_after = data_files(spark, t)
    snapshot_after = tables.current_snapshot_id(spark, t)
    nulls = spark.sql(f"SELECT count(*) AS n FROM {t} WHERE {COLUMN} IS NULL").first().n

    report = {
        "column_added": COLUMN,
        "rows": rows,
        "data_files_before": len(files_before),
        "data_files_after": len(files_after),
        "data_files_unchanged": files_before == files_after,
        "snapshot_before": snapshot_before,
        "snapshot_after": snapshot_after,
        "snapshot_unchanged": snapshot_before == snapshot_after,
        "rows_with_null_new_column": nulls,
        "schema": [f"{f.name}: {f.dataType.simpleString()}" for f in spark.table(t).schema.fields],
    }
    RESULTS_DIR.mkdir(exist_ok=True)
    (RESULTS_DIR / "schema_evolution_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    if not report["data_files_unchanged"]:
        raise SystemExit("Unexpected: data files changed during ADD COLUMN")
    spark.stop()


if __name__ == "__main__":
    main()

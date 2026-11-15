"""Part 3.3: time travel on trips_clean.

1. History: every snapshot with its operation and the row count AS OF that
   snapshot (VERSION AS OF <snapshot_id>).
2. A visible change: simulate a bad data-correction job that deletes the last
   day of the month. Show the row count before the change, after it, and the
   count read AS OF the earlier snapshot (time travel) - it still sees the old
   rows. Then roll the table back to that snapshot so the real data and the
   raw = clean + rejected reconciliation are intact.

Pass --no-demo-change to only print history.

Run: python -m pipeline.transform.time_travel --year 2025 --month 1
"""
from __future__ import annotations

import argparse
import csv
from datetime import timedelta

from pipeline.config import RESULTS_DIR, load_config
from pipeline.spark_session import build_spark
from pipeline.transform import tables
from pipeline.transform.transformations import month_bounds


def count_as_of(spark, table: str, snapshot_id: int) -> int:
    return spark.sql(f"SELECT count(*) AS n FROM {table} VERSION AS OF {snapshot_id}").first().n


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--no-demo-change", action="store_true")
    args = parser.parse_args()

    cfg = load_config()
    spark = build_spark("time_travel", cfg)
    t = cfg.trips_clean
    RESULTS_DIR.mkdir(exist_ok=True)

    # ---- 1. Snapshot history with counts ----
    history = spark.sql(f"""
        SELECT h.made_current_at, h.snapshot_id, s.operation
        FROM {t}.history h JOIN {t}.snapshots s ON h.snapshot_id = s.snapshot_id
        ORDER BY h.made_current_at
    """).collect()
    history_rows = [
        {"made_current_at": str(r.made_current_at), "snapshot_id": r.snapshot_id,
         "operation": r.operation, "row_count": count_as_of(spark, t, r.snapshot_id)}
        for r in history
    ]
    print("Snapshot history:")
    for r in history_rows:
        print(f"  {r['made_current_at']}  {r['snapshot_id']}  {r['operation']:<10} rows={r['row_count']:,}")
    with (RESULTS_DIR / "time_travel_history.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(history_rows[0]))
        writer.writeheader()
        writer.writerows(history_rows)

    if args.no_demo_change:
        spark.stop()
        return

    # ---- 2. Change -> time travel -> rollback ----
    _, next_month = month_bounds(args.year, args.month)
    last_day = next_month - timedelta(days=1)

    before_id = tables.current_snapshot_id(spark, t)
    before_count = spark.table(t).count()

    print(f"\nSimulating a bad change: DELETE trips with pickup_date = {last_day}")
    spark.sql(f"DELETE FROM {t} WHERE pickup_date = DATE '{last_day}'")
    after_id = tables.current_snapshot_id(spark, t)
    after_count = spark.table(t).count()

    as_of_before = count_as_of(spark, t, before_id)
    deleted_day_as_of = spark.sql(
        f"SELECT count(*) AS n FROM {t} VERSION AS OF {before_id} WHERE pickup_date = DATE '{last_day}'"
    ).first().n

    print(f"Rolling back to snapshot {before_id}")
    spark.sql(f"CALL polaris.system.rollback_to_snapshot('{tables.spark_table_ident(t)}', {before_id})")
    restored_count = spark.table(t).count()

    report = [
        {"step": "before change (current)", "snapshot_id": before_id, "row_count": before_count},
        {"step": f"after DELETE {last_day} (current)", "snapshot_id": after_id, "row_count": after_count},
        {"step": "time travel: VERSION AS OF before-snapshot", "snapshot_id": before_id, "row_count": as_of_before},
        {"step": f"time travel: rows on {last_day} AS OF before-snapshot", "snapshot_id": before_id,
         "row_count": deleted_day_as_of},
        {"step": "after rollback_to_snapshot (current)", "snapshot_id": tables.current_snapshot_id(spark, t),
         "row_count": restored_count},
    ]
    with (RESULTS_DIR / "time_travel_report.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["step", "snapshot_id", "row_count"])
        writer.writeheader()
        writer.writerows(report)
    print()
    for r in report:
        print(f"  {r['step']:<55} snapshot={r['snapshot_id']}  rows={r['row_count']:,}")

    assert as_of_before == before_count, "time travel count should equal the pre-change count"
    assert restored_count == before_count, "rollback should restore the pre-change count"
    spark.stop()


if __name__ == "__main__":
    main()

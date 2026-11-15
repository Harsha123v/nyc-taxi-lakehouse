# Results

Files in this folder are produced by running the pipeline. Commit them after your run.

| File | Produced by | Shows |
|---|---|---|
| `load_summary_2025-01.json` | `make transform` | raw / clean / rejected counts, rejects by reason, MERGE before/after counts |
| `schema_evolution_report.json` | `make schema-evolution` | data files and snapshot unchanged by ADD COLUMN |
| `time_travel_history.csv` | `make time-travel` | every snapshot with its row count |
| `time_travel_report.csv` | `make time-travel` | count before change, after change, AS OF earlier snapshot, after rollback |
| `dq_report.json` | `make dq` | pass/fail of each data quality check |
| `q1_top10_pickup_zones_by_revenue.csv` | `make snowflake-queries` | Snowflake Q1 |
| `q2_trip_demand_by_hour.csv` | `make snowflake-queries` | Snowflake Q2 |
| `q3_avg_tip_pct_by_payment_type.csv` | `make snowflake-queries` | Snowflake Q3 |

Optionally add Snowsight screenshots here as `snowflake_q1.png` etc.

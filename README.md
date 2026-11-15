# NYC Taxi Lakehouse: S3 → Spark → Iceberg (Polaris) → Snowflake

A pipeline that turns one month of NYC TLC yellow-taxi trips into clean, enriched Iceberg tables and a daily zone metrics table, readable from Snowflake, with data quality checks and unit tests.

```
TLC website ──Part 1──▶ S3 raw/ ──Part 2──▶ Iceberg trips_clean ──Part 3──▶ Iceberg daily_zone_metrics
 (Parquet + CSV)        (as downloaded)     + trips_rejected (quarantine)
                                                │                                   │
                              Apache Polaris (REST catalog, Docker) tracks both tables
                                                │                                   │
                                                └──── metadata.json on S3 ─────────▶ Snowflake (Part 4)
                                                                                     external volume +
                                                                                     object-store catalog integration
```

| Layer | Location | Contents |
|---|---|---|
| Raw | `s3://$BUCKET/raw/yellow_trips/year=2025/month=01/` and `raw/taxi_zones/` | Files exactly as downloaded |
| Clean | Iceberg `nyc_taxi.trips_clean` (partitioned by `pickup_date`) | Validated, deduplicated, enriched trips |
| Quarantine | Iceberg `nyc_taxi.trips_rejected` (partitioned by `source_month`) | Rejected rows + `reject_reason` |
| Metrics | Iceberg `nyc_taxi.daily_zone_metrics` (partitioned by `pickup_date`) | Trips, revenue, avg tip %, avg minutes per day and pickup zone |

Iceberg data and metadata live under `s3://$BUCKET/warehouse/`.

---

## Prerequisites

- Python 3.10+, Java 11 or 17 (for Spark 3.5), Docker, AWS CLI v2 (2.13+, for `aws configure export-credentials`)
- An AWS account with an **admin IAM user or role** (not root) for one-time setup
- A Snowflake trial account, **created on AWS in the same region as your bucket** (this repo assumes `us-east-1`)

```bash
cp .env.example .env          # fill in BUCKET, AWS_ACCOUNT_ID, POLARIS_ROOT_CLIENT_SECRET, ...
python -m venv .venv && source .venv/bin/activate
make install
make test                     # unit tests run fully offline
```

## Setup (once)

```bash
ADMIN_PROFILE=<your-admin-profile> make setup-aws
```

This creates the private bucket (public access blocked, SSE-S3 encryption), and three least-privilege identities:

| Identity | Can do | Used by |
|---|---|---|
| IAM user `nyc-taxi-pipeline` | read/write `raw/*` and `warehouse/*` only; assume `nyc-taxi-polaris` | ingest script, Spark |
| IAM role `nyc-taxi-polaris` | read/write `warehouse/*` only | Polaris (writes Iceberg metadata) |
| IAM role `nyc-taxi-snowflake-read` | read-only `warehouse/*` | Snowflake external volume |

Then create an access key for the pipeline user and store it **only** in your local AWS profile, as the script prints:

```bash
aws --profile <admin> iam create-access-key --user-name nyc-taxi-pipeline
aws configure --profile nyc-taxi-pipeline
```

Start and configure Polaris:

```bash
make polaris-up           # apache/polaris in Docker on :8181
make polaris-bootstrap    # creates catalog, principal spark_etl, roles; writes .polaris.env
```

Spark talks to Polaris as the `spark_etl` principal, whose catalog role has only `CATALOG_MANAGE_CONTENT` (manage namespaces/tables, no catalog admin).

---

## Running each part

Each part is a single command. Defaults: `YEAR=2025 MONTH=1` (override, e.g. `make transform MONTH=2`).

### Part 1: Ingest to S3
```bash
make ingest
```
Downloads the month's trip Parquet and the zone CSV and uploads them unchanged to `raw/`. **Re-run safe:** keys are deterministic, so a re-run overwrites the same object rather than adding one, and each object stores a sha256 in its metadata so identical content is skipped.

### Part 2: Transform with Spark into Iceberg
```bash
make transform
```
PySpark, run locally (`src/pipeline/transform/build_trips_clean.py`):

- **Clean** — each row gets the first failing reason, or none:
  `null_pickup_or_dropoff`, `dropoff_before_pickup`, `non_positive_fare` (≤ 0 or NULL), `outside_month` (pickup date not in the chosen month), `exact_duplicate` (identical across all columns, first copy kept).
- **Enrich** — `trip_minutes`, `avg_mph`, `tip_pct`, pickup/dropoff borough and zone names from the lookup (broadcast left join), and a deterministic `trip_id` (sha256 of the raw record).
- **Write** — `trips_clean`, partitioned by `pickup_date`, registered in **Apache Polaris**. Rejected rows go to `trips_rejected` with `reject_reason`.

### Part 3: Iceberg features

| Feature | Command | How |
|---|---|---|
| 1. Idempotent load | `make transform` (run it again) | `MERGE INTO trips_clean ON trip_id ... WHEN NOT MATCHED THEN INSERT *`. A second run inserts 0 rows: compare `trips_clean_rows_for_month_before/after` in `results/load_summary_2025-01.json`. |
| 2. Schema evolution | `make schema-evolution` | `ALTER TABLE ... ADD COLUMN is_airport_trip BOOLEAN`. Metadata-only: the script proves the data-file list and current snapshot are unchanged. Old rows read NULL, later loads populate it. |
| 3. Time travel | `make time-travel` | Lists every snapshot with its row count (`VERSION AS OF`), then simulates a bad change (deletes the last day of the month), shows counts before, after, and **as of the earlier snapshot**, then `rollback_to_snapshot` restores the table. |
| 4. Metrics table | `make metrics` | Builds `daily_zone_metrics` (trips, total revenue, avg tip %, avg trip minutes by pickup date and pickup zone) with dynamic partition overwrite, so re-runs replace rather than append. |

`make all` runs Parts 1–3 and the DQ checks in order (it runs `transform` twice to demonstrate idempotency).

### Part 4: Query in Snowflake

Polaris runs on localhost, which Snowflake cannot reach, so Snowflake uses an **external volume on the bucket plus an `OBJECT_STORE` catalog integration** and reads each table's current `metadata.json` directly. Polaris stays the only writer; Snowflake is a read-only consumer through a read-only IAM role.

1. `make snowflake-sql` → writes `snowflake/generated/01_setup.sql` and `02_tables.sql` (with each table's current metadata file).
2. Run `01_setup.sql` in Snowsight. From `DESC EXTERNAL VOLUME`, copy `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`, then:
   ```bash
   ADMIN_PROFILE=<admin> bash infra/update_snowflake_trust.sh <IAM_USER_ARN> <EXTERNAL_ID>
   ```
   Re-run the `SYSTEM$VERIFY_EXTERNAL_VOLUME` line; it should report success.
3. Run `02_tables.sql` in Snowsight.
4. `make snowflake-queries` runs `snowflake/queries.sql` and saves each answer to `results/*.csv` (or run the file in Snowsight and download the CSVs / take screenshots).

After any later pipeline run, repeat `make snowflake-sql` and run `02_tables.sql` again; its `ALTER ICEBERG TABLE ... REFRESH` moves Snowflake to the newest snapshot.

`queries.sql` answers: top 10 pickup zones by revenue, trip demand by hour of day, and average tip % by payment type.

### Part 5: Data quality and tests
```bash
make dq      # 9 checks on the real tables; exits non-zero on any failure
make test    # pytest unit tests, no cloud needed
```

DQ checks (`results/dq_report.json`): `trip_id` not null; `trip_id` unique; required columns not null; valid ranges (fare > 0, dropoff ≥ pickup, minutes ≥ 0); pickup date within month; pickup zone found in lookup; **raw = clean + rejected**; reject reasons from the allowed set; metrics add back up to `trips_clean` (counts exact, revenue within rounding).

Unit tests cover every reject rule and its precedence, duplicate handling, reconciliation, enrichment math, zone join (including unknown IDs), the airport flag, `trip_id` stability, metrics aggregation, schema alignment, the DQ check functions, ingest idempotency (with a fake S3 client), and the SQL file parser.

---

## Assumptions

Where the brief left room for interpretation, these are the choices made:

- **Month membership** is decided by **pickup** date. A trip picked up on Jan 31 at 23:59 and dropped off Feb 1 belongs to January.
- **Timestamps** in TLC files are NYC local wall-clock times with no zone. Spark runs with session time zone UTC so values are never shifted, and Snowflake queries set `TIMEZONE = 'UTC'` so the hour of day matches what TLC recorded.
- **Zero-length trips** (dropoff = pickup) are kept; only dropoff **before** pickup is rejected. Their `avg_mph` is NULL instead of a divide-by-zero.
- **`tip_pct`** = `tip_amount / fare_amount × 100`. TLC records only card tips, so cash tips show as ~0%.
- **Revenue** = `total_amount` (everything the rider paid: fare, tips, tolls, taxes, surcharges).
- **`avg_tip_pct`** in the metrics table is the mean of per-trip percentages.
- **Exact duplicates** are rows identical in every column; one copy is kept, the others are quarantined.
- **`trip_id`**: TLC has no trip key, so it is a sha256 of all raw columns. It is unique once exact duplicates are removed and identical across runs, which is what makes `MERGE` idempotent.
- **MERGE is insert-only**: a matched `trip_id` means the same raw record, so there is nothing to update. If enrichment logic changes, `python -m pipeline.transform.build_trips_clean ... --full-refresh` overwrites the month's partitions.
- **`is_airport_trip`** is true when pickup or dropoff zone is EWR (1), JFK (132) or LaGuardia (138). Existing rows are left NULL to honor "without rewriting existing data". A `--full-refresh` backfills them if wanted.
- **Unknown zone IDs** get `Unknown` borough/zone rather than being dropped.
- **Snowflake integration**: object-store catalog integration (not a REST integration to Polaris), because Snowflake cannot reach a catalog on localhost.
- **Polaris persistence** is in-memory, which is fine for a review. See below.
- **Spark runs locally** (PySpark), not as a Glue job.

## Security

- No credentials in the repo. `.env`, `.polaris.env` and rendered IAM policies are gitignored. AWS keys live only in your local AWS profile, and the Makefile exports short-lived credentials into each command's environment.
- Root credentials are never used; `setup_aws.sh` refuses to run as root.
- Every identity is scoped to the prefixes it needs (table above). The bucket blocks all public access and encrypts at rest.
- Only the public TLC dataset is used.

## Restarting Polaris

Polaris uses in-memory persistence, so `docker compose down` forgets the catalog (S3 data is untouched). After a restart, run `make polaris-bootstrap`, then re-register each table from its latest metadata file, e.g. in a PySpark shell built with `pipeline.spark_session.build_spark`:

```sql
CALL polaris.system.register_table('nyc_taxi.trips_clean', 's3://<bucket>/warehouse/nyc_taxi/trips_clean/metadata/<latest>.metadata.json')
```

## Cleanup (after review)

```bash
# Snowflake: run snowflake/teardown.sql
make polaris-down
ADMIN_PROFILE=<admin> make teardown     # deletes bucket + contents, IAM user, keys, roles (asks to confirm)
```

## Project layout

```
infra/                  AWS setup/teardown scripts and least-privilege IAM policy templates
docker-compose.yml      Polaris
src/pipeline/
  config.py             paths, table names, settings from .env
  spark_session.py      Spark + Iceberg + Polaris + S3 configuration
  polaris_bootstrap.py  catalog, principal, roles
  ingest/               Part 1
  transform/            Parts 2-3 (transformations.py holds all pure logic)
  snowflake/            Part 4 SQL generation and query runner
  quality/              Part 5 checks and runner
snowflake/              setup template, queries.sql, teardown.sql, generated/
tests/                  pytest unit tests
results/                outputs of a run (CSV / JSON), committed for review
docs/WALKTHROUGH.md     design notes for the follow-up call
```

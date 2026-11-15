# Walkthrough for the follow-up call

Notes to explain the design and every moving part. Read alongside the code.

## The 60-second summary

"I land the TLC files unchanged in S3 (raw). A PySpark job standardizes the columns, tags every row with the first rule it fails, writes failures to a quarantine Iceberg table and merges the clean, enriched rows into `trips_clean`, keyed by a hash of the raw record so re-runs never duplicate. Polaris, running in Docker, is the Iceberg REST catalog. I then demonstrate schema evolution, time travel with rollback, and build a daily zone metrics table. Snowflake reads the same Iceberg files through an external volume. Nine DQ checks, including raw = clean + rejected, gate the run, and the transformation logic is unit-tested without any cloud dependency."

## Design decisions and why

**Pure functions vs jobs.** All logic is in `transform/transformations.py` and `quality/checks.py` and takes/returns DataFrames. Jobs only read and write. That is why the tests need nothing but a local Spark.

**Reject reason with `F.when` chain.** `add_reject_reason` evaluates rules in order, so each row gets exactly one reason (the first it fails). Every raw row ends up in exactly one place, which is what makes the reconciliation check exact.

**Duplicates with a window.** `row_number()` over a window partitioned by all 20 columns: copy 1 is kept, copies 2+ are `exact_duplicate`. Because copies are identical, if copy 1 fails a rule all copies fail the same rule, so no copy is wrongly kept.

**`trip_id`.** TLC has no key. sha256 of all columns (NULLs replaced by a marker so `(NULL,'a')` ≠ `('a',NULL)`). Deterministic, so the same record always gets the same ID, which `MERGE` uses.

**Insert-only MERGE.** `ON t.trip_id = s.trip_id AND t.pickup_date = s.pickup_date`. The `pickup_date` condition lets Iceberg prune to matching partitions. A match means the identical raw record, so there is no update clause. `--full-refresh` exists for when logic changes.

**Quarantine idempotency.** `trips_rejected` is partitioned by `source_month` and written with `overwritePartitions()` (dynamic overwrite), so a re-run replaces the month instead of appending.

**Schema evolution.** Iceberg identifies columns by ID in metadata, not by position in files. `ADD COLUMN` writes a new metadata.json only: no data files change, no new snapshot. The script compares `.files` and the `main` ref before/after to prove it. Old files simply don't contain the column ID, so readers return NULL.

**`align_to_schema`.** `INSERT *` requires the source to match the target columns. After the column is added, this function projects the source onto the table schema (order + types, missing columns as typed NULLs), so the same job works before and after evolution.

**Time travel.** Every commit creates a snapshot. `VERSION AS OF <id>` reads the table as of that snapshot. The demo deletes a day, shows the old count is still readable via time travel, then `rollback_to_snapshot` moves `main` back. Data files from the "bad" snapshot are not deleted until snapshots are expired.

**Why read `refs` for the current snapshot?** After a rollback, the newest committed snapshot is not the current one. The `main` ref is authoritative.

**Snowflake via object store.** Snowflake can't reach localhost Polaris. Iceberg's metadata.json fully describes a table version, so Snowflake reads it from S3 via the external volume. `ALTER ICEBERG TABLE ... REFRESH '<new metadata path>'` advances it. With a hosted catalog (Snowflake Open Catalog / Polaris with a public endpoint), I'd switch to a REST catalog integration and get automatic refresh.

**Timezones.** TLC timestamps are local wall-clock values without zone. Spark session TZ = UTC means no shifting on read or write; Snowflake's session TZ is set to UTC so `HOUR()` returns the recorded hour.

**Security.** Three identities, each scoped to prefixes: the pipeline user (raw + warehouse), the Polaris role (warehouse only, assumed by the local Polaris container), the Snowflake role (read-only warehouse, trust bound to Snowflake's IAM user + external ID). No keys in the repo; the Makefile exports temporary credentials per command.

## Likely questions

- **How would you schedule this?** Airflow/Step Functions running ingest → transform → metrics → dq → snowflake refresh, with DQ failure stopping the downstream steps.
- **How would it scale to years of data?** Run per month in parallel; partition pruning keeps MERGE cheap; move to Glue/EMR; compact small files with `rewrite_data_files` and run `expire_snapshots` / `remove_orphan_files`.
- **Late-arriving data?** Re-run the month: MERGE inserts only new records; metrics overwrite the month's partitions.
- **Why copy-on-write?** Simpler files for Snowflake and readers; writes are mostly appends. For frequent updates I'd consider merge-on-read with periodic compaction.
- **Why left join zones?** Never silently drop trips. Unknown IDs get `Unknown` and a DQ check reports them.
- **What does the DQ reconciliation catch?** Any row lost or duplicated between raw and the two output tables, e.g. a bad delete (try running the time-travel DELETE without rollback: the check fails).
- **Why not keep `trip_id` from the source?** There isn't one in TLC data.
- **Limits.** Polaris in-memory persistence (use its JDBC/Postgres backend for real use); Snowflake refresh is manual; double for money (decimal would be exact).

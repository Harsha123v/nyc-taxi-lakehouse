-- Template rendered by `make snowflake-sql` into snowflake/generated/01_setup.sql
-- (fills in ${BUCKET} and ${AWS_ACCOUNT_ID}). Run the rendered file in Snowsight.
--
-- Why an OBJECT_STORE catalog integration?
--   Polaris runs on localhost, which Snowflake cannot reach. Iceberg keeps the
--   full table state in metadata.json files on S3, so Snowflake reads those
--   directly through an external volume. Pointing a table at a newer
--   metadata.json (ALTER ICEBERG TABLE ... REFRESH) picks up new snapshots.
--   Polaris stays the single writer/catalog; Snowflake is a read-only consumer.

USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS NYC_TAXI_WH
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS NYC_TAXI;
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.ANALYTICS;

-- Read-only access to the Iceberg warehouse prefix through the
-- nyc-taxi-snowflake-read IAM role (least privilege: Get/List on warehouse/ only).
CREATE EXTERNAL VOLUME IF NOT EXISTS NYC_TAXI_VOL
  STORAGE_LOCATIONS = ((
    NAME = 'nyc-taxi-warehouse'
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://${BUCKET}/warehouse/'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::${AWS_ACCOUNT_ID}:role/nyc-taxi-snowflake-read'
  ))
  ALLOW_WRITES = FALSE;

-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from the
-- STORAGE_LOCATION_1 row, then on your machine run:
--   ADMIN_PROFILE=<admin> bash infra/update_snowflake_trust.sh <IAM_USER_ARN> <EXTERNAL_ID>
DESC EXTERNAL VOLUME NYC_TAXI_VOL;

-- After the trust policy is updated this should return "success": true.
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('NYC_TAXI_VOL');

CREATE CATALOG INTEGRATION IF NOT EXISTS NYC_TAXI_OBJECT_STORE
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = ICEBERG
  ENABLED = TRUE;

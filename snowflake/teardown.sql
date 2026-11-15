-- Remove every Snowflake object this project created. Run after the review.
-- Iceberg tables here are read-only views of S3 data: dropping them does not
-- touch S3 (delete S3 with infra/teardown_aws.sh).
USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS NYC_TAXI;
DROP CATALOG INTEGRATION IF EXISTS NYC_TAXI_OBJECT_STORE;
DROP EXTERNAL VOLUME IF EXISTS NYC_TAXI_VOL;
DROP WAREHOUSE IF EXISTS NYC_TAXI_WH;

"""Builds a SparkSession wired to the Polaris Iceberg REST catalog and S3.

- Iceberg tables live in the Spark catalog named `polaris`, backed by the
  Polaris REST API. Spark authenticates with the least-privilege `spark_etl`
  principal created by polaris_bootstrap.py (OAuth2 client credentials).
- Iceberg data/metadata I/O uses S3FileIO with the AWS default credential
  chain (the pipeline user's credentials exported by the Makefile).
- Raw files are read via Hadoop's s3a connector.
- Session time zone is UTC so TLC timestamps (local NYC wall-clock values with
  no zone) are never shifted. See README "Assumptions".
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from pipeline.config import Config, load_config, require

ICEBERG_VERSION = "1.9.1"
SPARK_SCALA = "3.5_2.12"
HADOOP_AWS_VERSION = "3.3.4"  # must match the Hadoop version bundled with pyspark 3.5

PACKAGES = [
    f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_SCALA}:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
]


def build_spark(app_name: str, cfg: Config | None = None) -> SparkSession:
    cfg = cfg or load_config()
    require(cfg.bucket, "BUCKET")
    client_id = require(cfg.polaris_client_id, "POLARIS_CLIENT_ID")
    client_secret = require(cfg.polaris_client_secret, "POLARIS_CLIENT_SECRET")

    cat = "spark.sql.catalog.polaris"
    spark = (
        SparkSession.builder.appName(app_name)
        .master(cfg.spark_master)
        .config("spark.driver.memory", cfg.spark_driver_memory)
        .config("spark.jars.packages", ",".join(PACKAGES))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "32")
        # --- Polaris (Iceberg REST catalog) ---
        .config(cat, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{cat}.type", "rest")
        .config(f"{cat}.uri", cfg.polaris_uri)
        .config(f"{cat}.warehouse", cfg.polaris_catalog)
        .config(f"{cat}.credential", f"{client_id}:{client_secret}")
        .config(f"{cat}.scope", "PRINCIPAL_ROLE:ALL")
        .config(f"{cat}.oauth2-server-uri", f"{cfg.polaris_uri}/v1/oauth/tokens")
        .config(f"{cat}.token-refresh-enabled", "true")
        .config(f"{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{cat}.client.region", cfg.aws_region)
        # --- s3a for reading the raw layer ---
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{cfg.aws_region}.amazonaws.com")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

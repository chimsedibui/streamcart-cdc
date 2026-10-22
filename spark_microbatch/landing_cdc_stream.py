"""
Reads every topic matching `cdc.ecommerce.*` from the Kafka buffer and appends
the raw records (Debezium envelope untouched, as JSON strings) into a single
append-only Iceberg table `<catalog>.landing.cdc_events`, partitioned by
source table and event day. Downstream jobs parse the envelope from there.

All settings are overridable via environment variables (see below), so the
job runs unchanged inside docker-compose or from the host.
"""

import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_PATTERN = os.getenv("CDC_TOPIC_PATTERN", r"cdc\.ecommerce\..*")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")
MAX_OFFSETS_PER_TRIGGER = os.getenv("MAX_OFFSETS_PER_TRIGGER", "50000")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")

CATALOG = os.getenv("ICEBERG_CATALOG", "lakehouse")
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://polaris:8181/api/catalog")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "lakehouse")   # the Polaris catalog name
# Polaris principal as clientId:clientSecret (written by polaris-init)
ICEBERG_CREDENTIAL_FILE = os.getenv("ICEBERG_CREDENTIAL_FILE", "/run/secrets/polaris/spark")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")

LANDING_TABLE = f"{CATALOG}.landing.cdc_events"
CHECKPOINT_DIR = os.getenv(
    "CHECKPOINT_DIR", "/opt/spark/work-dir/checkpoints/landing_cdc_events"
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("landing-cdc-to-iceberg")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", ICEBERG_REST_URI)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config(f"spark.sql.catalog.{CATALOG}.credential", open(ICEBERG_CREDENTIAL_FILE).read().strip())
        .config(f"spark.sql.catalog.{CATALOG}.scope", "PRINCIPAL_ROLE:ALL")
        .config(
            f"spark.sql.catalog.{CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", S3_ENDPOINT)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config("spark.sql.shuffle.partitions", "4")
        # The Kafka source declares every column nullable; topic, partition, offset and timestamp
        # are never null, so the table keeps them NOT NULL and the write skips that check.
        .config("spark.sql.iceberg.check-nullability", "false")
        .getOrCreate()
    )


def ensure_landing_table(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.landing")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {LANDING_TABLE} (
            topic            STRING    NOT NULL,
            source_table     STRING,
            kafka_partition  INT       NOT NULL,
            kafka_offset     BIGINT    NOT NULL,
            kafka_timestamp  TIMESTAMP NOT NULL,
            record_key       STRING,
            record_value     STRING,
            ingested_at      TIMESTAMP NOT NULL
        )
        USING iceberg
        PARTITIONED BY (source_table, days(kafka_timestamp))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd'
        )
        """
    )


def main() -> None:
    spark = build_spark()
    ensure_landing_table(spark)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Keep the Debezium envelope raw; record_value is NULL for delete
    # tombstones, which downstream compaction/parsing must expect.
    landing = raw.select(
        F.col("topic"),
        F.regexp_extract("topic", r"^cdc\.ecommerce\.(.+)$", 1).alias("source_table"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("record_key"),
        F.col("value").cast("string").alias("record_value"),
        F.current_timestamp().alias("ingested_at"),
    )

    query = (
        landing.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .option("fanout-enabled", "true")
        .toTable(LANDING_TABLE)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

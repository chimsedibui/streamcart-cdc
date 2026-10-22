# Spark landing layer (Kafka → Iceberg on SeaweedFS)

`landing_cdc_stream.py` is a Structured Streaming micro-batch job that reads
every Debezium topic matching `cdc.ecommerce.*` and appends the **raw**
records (Debezium envelope kept as JSON strings) to the Iceberg table
`lakehouse.landing.cdc_events`, stored in the SeaweedFS S3 bucket `s3://warehouse/`
via the Iceberg REST catalog.

## Landing table

| Column | Meaning |
| --- | --- |
| `topic` | Kafka topic the record came from |
| `source_table` | Table name extracted from the topic (`cdc.ecommerce.<table>`) |
| `kafka_partition` / `kafka_offset` | Exact position in the buffer (dedup key) |
| `kafka_timestamp` | Broker append time |
| `record_key` / `record_value` | Raw JSON key and Debezium envelope (`record_value` is NULL for delete tombstones) |
| `ingested_at` | When Spark landed the row |

Partitioned by `source_table` and `days(kafka_timestamp)`. Micro-batches fire
every 30 s (`TRIGGER_INTERVAL`), capped at 50 000 records per trigger.

## Run

```bash
docker compose up -d spark-landing   # pulls seaweedfs and iceberg-rest with it
docker logs -f cdc-spark-landing     # first start downloads Iceberg/Kafka jars from Maven
```

Endpoints: SeaweedFS S3 API <http://localhost:8333> (access key admin / secret admin123),
Iceberg REST catalog <http://localhost:8181>.

## Inspect the landed data

```bash
docker exec -it cdc-spark-landing /opt/spark/bin/spark-sql \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,org.apache.iceberg:iceberg-aws-bundle:1.9.1 \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=rest \
  --conf spark.sql.catalog.lakehouse.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.lakehouse.warehouse=s3://warehouse/ \
  --conf spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.lakehouse.s3.endpoint=http://seaweedfs:8333 \
  --conf spark.sql.catalog.lakehouse.s3.path-style-access=true \
  -e "SELECT source_table, count(*) FROM lakehouse.landing.cdc_events GROUP BY source_table"
```

## Configuration (env vars on the `spark-landing` service)

| Variable | Default |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` |
| `CDC_TOPIC_PATTERN` | `cdc\.ecommerce\..*` |
| `STARTING_OFFSETS` | `earliest` |
| `MAX_OFFSETS_PER_TRIGGER` | `50000` |
| `TRIGGER_INTERVAL` | `30 seconds` |
| `ICEBERG_CATALOG` | `lakehouse` |
| `ICEBERG_REST_URI` | `http://iceberg-rest:8181` |
| `ICEBERG_WAREHOUSE` | `s3://warehouse/` |
| `S3_ENDPOINT` | `http://seaweedfs:8333` |
| `CHECKPOINT_DIR` | `/opt/spark/work-dir/checkpoints/landing_cdc_events` |

Checkpoints live in the `spark_checkpoints` volume, so restarts resume from
the last committed offsets. To rebuild the landing table from scratch, drop
the table and remove that volume together, otherwise you get duplicates or
gaps.

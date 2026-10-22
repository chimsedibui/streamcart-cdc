# Spark landing layer (Kafka → Iceberg on SeaweedFS)

`landing_cdc_stream.py` is a Structured Streaming micro-batch job that reads
every Debezium topic matching `cdc.ecommerce.*` and appends the **raw**
records (Debezium envelope kept as JSON strings) to the Iceberg table
`lakehouse.landing.cdc_events`, stored in the SeaweedFS S3 bucket `s3://warehouse/` and registered in the Polaris catalog `lakehouse`
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
docker compose up -d spark-landing   # pulls seaweedfs and polaris with it
docker logs -f cdc-spark-landing     # first start downloads Iceberg/Kafka jars from Maven
```

Endpoints: SeaweedFS S3 API <http://localhost:8333> (access key admin / secret admin123),
Polaris Iceberg REST catalog <http://localhost:8181/api/catalog>. The job signs in to Polaris as the principal
`spark` (`secrets/polaris/spark`, written by `polaris-init`).

## Inspect the landed data

```bash
lakehouse/spark-sql.sh -e "SELECT source_table, count(*) FROM lakehouse.landing.cdc_events GROUP BY source_table"
```

`lakehouse/spark-sql.sh` runs Spark SQL in this container as the read-only Polaris principal `analyst`.

## Configuration (env vars on the `spark-landing` service)

| Variable | Default |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` |
| `CDC_TOPIC_PATTERN` | `cdc\.ecommerce\..*` |
| `STARTING_OFFSETS` | `earliest` |
| `MAX_OFFSETS_PER_TRIGGER` | `50000` |
| `TRIGGER_INTERVAL` | `30 seconds` |
| `ICEBERG_CATALOG` | `lakehouse` |
| `ICEBERG_REST_URI` | `http://polaris:8181/api/catalog` |
| `ICEBERG_WAREHOUSE` | `lakehouse` (Polaris catalog name) |
| `ICEBERG_CREDENTIAL_FILE` | `/run/secrets/polaris/spark` |
| `S3_ENDPOINT` | `http://seaweedfs:8333` |
| `CHECKPOINT_DIR` | `/opt/spark/work-dir/checkpoints/landing_cdc_events` |

Checkpoints live in the `spark_checkpoints` volume, so restarts resume from
the last committed offsets. To rebuild the landing table from scratch, drop
the table and remove that volume together, otherwise you get duplicates or
gaps.

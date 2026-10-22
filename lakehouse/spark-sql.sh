#!/usr/bin/env bash
# Spark SQL against the lakehouse catalog, inside the spark-landing container (its Ivy cache already holds the jars).
#   lakehouse/spark-sql.sh -e "SELECT count(*) FROM lakehouse.silver.orders"
#   lakehouse/spark-sql.sh                  # interactive shell
set -euo pipefail
tty=$([[ -t 0 && $# -eq 0 ]] && echo -it || echo -i)
exec docker exec $tty cdc-spark-landing /opt/spark/bin/spark-sql \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1,org.apache.iceberg:iceberg-aws-bundle:1.9.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=rest \
  --conf spark.sql.catalog.lakehouse.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.lakehouse.warehouse=s3://warehouse/ \
  --conf spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.lakehouse.s3.endpoint=http://seaweedfs:8333 \
  --conf spark.sql.catalog.lakehouse.s3.path-style-access=true \
  --conf spark.ui.enabled=false \
  -S "$@" 2> >(grep -E 'Error|Exception' >&2)   # -S: results only; startup logs dropped, errors kept

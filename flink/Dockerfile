# Flink 1.20 with the Kafka SQL connector, the Iceberg Flink runtime (REST catalog + S3FileIO) and the shaded Hadoop
# client Iceberg's Flink module needs on the classpath. Every jar is pinned by SHA-256.
FROM flink:1.20.5-scala_2.12-java17
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eu; cd /opt/flink/lib; \
    fetch() { curl -sSfL -o "$(basename "$1")" "$MAVEN/$1" && echo "$2  $(basename "$1")" | sha256sum -c -; }; \
    fetch org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar \
          1086f3eee73d727e234860fcd03adafc0d76f2fc70a25d39c36427693fff749d; \
    fetch org/apache/iceberg/iceberg-flink-runtime-1.20/1.9.1/iceberg-flink-runtime-1.20-1.9.1.jar \
          06dc2cceae731028111404eed6193ae89c9317a788eca712e3689357e2527d28; \
    fetch org/apache/iceberg/iceberg-aws-bundle/1.9.1/iceberg-aws-bundle-1.9.1.jar \
          ea5b6fec1b547e2fe936f804c737943f5ee322d038560f63a8261deb766def1b; \
    fetch org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar \
          f3d2347a6e1c6885d5bcfd4f60c3ac3810ec11068fc161e04329baabf412d963; \
    fetch org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar \
          15f01bc804294df06d2effc87de363a83cf589f50558bdbf48f72541ad8de854
# a new named volume takes this directory's owner, so the flink user can write checkpoints
RUN mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints
COPY --chown=flink:flink sql /opt/flink/sql
COPY --chown=flink:flink submit.sh /opt/flink/submit.sh

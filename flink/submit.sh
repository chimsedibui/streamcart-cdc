#!/bin/sh
# Submits flink/sql/silver.sql to the session cluster unless a job of that name is already running, so the
# flink-silver service can run on every `docker compose up` without starting a second copy. After a restart the
# job resumes from the Kafka offsets committed at its last checkpoint; replayed changes are absorbed by the upsert.
set -eu
REST="http://${FLINK_JOBMANAGER:-flink-jobmanager}:8081"
NAME=silver-cdc

until curl -sf "$REST/overview" >/dev/null; do sleep 2; done
if curl -sf "$REST/jobs/overview" | grep -q "\"name\":\"$NAME\",\"state\":\"\(RUNNING\|CREATED\|RESTARTING\)\""; then
  echo "$NAME is already running"
  exit 0
fi
exec /opt/flink/bin/sql-client.sh -Drest.address="${FLINK_JOBMANAGER:-flink-jobmanager}" -f /opt/flink/sql/silver.sql

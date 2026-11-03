"""Connections the platform jobs use: Trino (the SQL engine for gold, checks and maintenance), the ops schema in
platform-db (check results and publishes, charted in Grafana) and Kafka (silver freshness).

Defaults fit a shell on the host; the Dagster containers set the in-network addresses.
"""

import os
import time
from urllib.parse import urlparse

import trino

TRINO_URL = os.getenv("TRINO_URL", "http://localhost:8090")
# The only Trino user lakehouse_rw lets in (lakehouse/trino/rules.json)
TRINO_USER = os.getenv("TRINO_USER", "dagster")
# postgresql://platform:<password>@platform-db:5432/platform (orchestration/entrypoint.sh); unset: no ops rows
OPS_DSN = os.getenv("OPS_DSN")
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


class Trino:
    """A Trino session on one catalog; lakehouse_rw by default, where silver.x and gold.x name the lakehouse tables."""

    def __init__(self, catalog: str = "lakehouse_rw", user: str = TRINO_USER):
        url = urlparse(TRINO_URL)
        # request_timeout: one HTTP round trip; a busy coordinator can take longer than the client's 30 s default
        self.conn = trino.dbapi.connect(host=url.hostname, port=url.port or 8080, http_scheme=url.scheme,
                                        user=user, catalog=catalog, schema="information_schema",
                                        request_timeout=300)
        self.catalog = catalog

    def rows(self, sql: str) -> list[tuple]:
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def value(self, sql: str):
        rows = self.rows(sql)
        return rows[0][0] if rows else None

    def execute(self, sql: str) -> int | None:
        """DDL/DML; returns the row count Trino reports for CREATE TABLE AS, INSERT and DELETE."""
        rows = self.rows(sql)
        return rows[0][0] if rows and rows[0] and type(rows[0][0]) is int else None

    def columns(self, schema: str, table: str) -> dict[str, str]:
        return dict(self.rows(f"SELECT column_name, data_type FROM {self.catalog}.information_schema.columns "
                              f"WHERE table_schema = '{schema}' AND table_name = '{table}' ORDER BY ordinal_position"))

    def tables(self, schema: str) -> list[str]:
        return [r[0] for r in self.rows(f"SELECT table_name FROM {self.catalog}.information_schema.tables "
                                        f"WHERE table_schema = '{schema}'")]

    def exists(self, schema: str, table: str) -> bool:
        return table in self.tables(schema)


def record_check(dataset: str, check: str, passed: bool, severity: str, failing_rows=None, metric=None,
                 detail=None, run_id=None):
    _ops("INSERT INTO ops.check_results (dataset, check_name, passed, severity, failing_rows, metric, detail, run_id) "
         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
         (dataset, check, passed, severity, failing_rows, metric, detail, run_id))


def record_publish(started_at: float, dataset: str, branch: str, outcome: str, rows_written=None,
                   rows_quarantined=None, snapshot_id=None, detail=None, run_id=None):
    _ops("INSERT INTO ops.publishes (started_at, dataset, branch, outcome, rows_written, rows_quarantined, "
         "snapshot_id, detail, run_id) VALUES (to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s)",
         (started_at, dataset, branch, outcome, rows_written, rows_quarantined, snapshot_id, detail, run_id))


def _ops(sql: str, params: tuple):
    if not OPS_DSN:
        return
    import psycopg
    with psycopg.connect(OPS_DSN, connect_timeout=10) as conn:
        conn.execute(sql, params)


def kafka_backlog_age(group: str, topics: list[str]) -> dict[str, float]:
    """Per topic: seconds since the oldest message the consumer group has not committed yet (0: nothing waiting).

    This is how stale the group's output is. A quiet topic is fresh, however long ago its last commit was."""
    from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

    admin = KafkaAdminClient(bootstrap_servers=KAFKA, client_id="platform-freshness")
    consumer = KafkaConsumer(bootstrap_servers=KAFKA, enable_auto_commit=False, group_id=None,
                             consumer_timeout_ms=5000)
    try:
        committed = admin.list_group_offsets(group)[group]
        parts = [TopicPartition(t, p) for t in topics for p in (consumer.partitions_for_topic(t) or [])]
        ends = consumer.end_offsets(parts)
        now, age = time.time(), {t: 0.0 for t in topics}
        for tp in parts:
            done = committed[tp].offset if tp in committed and committed[tp].offset >= 0 else 0
            if done >= ends[tp]:
                continue
            consumer.assign([tp])
            consumer.seek(tp, done)
            batch = consumer.poll(timeout_ms=5000, max_records=1).get(tp)
            oldest = batch[0].timestamp / 1000 if batch else now
            age[tp.topic] = max(age[tp.topic], now - oldest)
        return age
    finally:
        consumer.close()
        admin.close()

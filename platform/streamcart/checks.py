"""Checks the specs declare, evaluated with Trino: expectations (row-level SQL), the primary key, the schema, freshness
and reconciliation with the source.

A row fails an expectation unless its SQL is TRUE (NULL fails too). Severity follows on_fail: block is ERROR and
stops a publish; quarantine and warn are WARN (quarantined rows are set aside, warned ones only counted).
"""

import os
import time
from dataclasses import dataclass

from .engine import Trino, kafka_backlog_age
from .source import psql
from .specs import Expectation, Spec

SAMPLE = 5


@dataclass
class CheckOutcome:
    name: str
    passed: bool
    severity: str                  # ERROR | WARN
    failing_rows: int | None = None
    metric: float | None = None
    detail: str = ""
    blocking: bool = False

    def line(self) -> str:
        mark = "✓" if self.passed else ("✗" if self.severity == "ERROR" else "!")
        rows = f" {self.failing_rows} rows" if self.failing_rows else ""
        return f"  {mark} {self.name}{rows}{': ' + self.detail if self.detail else ''}"


def failing(e: Expectation) -> str:
    return f"NOT coalesce(({e.sql}), false)"


def keys(spec: Spec) -> str:
    return ", ".join(spec.primary_key)


def expectation(t: Trino, spec: Spec, e: Expectation, table: str) -> CheckOutcome:
    n = t.value(f"SELECT count(*) FROM {table} WHERE {failing(e)}")
    detail = ""
    if n:
        sample = t.rows(f"SELECT {keys(spec)} FROM {table} WHERE {failing(e)} LIMIT {SAMPLE}")
        detail = f"e.g. {', '.join('/'.join(str(v) for v in r) for r in sample)}"
    return CheckOutcome(e.name, passed=n == 0, severity="ERROR" if e.on_fail == "block" else "WARN",
                        failing_rows=n, detail=detail, blocking=e.on_fail == "block")


def primary_key(t: Trino, spec: Spec, table: str) -> CheckOutcome:
    nulls = " OR ".join(f"{k} IS NULL" for k in spec.primary_key)
    dup = t.value(f"SELECT count(*) FROM (SELECT {keys(spec)} FROM {table} GROUP BY {keys(spec)} HAVING count(*) > 1)")
    null = t.value(f"SELECT count(*) FROM {table} WHERE {nulls}")
    return CheckOutcome("primary_key", passed=dup == 0 and null == 0, severity="ERROR", failing_rows=dup + null,
                        detail=f"{dup} duplicated keys, {null} null keys" if dup or null else "", blocking=True)


def schema(t: Trino, spec: Spec, schema_name: str, table: str) -> CheckOutcome:
    actual = t.columns(schema_name, table)
    if actual == spec.columns:
        return CheckOutcome("schema", passed=True, severity="ERROR", blocking=True)
    diff = [f"{c}: spec {spec.columns.get(c, '-')}, query {actual.get(c, '-')}"
            for c in dict.fromkeys([*spec.columns, *actual]) if spec.columns.get(c) != actual.get(c)]
    if not diff:
        diff = ["column order differs"]
    return CheckOutcome("schema", passed=False, severity="ERROR", detail="; ".join(diff), blocking=True)


def silver_freshness(specs: list[Spec]) -> dict[str, CheckOutcome]:
    """Seconds the oldest change for each silver table has been waiting in Kafka for the Flink job."""
    ages = kafka_backlog_age("flink-silver", [s.topic for s in specs])
    return {s.dataset: freshness(s, ages[s.topic], "oldest change not yet in silver") for s in specs}


def gold_freshness(t: Trino, spec: Spec) -> CheckOutcome:
    last = t.value(f"SELECT to_unixtime(max(committed_at)) FROM gold.\"{spec.name}$snapshots\"")
    age = time.time() - last if last else float("inf")
    return freshness(spec, age, "since the last publish")


def freshness(spec: Spec, age: float, what: str) -> CheckOutcome:
    severity = "ERROR" if spec.error_after and age > spec.error_after else "WARN"
    late = (spec.error_after and age > spec.error_after) or (spec.warn_after and age > spec.warn_after)
    shown = "never" if age == float("inf") else f"{age:.0f}s"
    return CheckOutcome("freshness", passed=not late, severity=severity, metric=None if age == float("inf") else age,
                        detail=f"{shown} {what} (warn after {spec.warn_after}s, error after {spec.error_after}s)")


def reconcile(t: Trino, spec: Spec, pg_rows: list[tuple]) -> CheckOutcome:
    """Gold vs the same metric computed on PostgreSQL: rows keyed by reconcile.key, columns reconcile.compare."""
    rec = spec.reconcile
    cols = [*rec["key"], *rec["compare"]]
    nk = len(rec["key"])
    lake = {tuple(str(v) for v in r[:nk]): tuple(str(v) for v in r[nk:]) for r in
            t.rows(f"SELECT {', '.join(cols)} FROM gold.{spec.name}")}
    source = {tuple(str(v) for v in r[:nk]): tuple(str(v) for v in r[nk:]) for r in pg_rows}
    bad = sorted(k for k in source.keys() | lake.keys() if source.get(k) != lake.get(k))
    detail = "; ".join(f"{'/'.join(k)}: source {source.get(k)} gold {lake.get(k)}" for k in bad[:SAMPLE])
    return CheckOutcome("reconcile_with_source", passed=not bad, severity="WARN", failing_rows=len(bad),
                        detail=detail or f"{len(source)} rows equal", blocking=False)


def source_rows(spec: Spec) -> list[tuple]:
    """The reconcile query's rows from PostgreSQL: over SOURCE_DSN (psycopg) in the platform containers, else PSQL."""
    sql = (spec.path.parent / spec.reconcile["sql"]).read_text()
    if os.getenv("SOURCE_DSN"):
        import psycopg
        with psycopg.connect(os.environ["SOURCE_DSN"], connect_timeout=10) as conn:
            return conn.execute(sql).fetchall()
    return [tuple(line.split("\t")) for line in psql(sql, "-F", "\t").splitlines() if line]


def quarantine_table(spec: Spec) -> str:
    return f"quarantine.{spec.layer}_{spec.name}"


def quarantine(t: Trino, spec: Spec, e: Expectation, table: str, run_id: str, only_new: bool = False) -> int:
    """Copies the rows failing `e` into quarantine.<layer>_<name>, with the expectation, run and time.
    only_new: skip rows already quarantined for this expectation in the same version (silver: _source_ts)."""
    q = quarantine_table(spec)
    t.execute("CREATE SCHEMA IF NOT EXISTS quarantine")
    t.execute(f"CREATE TABLE IF NOT EXISTS {q} AS SELECT s.*, CAST(NULL AS varchar) AS _expectation, "
              f"CAST(NULL AS varchar) AS _run_id, CAST(NULL AS timestamp(6) with time zone) AS _quarantined_at "
              f"FROM {table} s WITH NO DATA")
    cols = ", ".join(t.columns(*table.split(".")))
    new = ""
    if only_new:
        match = " AND ".join(f"q.{k} = s.{k}" for k in spec.primary_key)
        new = (f" AND NOT EXISTS (SELECT 1 FROM {q} q WHERE {match} AND q._expectation = '{e.name}' "
               f"AND q._source_ts IS NOT DISTINCT FROM s._source_ts)")
    return t.execute(f"INSERT INTO {q} ({cols}, _expectation, _run_id, _quarantined_at) "
                     f"SELECT {', '.join('s.' + c for c in cols.split(', '))}, '{e.name}', '{run_id}', "
                     f"current_timestamp FROM {table} s WHERE {failing(e)}{new}") or 0

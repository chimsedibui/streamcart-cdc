"""Write-audit-publish for batch datasets, in Trino.

    write    the query runs into an audit table, audit.<name>__<run>; readers of gold.<name> see nothing yet
    audit    schema, primary key and every expectation run against the audit table. quarantine: failing rows move
             to quarantine.gold_<name>; block: any failing row stops here, and the audit table stays for inspection
    publish  gold.<name> is replaced by the audited rows in one Iceberg commit (CREATE OR REPLACE TABLE): readers
             switch from the old snapshot to the new one at once, and the old one stays for time travel/rollback

Why an audit table and not an Iceberg branch: Trino 484 parses CREATE BRANCH / INSERT INTO t @ branch, but its Iceberg
connector does not implement branches yet ("This connector does not support creating branches"). The audit step and
its guarantees are the same; once branches land, write becomes INSERT INTO gold.<name> @ audit_<run> and publish a
fast-forward of main.
"""

import time
import uuid
from dataclasses import dataclass, field

from . import checks
from .checks import CheckOutcome
from .engine import Trino, record_check, record_publish
from .specs import Spec

TABLE_PROPERTIES = "WITH (format_version = 2, format = 'PARQUET')"


@dataclass
class BuildResult:
    dataset: str
    audit_table: str
    outcome: str                      # published | blocked | failed
    rows_written: int = 0
    rows_quarantined: int = 0
    snapshot_id: int | None = None
    checks: list[CheckOutcome] = field(default_factory=list)
    detail: str = ""

    def summary(self) -> str:
        head = f"{self.dataset}: {self.outcome} ({self.rows_written} rows"
        head += f", {self.rows_quarantined} quarantined" if self.rows_quarantined else ""
        head += f", snapshot {self.snapshot_id})" if self.snapshot_id else ")"
        tail = f"\n  {self.detail}" if self.detail else ""
        return "\n".join([head, *(c.line() for c in self.checks)]) + tail


def build(spec: Spec, run_id: str, t: Trino | None = None) -> BuildResult:
    t = t or Trino()
    started = time.time()
    audit = f"audit.{spec.name}__{run_id.replace('-', '')[:8] if run_id != 'cli' else uuid.uuid4().hex[:8]}"
    result = BuildResult(spec.dataset, audit, "failed")
    try:
        for schema in ("audit", "gold", "quarantine"):
            t.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        t.execute(f"DROP TABLE IF EXISTS {audit}")
        # write
        result.rows_written = t.execute(f"CREATE TABLE {audit} {TABLE_PROPERTIES} AS\n{spec.sql}") or 0
        # audit
        result.checks.append(checks.schema(t, spec, "audit", audit.split(".")[1]))
        if result.checks[-1].passed:
            result.checks.append(checks.primary_key(t, spec, audit))
            for e in spec.expectations:
                outcome = checks.expectation(t, spec, e, audit)
                if e.on_fail == "quarantine" and outcome.failing_rows:
                    result.rows_quarantined += checks.quarantine(t, spec, e, audit, run_id)
                    t.execute(f"DELETE FROM {audit} WHERE {checks.failing(e)}")
                    outcome.detail += f" (moved to {checks.quarantine_table(spec)})"
                result.checks.append(outcome)
        blocked = [c for c in result.checks if c.blocking and not c.passed]
        if blocked:
            result.outcome = "blocked"
            result.detail = (f"not published: {', '.join(c.name for c in blocked)} failed; the audited rows stay in "
                             f"lakehouse.{audit} for inspection")
        else:
            # publish
            t.execute(f"CREATE OR REPLACE TABLE gold.{spec.name} {TABLE_PROPERTIES} AS SELECT * FROM {audit}")
            result.snapshot_id = t.value(f"SELECT snapshot_id FROM gold.\"{spec.name}$snapshots\" "
                                         f"ORDER BY committed_at DESC LIMIT 1")
            result.rows_written -= result.rows_quarantined
            result.outcome = "published"
            for old in t.tables("audit"):          # this run's and any kept from blocked runs
                if old.startswith(f"{spec.name}__"):
                    t.execute(f"DROP TABLE IF EXISTS audit.{old}")
    except Exception as e:
        result.outcome, result.detail = "failed", f"{type(e).__name__}: {e}"
        raise
    finally:
        for c in result.checks:
            record_check(spec.dataset, c.name, c.passed, c.severity, c.failing_rows, c.metric, c.detail, run_id)
        record_publish(started, spec.dataset, audit, result.outcome, result.rows_written, result.rows_quarantined,
                       result.snapshot_id, result.detail[:2000], run_id)
    return result

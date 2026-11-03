"""The Dagster code location: every dataset spec becomes an asset, every expectation an asset check.

    silver.<table>   external assets, kept current by the Flink job. A sensor records each new Iceberg snapshot as a
                     materialization; checks every 5 min: expectations (quarantine/warn) and freshness (Kafka backlog)
    gold.<name>      assets built by write-audit-publish in Trino (streamcart.wap) on the specs' schedules; blocking
                     checks stop the publish and everything downstream
    maintenance      daily: expire snapshots and orphan files everywhere, compact landing and gold
    lineage          daily: the streaming part of the graph to Marquez (gold runs report their own)
"""

import datetime
import hashlib
import time
from collections import defaultdict

import dagster as dg

from streamcart import checks, lineage, wap
from streamcart.engine import Trino, record_check
from streamcart.specs import Spec, load_all, ordered

SPECS = load_all()
SILVER = [s for s in ordered(SPECS) if s.mode == "streaming"]
GOLD = [s for s in ordered(SPECS) if s.mode == "batch"]


def key(dataset: str) -> dg.AssetKey:
    return dg.AssetKey(dataset.split("."))


def severity(level: str) -> dg.AssetCheckSeverity:
    return dg.AssetCheckSeverity.ERROR if level == "ERROR" else dg.AssetCheckSeverity.WARN


def check_result(spec: Spec, c: checks.CheckOutcome) -> dg.AssetCheckResult:
    meta = {"detail": c.detail} if c.detail else {}
    if c.failing_rows is not None:
        meta["failing_rows"] = c.failing_rows
    if c.metric is not None:
        meta["seconds"] = round(c.metric, 1)
    return dg.AssetCheckResult(asset_key=key(spec.dataset), check_name=c.name, passed=c.passed,
                               severity=severity(c.severity), metadata=meta)


def describe(spec: Spec) -> str:
    rules = "\n".join(f"- `{e.name}` ({e.on_fail}): `{e.sql}`" for e in spec.expectations) or "- none"
    return (f"{spec.description}\n\nSpec: `{spec.path.relative_to(spec.path.parents[2])}`\n\n"
            f"**Expectations**\n{rules}")


# ---------------------------------------------------------------- source and silver (external)

source_assets = [dg.AssetSpec(key=dg.AssetKey(["source", s.name]), group_name="source", kinds={"postgres"},
                              description=f"ecommerce.{s.name} in PostgreSQL, captured by Debezium to {s.topic}")
                 for s in SILVER]
silver_assets = [dg.AssetSpec(key=key(s.dataset), group_name="silver", kinds={"iceberg", "flink"},
                              owners=[f"team:{s.owner}"], description=describe(s),
                              deps=[dg.AssetKey(["source", s.name])],
                              metadata={"primary_key": ", ".join(s.primary_key), "topic": s.topic,
                                        "dagster/column_schema": dg.TableSchema(columns=[
                                            dg.TableColumn(n, t) for n, t in s.columns.items()])})
                 for s in SILVER]

silver_check_specs = [dg.AssetCheckSpec(e.name, asset=key(s.dataset), description=f"{e.on_fail}: {e.sql}")
                      for s in SILVER for e in s.expectations]
silver_check_specs += [dg.AssetCheckSpec("freshness", asset=key(s.dataset),
                                         description="age of the oldest change still waiting in Kafka")
                       for s in SILVER if s.warn_after or s.error_after]


@dg.multi_asset_check(specs=silver_check_specs, name="silver_checks", can_subset=True)
def silver_checks(context: dg.AssetCheckExecutionContext):
    """Expectations on the live silver tables (failing rows of quarantine expectations are copied to
    quarantine.silver_<table>, once per row version) and freshness from the Flink job's Kafka backlog."""
    wanted = {(ck.asset_key, ck.name) for ck in context.selected_asset_check_keys}
    t = Trino()
    run = context.op_execution_context.run_id
    fresh = checks.silver_freshness([s for s in SILVER if (key(s.dataset), "freshness") in wanted])
    for s in SILVER:
        table = f"silver.{s.name}"
        for e in s.expectations:
            if (key(s.dataset), e.name) not in wanted:
                continue
            c = checks.expectation(t, s, e, table)
            if e.on_fail == "quarantine" and c.failing_rows:
                added = checks.quarantine(t, s, e, table, run, only_new=True)
                c.detail += f" ({added} new in {checks.quarantine_table(s)})"
            record_check(s.dataset, c.name, c.passed, c.severity, c.failing_rows, None, c.detail, run)
            yield check_result(s, c)
        if s.dataset in fresh:
            c = fresh[s.dataset]
            record_check(s.dataset, c.name, c.passed, c.severity, None, c.metric, c.detail, run)
            yield check_result(s, c)


@dg.sensor(minimum_interval_seconds=120, default_status=dg.DefaultSensorStatus.RUNNING)
def silver_snapshots(context: dg.SensorEvaluationContext):
    """Records every new Iceberg snapshot the Flink job committed to a silver table as a materialization, so the
    asset graph shows when each table last changed."""
    t = Trino()
    union = "\nUNION ALL ".join(
        f"SELECT '{s.name}', snapshot_id, CAST(to_unixtime(committed_at) AS double), "
        f"element_at(summary, 'added-records'), element_at(summary, 'total-records') "
        f"FROM (SELECT * FROM silver.\"{s.name}$snapshots\" ORDER BY committed_at DESC LIMIT 1)" for s in SILVER)
    seen = dict(item.split("=") for item in (context.cursor or "").split(",") if item)
    events = []
    for name, snapshot, committed, added, total in t.rows(union):
        if seen.get(name) == str(snapshot):
            continue
        seen[name] = str(snapshot)
        events.append(dg.AssetMaterialization(asset_key=dg.AssetKey(["silver", name]), metadata={
            "snapshot_id": str(snapshot), "committed_at": dg.MetadataValue.timestamp(committed),
            "added_records": int(added or 0), "dagster/row_count": int(total or 0)}))
    return dg.SensorResult(asset_events=events, cursor=",".join(f"{k}={v}" for k, v in sorted(seen.items())))


# ---------------------------------------------------------------- gold (write-audit-publish)

def gold_asset(spec: Spec) -> dg.AssetsDefinition:
    builtin = [dg.AssetCheckSpec("schema", asset=key(spec.dataset), blocking=True,
                                 description="output columns and types equal the spec's"),
               dg.AssetCheckSpec("primary_key", asset=key(spec.dataset), blocking=True,
                                 description=f"({', '.join(spec.primary_key)}) is unique and not null")]
    declared = [dg.AssetCheckSpec(e.name, asset=key(spec.dataset), blocking=e.on_fail == "block",
                                  description=f"{e.on_fail}: {e.sql}") for e in spec.expectations]

    @dg.asset(key=key(spec.dataset), group_name="gold", kinds={"iceberg", "trino"}, owners=[f"team:{spec.owner}"],
              description=describe(spec), deps=[key(u) for u in spec.upstream], check_specs=builtin + declared,
              code_version=hashlib.sha256(spec.sql.encode()).hexdigest()[:12],
              metadata={"primary_key": ", ".join(spec.primary_key),
                        "sql": dg.MetadataValue.md(f"```sql\n{spec.sql}```"),
                        "dagster/column_schema": dg.TableSchema(columns=[
                            dg.TableColumn(n, t) for n, t in spec.columns.items()])})
    def build(context: dg.AssetExecutionContext):
        run = context.run_id
        _lineage(lineage.gold_run("START", spec, SPECS, run))
        try:
            result = wap.build(spec, run)
        except Exception as e:
            _lineage(lineage.gold_run("FAIL", spec, SPECS, run, error=str(e)))
            raise
        context.log.info(result.summary())
        results = [check_result(spec, c) for c in result.checks]
        reported = {c.name for c in result.checks}
        for missing in [s for s in builtin + declared if s.name not in reported]:    # skipped after a schema failure
            results.append(dg.AssetCheckResult(asset_key=key(spec.dataset), check_name=missing.name, passed=False,
                                               severity=dg.AssetCheckSeverity.ERROR,
                                               metadata={"detail": "not evaluated: the schema check failed"}))
        if result.outcome != "published":
            _lineage(lineage.gold_run("FAIL", spec, SPECS, run, error=result.detail))
            for r in results:
                yield r
            raise dg.Failure(description=result.detail, metadata={"audit_table": f"lakehouse.{result.audit_table}"})
        _lineage(lineage.gold_run("COMPLETE", spec, SPECS, run))
        yield dg.MaterializeResult(check_results=results, metadata={
            "dagster/row_count": result.rows_written, "rows_quarantined": result.rows_quarantined,
            "snapshot_id": str(result.snapshot_id), "audit_table": f"lakehouse.{result.audit_table}"})

    return build


def reconcile_check(spec: Spec) -> dg.AssetChecksDefinition:
    rec = spec.reconcile

    @dg.asset_check(asset=key(spec.dataset), name="reconcile_with_source", blocking=False,
                    description=f"{', '.join(rec['compare'])} per {', '.join(rec['key'])} equal "
                                f"{rec['sql']} on PostgreSQL (exact only while the source is quiet)")
    def reconcile(context: dg.AssetCheckExecutionContext):
        c = checks.reconcile(Trino(), spec, checks.source_rows(spec))
        record_check(spec.dataset, c.name, c.passed, c.severity, c.failing_rows, None, c.detail,
                     context.op_execution_context.run_id)
        return check_result(spec, c)

    return reconcile


gold_assets = [gold_asset(s) for s in GOLD]
reconcile_checks = [reconcile_check(s) for s in GOLD if s.reconcile]


@dg.multi_asset_check(specs=[dg.AssetCheckSpec("freshness", asset=key(s.dataset),
                                               description="time since the last publish")
                             for s in GOLD if s.warn_after or s.error_after], name="gold_freshness",
                      can_subset=True)
def gold_freshness(context: dg.AssetCheckExecutionContext):
    wanted = {ck.asset_key for ck in context.selected_asset_check_keys}
    t = Trino()
    for s in GOLD:
        if key(s.dataset) not in wanted:
            continue
        try:
            c = checks.gold_freshness(t, s)
        except Exception as e:                     # not published yet: the table does not exist
            c = checks.CheckOutcome("freshness", False, "ERROR", detail=f"never published ({type(e).__name__})")
        record_check(s.dataset, c.name, c.passed, c.severity, None, c.metric, c.detail,
                     context.op_execution_context.run_id)
        yield check_result(s, c)


def _lineage(e: dict):
    try:
        lineage.send([e])
    except Exception:                              # Marquez is optional (profile lineage); never fail a build on it
        pass


# ---------------------------------------------------------------- maintenance and lineage

RETENTION = "1h"


@dg.op
def maintain_tables(context: dg.OpExecutionContext):
    """Iceberg housekeeping in Trino, one table at a time. Every table: expire snapshots older than 1 h, then remove
    orphan files. landing.cdc_events: compact yesterday's partitions (UTC days, complete by now; Spark only appends
    to today's). Gold needs no compaction: every publish writes the table afresh. Silver is not compacted here: Flink
    commits equality deletes every 30 s, and a Trino rewrite racing them is rejected by Iceberg's conflict check, so
    silver compaction belongs to a Spark rewrite_data_files job."""
    t = Trino()
    failures = []
    today = datetime.datetime.now(datetime.UTC).date()
    yesterday = today - datetime.timedelta(days=1)
    for schema in ("landing", "silver", "gold", "quarantine"):
        for table in sorted(t.tables(schema)):
            name = f"{schema}.{table}"
            steps = [f"expire_snapshots(retention_threshold => '{RETENTION}')",
                     f"remove_orphan_files(retention_threshold => '{RETENTION}')"]
            if name == "landing.cdc_events":
                steps.append(f"optimize WHERE kafka_timestamp >= TIMESTAMP '{yesterday} 00:00:00 UTC' "
                             f"AND kafka_timestamp < TIMESTAMP '{today} 00:00:00 UTC'")
            for step in steps:
                started = time.monotonic()
                try:
                    t.execute(f"ALTER TABLE {name} EXECUTE {step}")
                    context.log.info(f"{name}: {step.split('(')[0].split()[0]} in {time.monotonic() - started:.1f}s")
                except Exception as e:
                    failures.append(f"{name}: {step.split('(')[0].split()[0]}: {e}")
                    context.log.warning(failures[-1])
    for table in t.tables("audit"):               # leftovers of blocked runs older than a day go too
        created = t.value(f"SELECT min(to_unixtime(committed_at)) FROM audit.\"{table}$snapshots\"")
        if created and time.time() - created > 86400:
            t.execute(f"DROP TABLE IF EXISTS audit.{table}")
            context.log.info(f"dropped audit.{table}")
    if failures:
        raise dg.Failure(description=f"{len(failures)} maintenance steps failed",
                         metadata={"failures": "\n".join(failures)})


@dg.job(description="Iceberg housekeeping: snapshots, orphan files, compaction")
def maintenance():
    maintain_tables()


@dg.op
def publish_lineage(context: dg.OpExecutionContext):
    n = lineage.publish_static(SPECS)
    context.log.info(f"sent {n} OpenLineage events")


@dg.job(description="The streaming part of the lineage graph (Debezium, Flink silver, Spark landing) to Marquez")
def streaming_lineage():
    publish_lineage()


# ---------------------------------------------------------------- jobs and schedules

by_schedule = defaultdict(list)
for s in GOLD:
    by_schedule[s.schedule or "*/10 * * * *"].append(s)

gold_jobs, schedules = [], []
for i, (cron, members) in enumerate(sorted(by_schedule.items())):
    selection = dg.AssetSelection.assets(*[key(s.dataset) for s in members]) - dg.AssetSelection.checks(gold_freshness)
    job = dg.define_asset_job(f"gold{'' if i == 0 else f'_{i}'}", selection=selection,
                              description=f"gold datasets on {cron}: write-audit-publish (freshness runs in checks)")
    gold_jobs.append(job)
    schedules.append(dg.ScheduleDefinition(job=job, cron_schedule=cron,
                                           default_status=dg.DefaultScheduleStatus.RUNNING))

checks_job = dg.define_asset_job("checks", selection=dg.AssetSelection.checks(silver_checks, gold_freshness),
                                 description="silver expectations and freshness, gold freshness")
schedules += [
    dg.ScheduleDefinition(job=checks_job, cron_schedule="*/5 * * * *", default_status=dg.DefaultScheduleStatus.RUNNING),
    dg.ScheduleDefinition(job=maintenance, cron_schedule="30 3 * * *", default_status=dg.DefaultScheduleStatus.RUNNING),
    dg.ScheduleDefinition(job=streaming_lineage, cron_schedule="0 4 * * *",
                          default_status=dg.DefaultScheduleStatus.RUNNING),
]

defs = dg.Definitions(
    assets=[*source_assets, *silver_assets, *gold_assets],
    asset_checks=[silver_checks, gold_freshness, *reconcile_checks],
    jobs=[*gold_jobs, checks_job, maintenance, streaming_lineage],
    schedules=schedules,
    sensors=[silver_snapshots],
)

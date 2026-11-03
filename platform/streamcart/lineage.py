"""OpenLineage events for Marquez, derived from the specs: the same graph the code runs, with schemas and
column-level lineage.

    postgres  enterprise_db.ecommerce.<table>  --debezium:<table>-->   kafka  cdc.ecommerce.<table>
    kafka     cdc.ecommerce.<table>            --flink:silver.<table>--> lakehouse  silver.<table>   (identity columns)
    kafka     cdc.ecommerce.*                  --spark:landing-->      lakehouse  landing.cdc_events
    lakehouse silver/gold inputs               --gold.<name>-->        lakehouse  gold.<name>        (columns: sqlglot)

The streaming jobs never finish, so publish_static() reports each as one completed run describing its current shape
(Dagster sends it again daily and after each deploy). Gold runs are reported live from their Dagster run.
"""

import datetime
import json
import os
import urllib.request
import uuid

from sqlglot import exp
from sqlglot.lineage import lineage as column_lineage

from . import silver
from .specs import Spec

URL = os.getenv("OPENLINEAGE_URL", "http://localhost:5000")
PRODUCER = "https://github.com/KhanhXoe/Real-time-CDC-with-Kafka-Debezium/platform"
JOBS_NS = "streamcart"
NS = {"postgres": "postgres://postgres:5432", "kafka": "kafka://kafka:9092", "lakehouse": "iceberg://polaris/lakehouse"}
SPEC = "https://openlineage.io/spec"


def facet(kind: str, version: str, **body) -> dict:
    return {"_producer": PRODUCER, "_schemaURL": f"{SPEC}/facets/{version}/{kind}.json#/$defs/{kind}", **body}


def dataset(namespace: str, name: str, columns: dict[str, str] | None = None, description: str = "",
            column_sources: dict[str, list[tuple[str, str, str]]] | None = None) -> dict:
    facets = {}
    if columns:
        facets["schema"] = facet("SchemaDatasetFacet", "1-1-1",
                                 fields=[{"name": n, "type": t} for n, t in columns.items()])
    if description:
        facets["documentation"] = facet("DocumentationDatasetFacet", "1-1-0", description=description)
    if column_sources:
        facets["columnLineage"] = facet("ColumnLineageDatasetFacet", "1-2-0", fields={
            col: {"inputFields": [{"namespace": ns, "name": name, "field": field} for ns, name, field in sources]}
            for col, sources in column_sources.items() if sources})
    return {"namespace": NS[namespace], "name": name, "facets": facets}


def event(state: str, job: str, run_id: str, inputs: list[dict], outputs: list[dict], spec: Spec | None = None,
          sql: str = "", error: str = "") -> dict:
    job_facets = {}
    if spec:
        job_facets["ownership"] = facet("OwnershipJobFacet", "1-0-1", owners=[{"name": f"team:{spec.owner}"}])
        if spec.description:
            job_facets["documentation"] = facet("DocumentationJobFacet", "1-0-1", description=spec.description)
    if sql:
        job_facets["sql"] = facet("SQLJobFacet", "1-0-1", query=sql)
    run_facets = {}
    if error:
        run_facets["errorMessage"] = facet("ErrorMessageRunFacet", "1-0-1", message=error[:4000],
                                           programmingLanguage="SQL")
    return {"eventType": state, "eventTime": datetime.datetime.now(datetime.UTC).isoformat(),
            "producer": PRODUCER, "schemaURL": f"{SPEC}/2-0-2/OpenLineage.json#/$defs/RunEvent",
            "run": {"runId": run_id, "facets": run_facets},
            "job": {"namespace": JOBS_NS, "name": job, "facets": job_facets},
            "inputs": inputs, "outputs": outputs}


def send(events: list[dict]) -> int:
    for e in events:
        req = urllib.request.Request(f"{URL}/api/v1/lineage", json.dumps(e).encode(),
                                     {"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=10):
            pass
    return len(events)


def columns_of(spec: Spec) -> dict[str, str]:
    return silver.trino_columns(spec) if spec.mode == "streaming" else spec.columns


def gold_column_sources(spec: Spec, specs: dict[str, Spec]) -> dict[str, list[tuple[str, str, str]]]:
    """Output column -> the input columns it is computed from (sqlglot over the spec's SQL; best effort)."""
    schema = {}
    for name in spec.upstream:
        up = specs[name]
        schema.setdefault(up.layer, {})[up.name] = columns_of(up)
    out = {}
    for col in spec.columns:
        try:
            node = column_lineage(col, spec.sql, schema=schema, dialect="trino")
        except Exception:                     # an expression sqlglot cannot trace: table-level lineage only
            continue
        out[col] = sorted({(NS["lakehouse"], f"{n.expression.db}.{n.expression.name}", n.name.split(".")[-1])
                           for n in node.walk() if isinstance(n.expression, exp.Table)})
    return out


def static_events(specs: dict[str, Spec]) -> list[dict]:
    events = []
    streaming = [s for s in specs.values() if s.mode == "streaming"]
    for s in streaming:
        cols = {n: t for n, t in s.columns.items()}
        source = dataset("postgres", f"enterprise_db.ecommerce.{s.name}", cols)
        topic = dataset("kafka", s.topic, cols, column_sources={
            c: [(NS["postgres"], f"enterprise_db.ecommerce.{s.name}", c)] for c in cols})
        table = dataset("lakehouse", s.dataset, columns_of(s), s.description, column_sources={
            **{c: [(NS["kafka"], s.topic, c)] for c in cols}, "_source_ts": [(NS["kafka"], s.topic, "source.ts_ms")]})
        for job, inputs, outputs in ((f"debezium:{s.name}", [source], [topic]),
                                     (f"flink:{s.dataset}", [topic], [table])):
            run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{job}:{json.dumps(outputs, sort_keys=True)}"))
            events += [event(state, job, run_id, inputs, outputs, s) for state in ("START", "COMPLETE")]
    landing_cols = {"topic": "varchar", "source_table": "varchar", "kafka_partition": "integer",
                    "kafka_offset": "bigint", "kafka_timestamp": "timestamp(6) with time zone", "record_key": "varchar",
                    "record_value": "varchar", "ingested_at": "timestamp(6) with time zone"}
    topics = [dataset("kafka", s.topic) for s in streaming]
    landing = dataset("lakehouse", "landing.cdc_events", landing_cols,
                      "Every Debezium change as it arrived from Kafka, never rewritten (spark_microbatch/)")
    run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"spark:landing:{len(topics)}"))
    events += [event("START", "spark:landing", run_id, topics, [landing]),
               event("COMPLETE", "spark:landing", run_id, topics, [landing])]
    return events


def publish_static(specs: dict[str, Spec]) -> int:
    return send(static_events(specs))


def gold_run(state: str, spec: Spec, specs: dict[str, Spec], dagster_run: str, error: str = "") -> dict:
    """One OpenLineage run per dataset build, its id derived from the Dagster run that may build several."""
    run_id = str(uuid.uuid5(uuid.UUID(dagster_run), spec.dataset))
    inputs = [dataset("lakehouse", up, columns_of(specs[up])) for up in spec.upstream]
    sources = gold_column_sources(spec, specs) if state == "COMPLETE" else None
    outputs = [dataset("lakehouse", spec.dataset, spec.columns, spec.description, sources)]
    return event(state, spec.dataset, run_id, inputs, outputs, spec, sql=spec.sql, error=error)

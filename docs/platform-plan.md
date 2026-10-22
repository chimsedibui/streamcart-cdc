# Platform plan: a self-hosted lakehouse platform on the StreamCart base

Goal: grow this repo from "one CDC pipeline" into a small **Databricks-style platform**: a place where we *build*
pipelines declaratively, *validate* them before data is published, and *manage* them (runs, lineage, ownership,
cost of reprocessing) from one control plane, all on open-source parts that run in docker compose today and map
to managed services later.

## 1. Where we start

| Layer | Today | Notes |
|---|---|---|
| Source | `source/` vendored from the Simulated project (`scripts/sync-source.sh`) | Marketplace OLTP, Source API (only writer), identity (EdDSA JWT), simulator. Same source as the ClickHouse project |
| Ingestion | Debezium → Kafka (`cdc.ecommerce.<table>`, `events.<aggregate>`) | Plain JSON envelopes, PII hashed at the connector, outbox → domain events |
| Landing | Spark Structured Streaming → `lakehouse.landing.cdc_events` (Iceberg v2 on SeaweedFS S3, Polaris catalog) | Raw envelope per row, partitioned by table and day |
| Serving | Streamlit live counter | No SQL access to the lake yet |
| Validation | none here; upstream has a schema contract check and `quality/check_data.py` | Both target ClickHouse; logic is reusable |
| Orchestration, lineage, catalog UI | none | |

## 2. What "Databricks-like" means for us

We copy the *capabilities*, not the product. Each Databricks concept maps to one open component:

| Databricks | Here | Why this one |
|---|---|---|
| Delta Lake | **Apache Iceberg v2** (already in place) | Engine-neutral: Spark, Flink and Trino read and write the same tables; native branches and tags |
| Unity Catalog | **Apache Polaris** (Iceberg REST catalog, in place) | Same REST protocol, plus persistence, principals and roles; credential vending once storage has STS |
| Delta Live Tables (declarative pipelines + expectations) | **Pipeline specs (YAML) in `pipelines/`**, compiled by a small `platform` CLI into Flink SQL / Spark jobs and Dagster assets | Keeps pipelines as reviewable code; one spec drives build, tests and docs |
| Workflows / Jobs | **Dagster** | Asset-based (tables are first-class), built-in asset checks = expectations, partitions and backfills, good UI |
| Streaming | **Flink** (silver upserts from Kafka) | Low-latency CDC upserts into Iceberg; Spark stays for batch, backfills and table maintenance |
| SQL Warehouse | **Trino** | Ad hoc SQL and BI over the same Iceberg tables |
| Notebooks | **JupyterLab + Spark Connect** | Notebooks talk to the shared Spark, no local Spark install |
| Lineage | **OpenLineage → Marquez** | Spark, Flink and Dagster all emit OpenLineage events |
| Dashboards | **Superset** (via Trino); Grafana for platform metrics | |

## 3. Target architecture

```text
 source/ (shared with the Simulated project)
 Source API ─► PostgreSQL ─► Debezium ─► Kafka ──┬─► Spark landing ─► landing.cdc_events      (raw, replayable)
                                                 └─► Flink SQL ─────► silver.<table>           (current state, upsert)
                                       Dagster assets (Spark/Trino) ─► gold.<mart>              (business metrics)

 Every write goes to an Iceberg *audit branch* first ─► checks pass ─► fast-forward to main (write-audit-publish)

 Control plane: Dagster UI (runs, assets, checks)  ·  Polaris (catalog, access)  ·  Marquez (lineage)
                Trino + Superset (SQL, BI)  ·  JupyterLab (exploration)  ·  Grafana (pipeline health)
```

Medallion contract:

- **landing**: exactly what Kafka had, never rewritten. Lets us rebuild any layer without re-snapshotting Postgres.
- **bronze** (deferred): one typed, append-only table per source table. `landing` already keeps every change
  replayable, so bronze is added only when a consumer needs typed change history.
- **silver**: current state per primary key (Iceberg MERGE / Flink upsert), deletes applied using `before`.
- **gold**: marts with documented metric definitions (GMV, AOV, fulfilment SLA, return rate, seller payouts).

## 4. Building pipelines: the pipeline spec

One YAML file per dataset is the single source of truth. Example `pipelines/silver/orders.yml`:

```yaml
dataset: silver.orders
owner: data-eng
from: kafka:cdc.ecommerce.orders
mode: streaming            # streaming → Flink upsert, batch → Spark/Trino job under Dagster
primary_key: [id]
order_by: source_lsn       # last change per key wins, replay-safe
columns:                   # generated from the source schema; checked by the contract test
  id: uuid
  status: string
  total: bigint
  created_at: timestamp
expectations:
  - name: total_matches_parts
    sql: total = subtotal - shop_discount - platform_discount + shipping_fee - shipping_subsidy
    on_fail: block         # block | quarantine | warn
  - name: known_status
    sql: status IN ('UNPAID','TO_SHIP','READY_TO_SHIP','SHIPPED','DELIVERY_FAILED','DELIVERED','COMPLETED','CANCELLED','RETURNED_TO_SELLER','RETURN_REFUND')
    on_fail: quarantine
freshness: { warn_after: 2m, error_after: 10m }
```

The `platform` CLI (Python, in `platform/`) does:

| Command | What it does |
|---|---|
| `platform new <dataset>` | Scaffold a spec; columns pre-filled from `source/postgres/init.sql` |
| `platform validate` | Lint specs, resolve dependencies, run the source contract check (no cluster needed; runs in CI) |
| `platform plan` | Show what changes: new tables, schema evolution, which datasets need a backfill |
| `platform deploy` | Create/evolve Iceberg tables in Polaris, submit Flink SQL jobs, register Dagster assets |
| `platform backfill <dataset> --from --to` | Rebuild from `landing` on an audit branch, check, publish |
| `platform test <dataset>` | Run the pipeline on a fixture slice of `landing` and assert expectations |

## 5. Validating pipelines

Validation happens at four gates, cheapest first:

1. **Source contract (CI, every sync of `source/`)**: port upstream `check_contract.py` to compare
   `init.sql` with the pipeline specs: a new source column, a type change, a dropped table or a missing
   `REPLICA IDENTITY FULL` fails the build before anything runs. This is what keeps the two projects honest about
   sharing one source.
2. **Spec tests (CI)**: `platform test` runs each transformation on small recorded `landing` fixtures (captured
   from the simulator) in a local Spark/DuckDB session.
3. **Write-audit-publish (runtime)**: every batch write and every backfill goes to an Iceberg branch
   (`spark.wap.branch` / Flink branch writes); Dagster asset checks run the spec's expectations on the branch; only
   on success is `main` fast-forwarded. Failing rows go to `quarantine.<dataset>` when the policy says so.
4. **Reconciliation (scheduled)**: port the relevant groups of upstream `quality/check_data.py`
   (money, stock, state, pipeline) to Trino SQL: silver vs PostgreSQL row counts and checksums per table, gold
   metrics recomputed from silver. Results are stored in `ops.check_results` and charted in Grafana.

## 6. Managing pipelines

- **Runs and assets**: Dagster is the control plane: every dataset is an asset with its owner, schedule or
  sensor, partitions, last materialization, check history. Flink jobs are represented as observable assets
  (freshness from the Iceberg snapshot timestamp).
- **Catalog and access**: Polaris roles per layer: Spark/Flink service principals write, analysts read
  `silver`/`gold` only, nobody reads `landing` except platform jobs. Reuse the identity service's issuer pattern
  later for SSO.
- **Lineage**: OpenLineage from Spark, Flink and Dagster into Marquez: column-level for silver/gold.
- **Table maintenance**: a Dagster schedule runs Iceberg `rewrite_data_files`, `expire_snapshots`,
  `remove_orphan_files` per table (Spark), with per-layer retention.
- **Health**: Prometheus scrapes Kafka Connect, Flink, Spark, Dagster; Grafana alerts on connector down, slot lag,
  consumer lag, freshness breach, failed checks (alert rules can be adapted from upstream `observability/`).

## 7. Phases

| Phase | Scope | Exit criteria |
|---|---|---|
| **0. Shared source** (done in this branch) | Vendor `source/`, rewire Debezium, sync script | Simulator traffic lands in `landing.cdc_events` |
| **1. Silver on Flink** (done: 31/31 tables reconcile, Polaris in place) | Flink cluster in compose, generated Flink SQL `debezium-json` Kafka source → Iceberg upsert for the 31 business tables, `quality/reconcile.py`; Polaris replaces the REST catalog | `silver.*` row counts equal PostgreSQL within 1 min of the simulator stopping |
| **2. SQL access** | Trino + Superset; first gold marts (daily GMV, orders by status, seller SLA) as Dagster assets | A gold metric matches the same SQL run on PostgreSQL |
| **3. Validation framework** | Pipeline specs + `platform validate/test`, contract check in GitHub Actions, WAP + asset checks, quarantine tables | A seeded bad change (e.g. total mismatch) is blocked from `main` and shows up in Dagster |
| **4. Management** | Dagster as the single UI, OpenLineage/Marquez, maintenance schedules, Grafana alerts | Any table's owner, lineage, freshness and last check are visible in one place |
| **5. Developer experience** | `platform new/plan/deploy/backfill`, JupyterLab + Spark Connect, per-developer Iceberg branches | A new mart goes from spec to published in one PR without hand-written job code |
| **6. Cloud path** | Same specs on AWS: MSK, S3, Glue or Polaris, EMR/Flink on Kinesis Data Analytics, Athena/Trino | Only connection settings differ between local and cloud |

## 8. Decisions to make before Phase 1

1. **Flink or Spark for silver?** Recommendation: Flink for the streaming upserts (lower latency, native CDC
   changelog), Spark for backfills and maintenance. Both write the same Iceberg tables.
2. ~~Polaris now or later?~~ Done in Phase 1. The previous `tabulario/iceberg-rest` kept its catalog in an
   in-memory SQLite database, so every restart lost all table definitions.
3. **Dagster or Airflow?** Recommendation: Dagster: asset checks and partitions are exactly the
   "validate and manage" features we need; Airflow would need them built on top.
4. **Contract ownership**: schema changes are made only in the Simulated project and pulled with
   `scripts/sync-source.sh`; the contract check in this repo decides whether a sync can merge.

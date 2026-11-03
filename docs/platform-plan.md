# Platform plan: a self-hosted lakehouse platform on the StreamCart base

Goal: grow this repo from "one CDC pipeline" into a small **Databricks-style platform**: a place where we *build*
pipelines declaratively, *validate* them before data is published, and *manage* them (runs, lineage, ownership,
cost of reprocessing) from one control plane, all on open-source parts that run in docker compose today and map
to managed services later.

## 1. Where we are

| Layer | Today | Notes |
|---|---|---|
| Source | `source/` vendored from the Simulated project (`scripts/sync-source.sh`) | Marketplace OLTP, Source API (only writer), identity (EdDSA JWT), simulator. Same source as the ClickHouse project |
| Ingestion | Debezium → Kafka (`cdc.ecommerce.<table>`, `events.<aggregate>`) | Plain JSON envelopes, PII hashed at the connector, outbox → domain events |
| Landing | Spark Structured Streaming → `lakehouse.landing.cdc_events` (Iceberg v2 on SeaweedFS S3, Polaris catalog) | Raw envelope per row, partitioned by table and day |
| Silver | Flink upsert → `lakehouse.silver.<table>` (31 tables), generated from `pipelines/silver/*.yml` | Reconciles 31/31 with PostgreSQL |
| Gold | Trino SQL under Dagster, write-audit-publish → `lakehouse.gold.*` (`fct_orders`, `daily_gmv`, `orders_by_status`, `seller_sla_daily`) | `daily_gmv` and `orders_by_status` equal the same SQL on PostgreSQL |
| Serving | Trino (`lakehouse` read-only catalog), Superset *Marketplace KPIs*, Streamlit live counter | |
| Validation | Specs + `platform/cli.py validate/test` in CI; audit table + checks per build; quarantine tables; live silver checks | Results in Dagster and `ops.*` |
| Orchestration, lineage, health | Dagster; Marquez (OpenLineage, column level); Prometheus + Grafana with alert rules | |

## 2. What "Databricks-like" means for us

We copy the *capabilities*, not the product. Each Databricks concept maps to one open component:

| Databricks | Here | Why this one |
|---|---|---|
| Delta Lake | **Apache Iceberg v2** (already in place) | Engine-neutral: Spark, Flink and Trino read and write the same tables; native branches and tags |
| Unity Catalog | **Apache Polaris** (Iceberg REST catalog, in place) | Same REST protocol, plus persistence, principals and roles; credential vending once storage has STS |
| Delta Live Tables (declarative pipelines + expectations) | **Pipeline specs (YAML) in `pipelines/`**, compiled by a small CLI (`platform/cli.py`, package `platform/streamcart`) into Flink SQL and Dagster assets | Keeps pipelines as reviewable code; one spec drives build, tests and docs |
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
                                       Dagster assets (Trino) ──────► gold.<mart>              (business metrics)

 Every batch write goes to an audit table first ─► checks pass ─► one commit replaces the published table
 (write-audit-publish; an Iceberg branch + fast-forward once Trino's Iceberg connector supports branches)

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

The CLI (`platform/cli.py`, Python) does:

| Command | What it does | State |
|---|---|---|
| `new <dataset>` | Scaffold a spec; silver columns, key and replica identity from the source schema (`--missing`: every captured table without one) | done |
| `validate` | Lint specs and SQL, resolve dependencies, check `flink/sql/silver.sql` is current, run the source contract (CI: against `init.sql`) | done |
| `generate` | Render `flink/sql/silver.sql` from the silver specs | done |
| `test [dataset]` | Run gold SQL on fixtures in Trino's memory catalog and assert schema, key, expectations and expected rows | done |
| `build <dataset>` | Write-audit-publish now (what Dagster runs on schedule) | done |
| `lineage` | Send the streaming lineage graph to Marquez | done |
| `plan` | Show what changes: new tables, schema evolution, which datasets need a backfill | Phase 5 |
| `deploy` | Create/evolve Iceberg tables in Polaris, submit Flink SQL jobs, register Dagster assets | Phase 5 |
| `backfill <dataset> --from --to` | Rebuild from `landing` on an audit table, check, publish | Phase 5 |

## 5. Validating pipelines

Validation happens at four gates, cheapest first:

1. **Source contract (CI, every sync of `source/`)**: port upstream `check_contract.py` to compare
   `init.sql` with the pipeline specs: a new source column, a type change, a dropped table or a missing
   `REPLICA IDENTITY FULL` fails the build before anything runs. This is what keeps the two projects honest about
   sharing one source.
2. **Spec tests (CI)**: `platform test` runs each transformation on small recorded `landing` fixtures (captured
   from the simulator) in a local Spark/DuckDB session.
3. **Write-audit-publish (runtime)**: every batch write goes to an audit table, `audit.<name>__<run>`; Dagster asset
   checks run the spec's schema, key and expectations there; only on success is `gold.<name>` replaced, in one
   Iceberg commit. Failing rows go to `quarantine.<layer>_<name>` when the policy says so. The plan was an Iceberg
   branch fast-forwarded to `main`; Trino 484 parses `CREATE BRANCH` but its Iceberg connector does not implement it
   yet, so the audit step uses a table. Switch to branches when Trino supports them (or move gold builds to Spark).
4. **Reconciliation (scheduled)**: silver vs PostgreSQL row counts and checksums per table (`quality/reconcile.py`),
   gold metrics recomputed on PostgreSQL (`reconcile:` in a gold spec; a Dagster check after every build, exact only
   while the source is quiet). Results are stored in `ops.check_results` and charted in Grafana. Still to port:
   upstream `quality/check_data.py`'s money and stock invariants.

## 6. Managing pipelines

- **Runs and assets**: Dagster is the control plane: every dataset is an asset with its owner, schedule or
  sensor, partitions, last materialization, check history. Flink jobs are represented as observable assets
  (freshness from the Iceberg snapshot timestamp).
- **Catalog and access**: Polaris principals per engine: `spark`, `flink` and `dagster` write, `analyst` reads
  (Trino's `lakehouse` catalog and Superset). Still to do: grants per layer (analysts on `silver`/`gold` only), and
  SSO reusing the identity service's issuer.
- **Lineage**: OpenLineage from Spark, Flink and Dagster into Marquez: column-level for silver/gold.
- **Table maintenance**: a daily Dagster job (Trino) expires snapshots and orphan files on every table and compacts
  yesterday's landing partitions. Silver compaction is not there yet: a Trino rewrite races Flink's equality deletes,
  so it needs Spark `rewrite_data_files`.
- **Health**: Prometheus scrapes the replication slot (postgres-exporter), consumer lag (kafka-exporter) and Flink;
  Grafana alerts on Debezium not reading the slot, slot lag, consumer lag, the Flink job down or failing checkpoints,
  and, from `ops.*`, freshness breaches, blocked publishes, failing checks and checks that stopped running.

## 7. Phases

| Phase | Scope | Exit criteria |
|---|---|---|
| **0. Shared source** (done in this branch) | Vendor `source/`, rewire Debezium, sync script | Simulator traffic lands in `landing.cdc_events` |
| **1. Silver on Flink** (done: 31/31 tables reconcile, Polaris in place) | Flink cluster in compose, generated Flink SQL `debezium-json` Kafka source → Iceberg upsert for the 31 business tables, `quality/reconcile.py`; Polaris replaces the REST catalog | `silver.*` row counts equal PostgreSQL within 1 min of the simulator stopping |
| **2. SQL access** (done) | Trino + Superset; first gold marts (daily GMV, orders by status, seller SLA) as Dagster assets | Done: `quality/reconcile.py --gold`, `daily_gmv` and `orders_by_status` equal PostgreSQL |
| **3. Validation framework** (done) | Pipeline specs + `validate/test`, contract check in GitHub Actions, WAP + asset checks, quarantine tables | Done: an order with a wrong total seeded into silver blocked `gold.fct_orders` (and its three marts) from publishing, failed `total_matches_parts` in Dagster, landed in `quarantine.silver_orders` and fired the Grafana alert |
| **4. Management** (done) | Dagster as the single UI, OpenLineage/Marquez, maintenance schedules, Grafana alerts | Done: every asset shows its owner, columns, checks and freshness in Dagster; lineage with columns in Marquez |
| **5. Developer experience** | `platform new/plan/deploy/backfill`, JupyterLab + Spark Connect, per-developer Iceberg branches | A new mart goes from spec to published in one PR without hand-written job code |
| **6. Cloud path** | Same specs on AWS: MSK, S3, Glue or Polaris, EMR/Flink on Kinesis Data Analytics, Athena/Trino | Only connection settings differ between local and cloud |

## 8. Decisions

1. **Flink or Spark for silver?** Recommendation: Flink for the streaming upserts (lower latency, native CDC
   changelog), Spark for backfills and maintenance. Both write the same Iceberg tables.
2. ~~Polaris now or later?~~ Done in Phase 1. The previous `tabulario/iceberg-rest` kept its catalog in an
   in-memory SQLite database, so every restart lost all table definitions.
3. ~~Dagster or Airflow?~~ Dagster (Phase 2): asset checks and partitions are exactly the "validate and manage"
   features we need; Airflow would need them built on top.
4. **Contract ownership**: schema changes are made only in the Simulated project and pulled with
   `scripts/sync-source.sh`; the contract check in this repo (`platform/cli.py validate`, CI) decides whether a sync
   can merge.
5. **Gold engine**: Trino, not Spark, for gold builds, checks and maintenance: one SQL dialect for production and
   tests (memory catalog), a much smaller footprint than a Spark Connect server, and the same engine BI reads with.
   The cost is the audit table instead of a branch (§5) and no silver compaction (§6).
6. **Silver freshness**: the age of the oldest change still waiting in Kafka for the Flink job, not the time since
   the last Iceberg commit: a quiet table is fresh, however long ago it last changed.

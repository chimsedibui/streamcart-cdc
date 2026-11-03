# StreamCart CDC

A real-time **Change Data Capture** lakehouse for a multi-seller marketplace (Shopee / TikTok Shop style).

The **source system** is shared with the [Simulated project](https://github.com/chimsedibui/Simulated-Real-time-CDC-with-Kafka-Debezium):
same PostgreSQL schema, same Source API and business rules, same identity service and traffic simulator. The two
projects differ only in what sits behind Kafka:

| Project | Behind Kafka |
|---|---|
| Simulated | ClickHouse bronze / silver / gold, Grafana |
| **StreamCart (this repo)** | A small lakehouse platform: Apache Iceberg on S3 (SeaweedFS) with Polaris, Spark and Flink ingestion, Trino SQL, Dagster, write-audit-publish checks, Marquez lineage, Superset and Grafana |

```text
 simulator ─► identity (JWT) ─► Source API ─► PostgreSQL ─► Debezium ─► Kafka ─┬► Spark landing ─► landing.cdc_events  (raw, replayable)
 (buyers, sellers,              (only writer,  (enterprise_db,  (pgoutput,   topics └► Flink upsert ──► silver.<table>      (current state, 31 tables)
  carriers, gateways)            outbox,        logical WAL)     PII hashed,                                │
                                 idempotency)                    outbox)        Dagster ─► Trino: write ─► audit ─► publish ─► gold.<mart>
                                                                                (specs)    checks block / quarantine / warn
                         Iceberg v2 on S3 (SeaweedFS), catalog: Polaris  ·  SQL: Trino, Superset  ·  lineage: Marquez  ·  health: Grafana
```

Every dataset is declared once, in a spec under `pipelines/` ([Platform](#platform-specs-validation-orchestration));
the [platform plan](platform-plan.md) explains the design and what comes next.

---

## Source system (`source/`, vendored)

`source/` is a verbatim copy of the Simulated project at the commit in `source/UPSTREAM`. Do not edit it here:
change it upstream, then pull it with

```bash
scripts/sync-source.sh          # or: scripts/sync-source.sh <branch|tag|commit>
```

| Service | Port | Role |
|---|---|---|
| `postgres` | `127.0.0.1:5432` | `enterprise_db.ecommerce`: 38 tables, logical replication, least-privilege roles (`source_api` writes, `cdc_user` replicates, `app_reader` reads) |
| `source-api` | `127.0.0.1:8100` (`/docs`) | The only writer. One business command = one transaction + one outbox event; `Idempotency-Key` on every write |
| `identity` | `127.0.0.1:8300` | Register / login (argon2id + pepper, lockout), rotating refresh tokens, OAuth2 client credentials and token exchange; EdDSA JWTs, JWKS at `/.well-known/jwks.json`. Own database `identity_db`, never captured |
| `simulator` | `127.0.0.1:8200` | Buyers, sellers, payment gateways and carriers; changes data only through the API, on a business clock (`SIM_SPEED`× wall clock) |
| `adminer` | `127.0.0.1:8081` | PostgreSQL UI |

**Auth model** (`source/source-api/app/auth.py`): callers send `Authorization: Bearer <JWT>` from the identity
service; the API verifies EdDSA signatures against the JWKS, audience `source-api`. Buyers only see their own rows,
sellers only their shops', `ops:read` sees everything with PII masked, `pii:read` / `admin` see it in clear. Payment
gateway and carrier webhooks are HMAC-signed (`X-Partner`, `X-Signature: t=…,v1=…`) with keys in
`secrets/webhooks/`. `AUTH_MODE` in `.env`: `off`, `permissive` (log would-be denials) or `enforce`.

Business and API documentation (Vietnamese, from upstream): [docs/source/](source/).

## Pipeline (this repo)

| Service | Port | Role |
|---|---|---|
| `kafka` | `29092` (host) | KRaft broker |
| `debezium` | `8083` | Kafka Connect + Debezium 2.7, config in `debezium/postgres-connector.json` |
| `kafka-ui` | `8080` | Topics, messages, connector |
| `seaweedfs` | `8333` | S3 API for the Iceberg warehouse bucket `warehouse` |
| `polaris` | `8181` | Apache Polaris: Iceberg REST catalog `lakehouse` (`/api/catalog`) with principals and roles; state in `polaris-db` |
| `polaris-bootstrap`, `polaris-init` | | One-shots: realm and root principal; catalog, roles and the `spark` / `flink` / `dagster` (write) and `analyst` (read-only) principals, whose credentials go to `secrets/polaris/` |
| `spark-landing` | | Structured Streaming: `cdc.ecommerce.*` → `lakehouse.landing.cdc_events` ([details](../spark_microbatch/README.md)) |
| `flink-jobmanager`, `flink-taskmanager` | `8082` (Flink UI) | Flink 1.20 session cluster with the Kafka and Iceberg connectors |
| `flink-silver` | | One-shot: submits `flink/sql/silver.sql` unless it is already running |
| `dashboard` | `8501` | Streamlit live counters from Kafka |

The platform services are in [compose profiles](https://docs.docker.com/compose/how-tos/profiles/), chosen by
`COMPOSE_PROFILES` in `.env` (default `platform,monitoring`; all four need about 16 GB of RAM with the base stack):

| Profile | Service | Port | Role |
|---|---|---|---|
| `platform` | `trino` | `8090` | Trino 484: catalog `lakehouse` (Polaris principal `analyst`, read-only), `lakehouse_rw` (principal `dagster`, Trino user `dagster` only), `memory` (test fixtures) |
| `platform` | `dagster-webserver`, `dagster-daemon` | `3070` | Dagster: assets, checks, schedules, sensors and runs for every spec |
| `platform` | `platform-db` | | PostgreSQL for Dagster, Marquez, Superset and `ops.*` (check results, publishes) |
| `monitoring` | `prometheus`, `postgres-exporter`, `kafka-exporter` | `9090` | Replication slot, consumer lag, Flink metrics |
| `monitoring` | `grafana` | `3000` | *Platform health* dashboard and alert rules (admin / `secrets/grafana_admin_password`) |
| `lineage` | `marquez`, `marquez-web` | `5000`, `3002` | OpenLineage backend and lineage UI |
| `bi` | `superset` | `8088` | Superset over Trino, *Marketplace KPIs* dashboard (admin / `secrets/superset_admin_password`) |

Connector, compared with upstream: same capture filter, `cdc_user` with its password from a secret file, salted
SHA-256 hashing of customer / address / seller PII, outbox → `events.<aggregate>`, no tombstones,
`decimal.handling.mode=string`. Values are **plain JSON** (upstream registers JSON Schemas in a Schema Registry),
because the Spark landing job and Flink's `debezium-json` format read plain JSON.

Topics: `cdc.ecommerce.<table>` (row changes, Debezium envelope with `before` / `after`), `events.<aggregate>`
(domain events: `order`, `payment`, `shipment`, `checkout`, `cart`, `customer`, `inventory`, `product`, `promotion`),
`cdc.transaction` (transaction boundaries).

---

## Quick start

```bash
./setup-env.sh                       # secrets/ (gitignored) and .env
docker compose up -d --build
debezium/register.sh                 # create the connector (fills in the PII salt)
```

The simulator seeds shops, catalog and customers through the API on first start (`SIM_SCALE`, default `small`).
Start traffic once seeding is done:

```bash
curl -s localhost:8200/status | python3 -m json.tool | grep -A3 seeding
curl -X POST localhost:8200/start -H 'Content-Type: application/json' -d '{}'
```

Call the Source API as a service client:

```bash
TOKEN=$(curl -s -X POST localhost:8300/v1/auth/token -d grant_type=client_credentials \
  -d client_id=quality -d client_secret=$(cat secrets/clients/quality) | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')
curl -s "localhost:8100/v1/orders?limit=5" -H "Authorization: Bearer $TOKEN"
```

Query the lakehouse (Trino, read-only):

```bash
docker exec -it cdc-trino trino --catalog lakehouse
#   SELECT status, count(*), sum(total) FROM silver.orders GROUP BY status;
#   SELECT * FROM gold.daily_gmv ORDER BY order_date;
lakehouse/spark-sql.sh -e "SELECT count(*) FROM lakehouse.landing.cdc_events"     # or Spark SQL
```

## Platform: specs, validation, orchestration

Every dataset has one spec, `pipelines/<layer>/<name>.yml`: owner, inputs, columns, primary key, expectations and
freshness. The rest is derived from the specs: the Flink job, the Dagster assets and checks, the lineage graph.

| Layer | Spec | Built by | Contract |
|---|---|---|---|
| `silver.<table>` (31) | `mode: streaming`, `from: kafka:cdc.ecommerce.<table>`; columns, key and replica identity mirror the source | Flink upsert, `flink/sql/silver.sql` (generated) | equals the source schema (`validate`) |
| `gold.<mart>` | `mode: batch`, `from: [silver.x, gold.y]`, `sql: <file>`, `schedule` | Trino SQL under Dagster, write-audit-publish | output columns equal the spec's (`schema` check) |

Silver is one Flink job: it reads each `cdc.ecommerce.<table>` topic as a Debezium changelog and upserts it by primary
key into `lakehouse.silver.<table>` (Iceberg v2, equality deletes), with `_source_ts` = commit time in PostgreSQL;
Iceberg commits on every 30 s checkpoint.

Gold marts, all in VND and business time (Asia/Ho_Chi_Minh): `fct_orders` (one row per order with its shop and
milestones), `daily_gmv` (orders, GMV, buyer paid, discounts, AOV, cancel rate per day), `orders_by_status`,
`seller_sla_daily` (packing SLA per shop and day). Metric definitions are in each spec's `description`.

### The CLI

```bash
pip install -r platform/requirements.txt
platform/cli.py validate          # specs, SQL, generated code, and the contract with the live source schema
platform/cli.py test              # gold SQL on fixtures (pipelines/gold/tests/) in Trino's memory catalog
platform/cli.py build gold.daily_gmv          # write-audit-publish now, as Dagster does on its schedule
platform/cli.py new silver.<table>            # or --missing; then: platform/cli.py generate
platform/cli.py new gold.<name>
platform/cli.py lineage           # the streaming lineage graph to Marquez
```

CI (`.github/workflows/platform.yml`) runs `validate` against a PostgreSQL loaded with `source/postgres/init.sql`
and `test` against a stock Trino, so a source sync that breaks a spec, or a gold query that breaks its fixtures,
fails the pull request.

After `scripts/sync-source.sh` brings a schema change, `validate` names what changed (new or dropped columns and
tables, type, key and replica identity changes). Update the silver specs, then:

```bash
platform/cli.py generate && docker compose build flink-jobmanager
```

A changed job is not hot-swapped: cancel `silver-cdc` in the Flink UI, then `docker compose up -d flink-silver`
(it resumes from the Kafka offsets of its last checkpoint; new columns need the Iceberg table evolved or dropped).

### Validation

| Gate | Where | What |
|---|---|---|
| Source contract | CI, `validate` | Silver specs = source tables, columns, types, keys, replica identity; gold SQL reads only what its spec declares |
| Spec tests | CI, `test` | Each gold query on its fixtures, with the same schema, key and expectation checks as a real build |
| Write-audit-publish | Dagster, every gold build | Query → `lakehouse.audit.<name>__<run>`; schema, key and expectations run there; `block` stops the publish (and everything downstream), `quarantine` moves failing rows to `lakehouse.quarantine.gold_<name>`, `warn` counts them. Publish is one Iceberg commit (`CREATE OR REPLACE TABLE`) |
| Live checks | Dagster `checks`, every 5 min | Silver expectations (`quarantine` copies failing rows to `quarantine.silver_<table>`), silver freshness = age of the oldest change still waiting in Kafka, gold freshness = time since the last publish |
| Reconciliation | Dagster (gold), `quality/reconcile.py` | Silver per table and gold metrics against PostgreSQL |

The audit step uses a staging table, not an Iceberg branch: Trino 484 parses `CREATE BRANCH` but its Iceberg
connector does not implement branches yet ([wap.py](../platform/streamcart/wap.py)). A blocked run keeps its audit
table for inspection until the next successful publish; results go to Dagster and to `ops.check_results` /
`ops.publishes` in `platform-db`.

### Dagster

<http://localhost:3070>: groups `source` → `silver` → `gold`, each asset with its owner, columns and checks.

| Job | Schedule | Does |
|---|---|---|
| `gold` | from the specs (`*/10 * * * *`) | write-audit-publish of the gold marts, then their reconcile checks |
| `checks` | `*/5 * * * *` | silver expectations and freshness, gold freshness |
| `maintenance` | `30 3 * * *` | expire snapshots (> 1 h) and orphan files on every table, compact yesterday's landing partitions, drop audit tables older than a day |
| `streaming_lineage` | `0 4 * * *` | Debezium / Flink / Spark lineage to Marquez |
| sensor `silver_snapshots` | every 2 min | each new silver snapshot becomes a materialization |

### Lineage and health

Marquez (<http://localhost:3002>, profile `lineage`) holds the graph `postgres → debezium → kafka → flink → silver →
gold`, with schemas and column-level lineage (gold columns traced through their SQL by sqlglot).

Grafana (<http://localhost:3000>, profile `monitoring`): dashboard *StreamCart / Platform health* and alert rules
for Debezium not reading the slot, slot lag, silver consumer lag, the Flink job down or failing checkpoints, a
freshness breach, a blocked publish, failing checks and checks that stopped running. No contact point is
provisioned: alerts show under *Alerting*; add Slack or email there.

### Check the lakehouse against the source

`quality/reconcile.py` compares silver (rows, Σ version and latest `updated_at` per table) and, with `--gold`, every
gold metric that has a `reconcile:` block in its spec. The source must hold still:

```bash
curl -X POST localhost:8200/stop
curl -X PUT localhost:8200/clock -H 'Content-Type: application/json' -d '{"speed":0.001}'   # scheduled jobs idle too
quality/reconcile.py --wait 120
quality/reconcile.py --gold
curl -X PUT localhost:8200/clock -H 'Content-Type: application/json' -d '{"speed":240}'
```

## Reset

```bash
docker compose --profile '*' down -v
sudo rm -rf source/postgres/data      # bind-mounted database files, owned by the container user
docker compose up -d && debezium/register.sh
```

Secrets survive a reset; `./setup-env.sh --rotate` replaces them (stack down and data removed first).

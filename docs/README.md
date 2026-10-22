# StreamCart CDC

A real-time **Change Data Capture** lakehouse for a multi-seller marketplace (Shopee / TikTok Shop style).

The **source system** is shared with the [Simulated project](https://github.com/chimsedibui/Simulated-Real-time-CDC-with-Kafka-Debezium):
same PostgreSQL schema, same Source API and business rules, same identity service and traffic simulator. The two
projects differ only in what sits behind Kafka:

| Project | Behind Kafka |
|---|---|
| Simulated | ClickHouse bronze / silver / gold, Grafana |
| **StreamCart (this repo)** | Spark micro-batches → Apache Iceberg on S3 (SeaweedFS), Flink next |

```text
 simulator ─► identity (JWT) ─► Source API ─► PostgreSQL ─► Debezium ─► Kafka ─┬► Spark landing ─► landing.cdc_events  (raw, replayable)
 (buyers, sellers,              (only writer,  (enterprise_db,  (pgoutput,   topics ├► Flink upsert ──► silver.<table>      (current state, 31 tables)
  carriers, gateways)            outbox,        logical WAL)     PII hashed,        └► Streamlit dashboard
                                 idempotency)                    outbox)                    Iceberg v2 on S3 (SeaweedFS), catalog: Polaris
```

Where this is going: [platform plan](platform-plan.md) (a Databricks-style build / validate / manage platform).

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
| `polaris-bootstrap`, `polaris-init` | | One-shots: realm and root principal; catalog, roles and the `spark` / `flink` (write) and `analyst` (read-only) principals, whose credentials go to `secrets/polaris/` |
| `spark-landing` | | Structured Streaming: `cdc.ecommerce.*` → `lakehouse.landing.cdc_events` ([details](../spark_microbatch/README.md)) |
| `flink-jobmanager`, `flink-taskmanager` | `8082` (Flink UI) | Flink 1.20 session cluster with the Kafka and Iceberg connectors |
| `flink-silver` | | One-shot: submits `flink/sql/silver.sql` unless it is already running |
| `dashboard` | `8501` | Streamlit live counters from Kafka |

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

Query the lakehouse:

```bash
lakehouse/spark-sql.sh -e "SELECT status, count(*), sum(total) FROM lakehouse.silver.orders GROUP BY status"
```

## Silver layer (Flink)

`flink/sql/silver.sql` is **generated** by `flink/gen_silver.py` from the live source schema (column types, primary
keys) and the connector's capture filter: one Flink job reads each `cdc.ecommerce.<table>` topic as a Debezium
changelog and upserts it by primary key into `lakehouse.silver.<table>` (Iceberg v2, equality deletes), with
`_source_ts` = commit time in PostgreSQL. Iceberg commits on every 30 s checkpoint.

After `scripts/sync-source.sh` brings a schema change:

```bash
flink/gen_silver.py --check           # fails when silver.sql no longer matches the source (the contract)
flink/gen_silver.py && docker compose build flink-jobmanager
```

A changed job is not hot-swapped: cancel `silver-cdc` in the Flink UI, then `docker compose up -d flink-silver`
(it resumes from the Kafka offsets of its last checkpoint; new columns need the Iceberg table evolved or dropped).

Check that silver equals the source (`quality/reconcile.py`: rows, Σ version and latest `updated_at` per table):

```bash
curl -X POST localhost:8200/stop
curl -X PUT localhost:8200/clock -H 'Content-Type: application/json' -d '{"speed":0.001}'   # scheduled jobs idle too
quality/reconcile.py --wait 120
curl -X PUT localhost:8200/clock -H 'Content-Type: application/json' -d '{"speed":240}'
```

## Reset

```bash
docker compose down -v
sudo rm -rf source/postgres/data      # bind-mounted database files, owned by the container user
docker compose up -d && debezium/register.sh
```

Secrets survive a reset; `./setup-env.sh --rotate` replaces them (stack down and data removed first).

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
 simulator ─► identity (JWT) ─► Source API ─► PostgreSQL ─► Debezium ─► Kafka ─► Spark landing ─► Iceberg (S3)
 (buyers, sellers,              (only writer,  (enterprise_db,  (pgoutput,   cdc.ecommerce.<table>    landing.cdc_events
  carriers, gateways)            outbox,        logical WAL)     PII hashed,  events.<aggregate>
                                 idempotency)                    outbox)      └► Streamlit dashboard
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
| `iceberg-rest` | `8181` | Iceberg REST catalog |
| `spark-landing` | | Structured Streaming: `cdc.ecommerce.*` → `lakehouse.landing.cdc_events` ([details](../spark_microbatch/README.md)) |
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

Check what landed in Iceberg: see [spark_microbatch/README.md](../spark_microbatch/README.md#inspect-the-landed-data).

## Reset

```bash
docker compose down -v
sudo rm -rf source/postgres/data      # bind-mounted database files, owned by the container user
docker compose up -d && debezium/register.sh
```

Secrets survive a reset; `./setup-env.sh --rotate` replaces them (stack down and data removed first).

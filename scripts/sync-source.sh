#!/usr/bin/env bash
# Vendors the marketplace source system from the Simulated project, so both projects run the same data source
# (same schema, Source API, identity service and simulator) with different stacks behind Kafka.
#
#   scripts/sync-source.sh            # upstream main
#   scripts/sync-source.sh <ref>      # a branch, tag or commit
#
# Copied verbatim: source/ (Postgres schema and roles, identity, source-api, simulator), the monitor_reader role
# that source/compose.yml mounts, and the source system docs. Never edit those files here; change them upstream and
# sync again. The commit is recorded in source/UPSTREAM.
set -euo pipefail
cd "$(dirname "$0")/.."

UPSTREAM="${SOURCE_UPSTREAM:-git@github.com:chimsedibui/Simulated-Real-time-CDC-with-Kafka-Debezium.git}"
REF="${1:-main}"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

git clone -q "$UPSTREAM" "$tmp/upstream"
git -C "$tmp/upstream" checkout -q "$REF"
commit=$(git -C "$tmp/upstream" rev-parse HEAD)

# source/postgres/data is the local database volume: keep it
rsync -a --delete --exclude 'postgres/data/' --exclude 'UPSTREAM' "$tmp/upstream/source/" source/
mkdir -p observability docs/source
cp "$tmp/upstream/observability/setup-postgres.sql" observability/setup-postgres.sql
cp "$tmp/upstream/docs/architecture/erDiagram.txt" \
   "$tmp/upstream/docs/architecture/entity_relationships.md" \
   "$tmp/upstream/docs/architecture/ecommerce_system_flow.md" \
   "$tmp/upstream/docs/business/01-nghiep-vu-va-du-lieu-nguon.md" \
   "$tmp/upstream/docs/business/02-source-api-va-bao-mat.md" docs/source/

printf '%s\n%s\n' "$UPSTREAM" "$commit" > source/UPSTREAM
echo "source/ synced to $commit"

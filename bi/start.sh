#!/bin/bash
# Migrate the metadata database, create the admin (secrets/superset_admin_password), then serve. Idempotent.
set -euo pipefail
superset db upgrade
superset fab create-admin --username admin --firstname Admin --lastname StreamCart --email admin@streamcart.local \
  --password "$(cat /run/secrets/superset_admin_password)" || true      # exists: kept
superset init
# The lakehouse connection, its datasets and the KPI dashboard (bi/bootstrap.py), once the server answers
(until curl -sf localhost:8088/health >/dev/null; do sleep 3; done; python /app/bootstrap.py) &
exec /usr/bin/run-server.sh

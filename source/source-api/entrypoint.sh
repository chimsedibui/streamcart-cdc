#!/bin/sh
set -eu
# Per-container shared metrics for both Uvicorn workers. Reset once before spawning workers.
mkdir -p /tmp/prometheus
find /tmp/prometheus -maxdepth 1 -name '*.db' -type f -delete
exec uvicorn app.main:app --host 0.0.0.0 --port 8100 --workers "${API_WORKERS:-2}"

#!/bin/sh
# Service account passwords from the secret files mounted under /run/secrets. Runs at init on a fresh volume; ./observability/start.sh
# runs it again on an existing one, so a changed .env takes effect.
set -eu
psql -v ON_ERROR_STOP=1 -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -v admin_pw="$(cat /run/secrets/postgres_password)" -v cdc_pw="$(cat /run/secrets/cdc_db_password)" -v monitor_pw="$(cat /run/secrets/monitor_password)" \
  -v api_pw="$(cat /run/secrets/source_api_db_password)" -v reader_pw="$(cat /run/secrets/app_reader_password)" <<'SQL'
ALTER ROLE CURRENT_USER PASSWORD :'admin_pw';
ALTER ROLE cdc_user PASSWORD :'cdc_pw';
ALTER ROLE monitor_reader PASSWORD :'monitor_pw';
ALTER ROLE source_api PASSWORD :'api_pw';
ALTER ROLE app_reader PASSWORD :'reader_pw';
SQL

#!/bin/sh
# identity_db for the identity service, owned by identity_app, which can connect to nothing else. A publication
# belongs to one database, so credentials kept here can never reach CDC. Runs at init on a fresh volume and from
# observability/start.sh on an existing one (idempotent).
set -eu
psql -v ON_ERROR_STOP=1 -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v pw="$(cat /run/secrets/identity_db_password)" <<'SQL'
SELECT 'CREATE ROLE identity_app LOGIN' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'identity_app')\gexec
ALTER ROLE identity_app PASSWORD :'pw';
SELECT 'CREATE DATABASE identity_db OWNER identity_app'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'identity_db')\gexec
REVOKE CONNECT ON DATABASE identity_db FROM PUBLIC;
GRANT CONNECT ON DATABASE identity_db TO identity_app;
SQL

#!/bin/sh
# Connection strings from the mounted secret files, so no password sits in the compose file or `docker inspect`
set -eu
PLATFORM_DB_PASSWORD=$(cat /run/secrets/platform_db_password)
export PLATFORM_DB_PASSWORD
export OPS_DSN="postgresql://platform:${PLATFORM_DB_PASSWORD}@platform-db:5432/platform"
export SOURCE_DSN="postgresql://app_reader:$(cat /run/secrets/app_reader_password)@postgres:5432/enterprise_db"
exec "$@"

#!/usr/bin/env bash
# Creates the secrets/ directory (gitignored): one file per secret, mounted read-only into the services that need
# it under /run/secrets, so no password appears in `docker inspect` or the compose file. Also writes .env (the
# non-secret settings, from .env.example). Existing values are never replaced: running it again only adds what is
# missing. The file layout matches the Simulated project, whose source/ is vendored here (scripts/sync-source.sh).
#
#   ./setup-env.sh            create what is missing
#   ./setup-env.sh --rotate   new random values for every secret. Databases set their passwords on first start
#                             only, so rotate with the stack down and its volumes removed (a data reset); the new
#                             PII salt also changes every hashed value.
set -euo pipefail
cd "$(dirname "$0")"
umask 077
rand() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

[[ "${1:-}" == "" || "${1:-}" == "--rotate" ]] || { echo "usage: $0 [--rotate]" >&2; exit 2; }
if [[ "${1:-}" == "--rotate" ]]; then
  if [[ -n "$(docker ps -q --filter label=com.docker.compose.project=streamcart 2>/dev/null)" ]]; then
    echo "stop the stack and remove its volumes first: docker compose down -v" >&2; exit 1
  fi
  if [[ -d source/postgres/data || -n "$(docker volume ls -q --filter label=com.docker.compose.project=streamcart)" ]]; then
    echo "rotation requires a data reset: remove project volumes and source/postgres/data first" >&2; exit 1
  fi
  rm -rf secrets
  echo "Removed the old secrets; writing new ones"
fi

mkdir -p secrets/clients secrets/webhooks secrets/polaris
# databases (source/postgres/passwords.sh, identity.sh), the Debezium PII salt, the Polaris catalog's database and
# root principal; the spark and flink principals' credentials are written to secrets/polaris/ by polaris-init
for f in postgres_password cdc_db_password source_api_db_password app_reader_password identity_db_password \
         monitor_password pii_salt polaris_db_password polaris_root_secret \
         password_pepper admin_password clients/sim-orchestrator clients/quality clients/identity \
         webhooks/MOMO webhooks/VNPAY webhooks/NAPAS webhooks/GHN webhooks/GHTK webhooks/SPX webhooks/JT webhooks/VTP; do
  # no trailing newline: Debezium reads the file verbatim
  [[ -s "secrets/$f" ]] || { rand > "secrets/$f"; echo "Wrote secrets/$f"; }
done
[[ -f .env ]] || { cp .env.example .env; echo "Wrote .env"; }
# placeholders, filled by polaris-init: a bind-mounted file that does not exist yet would become a directory
for p in spark flink analyst; do touch "secrets/polaris/$p"; done

# readable by the container users (the files stay private to this machine: secrets/ is gitignored)
chmod 755 secrets secrets/clients secrets/webhooks secrets/polaris
find secrets -type f -exec chmod 644 {} +

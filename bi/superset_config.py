# Superset for the lakehouse: metadata in platform-db (database superset), secrets from mounted files
import os


def _secret(name: str) -> str:
    with open(f"/run/secrets/{name}") as f:
        return f.read().strip()


SECRET_KEY = _secret("superset_secret_key")
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://platform:{_secret('platform_db_password')}@platform-db:5432/superset"
# SQL Lab and charts read through Trino's read-only `lakehouse` catalog; no uploads, no DML
PREVENT_UNSAFE_DB_CONNECTIONS = False     # trino://superset@trino:8080 is on the compose network
FEATURE_FLAGS = {"DASHBOARD_RBAC": True}
SQLLAB_TIMEOUT = 120
SUPERSET_WEBSERVER_TIMEOUT = 120
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False
ROW_LIMIT = 50000
LANGUAGES = {}                             # no language picker
os.environ.setdefault("SUPERSET_LOAD_EXAMPLES", "no")

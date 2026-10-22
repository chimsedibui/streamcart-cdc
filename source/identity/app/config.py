"""Identity service settings. Secrets are files (mounted under /run/secrets), never environment values."""

import os
from pathlib import Path


def env_secret(name: str) -> str:
    """NAME from the environment, or else the file NAME_FILE points to (a secret mounted under /run/secrets)."""
    value = os.getenv(name)
    return value if value is not None else open(os.environ[f"{name}_FILE"]).read().strip()


SECRETS = Path(os.getenv("SECRETS_DIR", "/run/secrets"))
KEY_FILE = Path(os.getenv("SIGNING_KEY_FILE", "/keys/signing.pem"))       # generated on first start
ISSUER = os.getenv("ISSUER", "http://identity:8300")
AUDIENCE = os.getenv("AUDIENCE", "source-api")
SOURCE_API_URL = os.getenv("SOURCE_API_URL", "http://source-api:8100")

ACCESS_TTL_S = int(os.getenv("ACCESS_TTL_S", "600"))                      # users: 10 minutes
CLIENT_TTL_S = int(os.getenv("CLIENT_TTL_S", "900"))                      # service clients and exchanged tokens
REFRESH_TTL_S = int(os.getenv("REFRESH_TTL_S", str(30 * 86400)))
LOCKOUT_FAILURES = 10                                                     # wrong passwords within the window
LOCKOUT_WINDOW_S = 15 * 60                                                # ... lock the account this long
LOGIN_PER_IP_PER_MIN = int(os.getenv("LOGIN_PER_IP_PER_MIN", "120"))

DB = dict(host=os.getenv("DB_HOST", "postgres"), port=int(os.getenv("DB_PORT", "5432")), dbname="identity_db",
          user=os.getenv("DB_USER", "identity_app"), password=env_secret("DB_PASSWORD"))

# Service clients. The secret of each is the file secrets/clients/<id>; scopes are what its tokens carry.
# impersonate: may exchange its token for one acting as a customer or a shop (RFC 8693), audited through `act`.
CLIENTS = {
    "sim-orchestrator": {"scopes": ["admin", "ops:read", "sim:clock"], "impersonate": True},
    "quality":          {"scopes": ["admin", "ops:read", "pii:read", "sim:clock"], "impersonate": True},
    "identity":         {"scopes": ["customers:provision", "sellers:provision"], "impersonate": False},
}


def secret(path: str) -> str:
    return (SECRETS / path).read_text().strip()

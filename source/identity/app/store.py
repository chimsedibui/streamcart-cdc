"""identity_db: users, memberships, refresh tokens, audit. Kept apart from the business database on purpose:
a publication belongs to one database, so nothing here can ever reach CDC, Kafka or the warehouse."""

import threading
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

from .config import DB

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email         TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'ACTIVE', 'DISABLED')),
    failed_logins INTEGER NOT NULL DEFAULT 0,
    first_failed_at TIMESTAMPTZ,
    locked_until  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_users_email ON users (lower(email));
-- What a user may act as: a customer, a shop (owner or staff), or platform ops/admin
CREATE TABLE IF NOT EXISTS memberships (
    user_id       UUID NOT NULL REFERENCES users(id),
    kind          TEXT NOT NULL CHECK (kind IN ('customer', 'shop', 'ops', 'admin')),
    resource_id   UUID,
    role          TEXT NOT NULL DEFAULT 'owner',
    UNIQUE NULLS NOT DISTINCT (user_id, kind, resource_id)         -- ops/admin have no resource
);
-- Rotating refresh tokens: each use issues the next in the same family; presenting a used one revokes the family
CREATE TABLE IF NOT EXISTS refresh_tokens (
    token_hash    TEXT PRIMARY KEY,
    family        UUID NOT NULL,
    user_id       UUID NOT NULL REFERENCES users(id),
    expires_at    TIMESTAMPTZ NOT NULL,
    used_at       TIMESTAMPTZ,
    revoked_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_refresh_family ON refresh_tokens (family);
CREATE TABLE IF NOT EXISTS audit (
    id            BIGSERIAL PRIMARY KEY,
    at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    event         TEXT NOT NULL,
    outcome       TEXT NOT NULL,
    user_id       UUID,
    client_id     TEXT,
    detail        TEXT
);
"""

psycopg2.extras.register_uuid()
_pool = None
_lock = threading.Lock()


def pool() -> ThreadedConnectionPool:
    global _pool
    with _lock:
        if _pool is None:
            _pool = ThreadedConnectionPool(1, 10, cursor_factory=psycopg2.extras.RealDictCursor, **DB)
    return _pool


@contextmanager
def tx():
    conn = pool().getconn()
    try:
        with conn, conn.cursor() as cur:
            yield cur
    finally:
        pool().putconn(conn)


def migrate():
    with tx() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(42)")            # workers start together
        cur.execute(SCHEMA)


def audit(cur, event: str, outcome: str, user_id=None, client_id=None, detail=None):
    cur.execute("INSERT INTO audit (event, outcome, user_id, client_id, detail) VALUES (%s, %s, %s, %s, %s)",
                (event, outcome, user_id, client_id, detail))

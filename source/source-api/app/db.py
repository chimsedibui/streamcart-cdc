"""Connection pool and the one-transaction-per-command helper."""

import os
import threading
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from fastapi import HTTPException
from psycopg2.pool import ThreadedConnectionPool


def secret(name: str) -> str:
    """NAME from the environment, or else the file NAME_FILE points to (a secret mounted under /run/secrets)."""
    value = os.getenv(name)
    return value if value is not None else open(os.environ[f"{name}_FILE"]).read().strip()


psycopg2.extras.register_uuid()

POOL_MAX = int(os.getenv("DB_POOL_MAX", "20"))
POOL_WAIT_S = float(os.getenv("DB_POOL_WAIT_S", "10"))

_pool: ThreadedConnectionPool | None = None
# psycopg2's pool raises at once when empty; this makes requests queue for a connection instead
_slots = threading.BoundedSemaphore(POOL_MAX)


def init_pool():
    global _pool
    _pool = ThreadedConnectionPool(
        minconn=2,
        maxconn=POOL_MAX,
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        user=os.getenv("PG_USER", "admin"),
        password=secret("PG_PASSWORD"),
        dbname=os.getenv("PG_DB", "enterprise_db"),
        application_name="source-api",
    )


def close_pool():
    if _pool:
        _pool.closeall()


@contextmanager
def transaction():
    """One business command = one transaction: commit on success, roll back on any error."""
    if not _slots.acquire(timeout=POOL_WAIT_S):
        raise HTTPException(503, {"code": "db_busy", "message": "No database connection available"},
                            headers={"Retry-After": "1"})
    try:
        conn = _pool.getconn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            _pool.putconn(conn)
    finally:
        _slots.release()

"""Idempotent command execution keyed by the Idempotency-Key header.

The key is claimed inside the command's own transaction, so a retried request either
waits for the in-flight original (row lock on the key) or replays its stored response.
Only successful responses are stored: a rejected command rolls back the claim too,
and a retry runs the command again.
"""

import hashlib
import json

import psycopg2
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from . import auth
from .db import transaction
from .errors import DomainError


def _hash(body) -> str:
    return hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()


def run_command(key: str | None, endpoint: str, body, handler, status_code: int = 200):
    """Run `handler(cur)` in one transaction; replay the stored result for a repeated key.

    A deadlock or serialization failure rolls everything back and answers 503: retrying with the
    same key is safe and expected.
    """
    try:
        return _run(key, endpoint, body, handler, status_code)
    except (psycopg2.errors.DeadlockDetected, psycopg2.errors.SerializationFailure):
        raise HTTPException(503, {"code": "retry", "message": "Concurrent update, retry the request"},
                            headers={"Retry-After": "1"})


def _run(key: str | None, endpoint: str, body, handler, status_code: int):
    if key:
        key = f"{auth.current().key_scope}:{key}"[:100]    # one caller can never replay another caller's response
    with transaction() as cur:
        if key:
            request_hash = _hash(body)
            cur.execute(
                """INSERT INTO ecommerce.idempotency_keys (key, endpoint, request_hash)
                   VALUES (%s, %s, %s) ON CONFLICT (key) DO NOTHING RETURNING key""",
                (key, endpoint, request_hash),
            )
            if cur.fetchone() is None:
                cur.execute(
                    "SELECT endpoint, request_hash, status_code, response FROM ecommerce.idempotency_keys WHERE key = %s",
                    (key,),
                )
                stored = cur.fetchone()
                if stored["endpoint"] != endpoint or stored["request_hash"] != request_hash:
                    raise DomainError(422, "idempotency_key_reused",
                                      "Idempotency-Key was already used for a different request")
                return JSONResponse(stored["response"], status_code=stored["status_code"],
                                    headers={"Idempotent-Replay": "true"})

        result = jsonable_encoder(handler(cur))

        if key:
            cur.execute(
                "UPDATE ecommerce.idempotency_keys SET status_code = %s, response = %s WHERE key = %s",
                (status_code, json.dumps(result), key),
            )
    return JSONResponse(result, status_code=status_code)

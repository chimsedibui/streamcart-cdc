"""Identity service: registration, password login, rotating refresh tokens, client credentials and token exchange.

Issues EdDSA JWTs that the Source API verifies against /.well-known/jwks.json. Buyers and sellers register here;
the matching customer or shop is created in the Source API through its provisioning endpoints, with this service's
own client token. Replaceable by any OIDC provider: the Source API only knows the issuer, audience and JWKS URL.
"""

import hashlib
import hmac
import logging
import secrets as pysecrets
import threading
import time
import uuid
from collections import defaultdict, deque
from typing import Literal

import httpx
from argon2 import PasswordHasher
from argon2.exceptions import VerificationError
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, generate_latest, multiprocess
from pydantic import BaseModel, Field

from . import keys, store
from .config import (CLIENT_TTL_S, CLIENTS, ISSUER, LOCKOUT_FAILURES, LOCKOUT_WINDOW_S, LOGIN_PER_IP_PER_MIN,
                     REFRESH_TTL_S, SOURCE_API_URL, secret)

log = logging.getLogger("identity")
LOGINS = Counter("identity_logins_total", "Password logins", ["outcome"])
TOKENS = Counter("identity_tokens_total", "Tokens issued", ["grant"])

# OWASP minimum for argon2id (19 MiB, 2 passes); the pepper lives outside the database
HASHER = PasswordHasher(time_cost=2, memory_cost=19456, parallelism=1)
PEPPER = secret("password_pepper").encode()
EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange"
DUMMY_HASH = HASHER.hash("not-a-password")            # verified for unknown emails: same timing as a real check

app = FastAPI(title="Identity", version="1.0")


@app.on_event("startup")
def startup():
    store.migrate()
    _bootstrap_admin()


def _peppered(password: str) -> str:
    return hmac.new(PEPPER, password.encode(), hashlib.sha256).hexdigest()


def _error(status: int, code: str, message: str, **headers):
    raise HTTPException(status, {"code": code, "message": message}, headers=headers or None)


# ------------------------------------------------------------------ per-IP login limit (per worker, in memory)

_attempts: dict[str, deque] = defaultdict(deque)
_attempts_lock = threading.Lock()


def _limit(ip: str):
    now = time.monotonic()
    with _attempts_lock:
        q = _attempts[ip]
        while q and q[0] < now - 60:
            q.popleft()
        if len(q) >= LOGIN_PER_IP_PER_MIN:
            _error(429, "rate_limited", "Too many login attempts", **{"Retry-After": "60"})
        q.append(now)


# ------------------------------------------------------------------ tokens

def _claims_for(cur, user_id) -> dict:
    cur.execute("SELECT kind, resource_id, role FROM memberships WHERE user_id = %s", (user_id,))
    rows = cur.fetchall()
    kinds = {r["kind"] for r in rows}
    scopes = (["admin", "ops:read", "pii:read"] if "admin" in kinds else []) + (["ops:read"] if "ops" in kinds else [])
    return {
        "kind": "admin" if "admin" in kinds else "ops" if "ops" in kinds else
                "seller" if "shop" in kinds else "customer",
        "cid": next((str(r["resource_id"]) for r in rows if r["kind"] == "customer"), None),
        "shops": [str(r["resource_id"]) for r in rows if r["kind"] == "shop"],
        "scope": " ".join(sorted(set(scopes))),
    }


def _issue_user_tokens(cur, user_id, family=None) -> dict:
    refresh = pysecrets.token_urlsafe(32)
    cur.execute("""INSERT INTO refresh_tokens (token_hash, family, user_id, expires_at)
                   VALUES (%s, %s, %s, now() + %s * interval '1 second')""",
                (hashlib.sha256(refresh.encode()).hexdigest(), family or uuid.uuid4(), user_id, REFRESH_TTL_S))
    TOKENS.labels("user").inc()
    return {"access_token": keys.sign(str(user_id), **_claims_for(cur, user_id)), "token_type": "Bearer",
            "expires_in": keys.ACCESS_TTL_S, "refresh_token": refresh}


def _client(client_id: str, client_secret: str) -> dict:
    spec = CLIENTS.get(client_id)
    try:
        ok = spec is not None and hmac.compare_digest(secret(f"clients/{client_id}"), client_secret)
    except FileNotFoundError:
        ok = False
    if not ok:
        _error(401, "invalid_client", "Unknown client or wrong secret")
    return spec


def _service_token(client_id: str, scopes: list) -> str:
    return keys.sign(f"client:{client_id}", CLIENT_TTL_S, kind="service", scope=" ".join(scopes))


# ------------------------------------------------------------------ registration (customer, seller)

class AddressIn(BaseModel):
    recipient_name: str
    phone: str
    street: str
    district: str
    province: str
    make_default: bool = False


class RegisterCustomer(BaseModel):
    email: str = Field(pattern=r"^[^@\s]+@[^@\s]+$", max_length=80)
    password: str = Field(min_length=10, max_length=128)
    full_name: str
    phone: str
    segment: Literal["NEW", "REGULAR", "BARGAIN_HUNTER", "LOYAL", "BUSINESS"] = "NEW"
    registered_channel: Literal["APP", "WEB"] = "APP"
    address: AddressIn


class ShopIn(BaseModel):
    name: str
    tier: Literal["MALL", "PREFERRED", "NORMAL"]
    province: str


class RegisterSeller(BaseModel):
    email: str = Field(pattern=r"^[^@\s]+@[^@\s]+$", max_length=80)
    password: str = Field(min_length=10, max_length=128)
    legal_name: str
    shop: ShopIn


def _register(body, profile: dict, path: str, kind: str):
    """Saga: user PENDING → provision the customer or shop in the Source API (idempotent on the user id, so a
    retry after a failure completes the same registration) → membership → ACTIVE."""
    with store.tx() as cur:
        cur.execute("SELECT id, status FROM users WHERE lower(email) = lower(%s)", (body.email,))
        user = cur.fetchone()
        if user and user["status"] != "PENDING":
            _error(409, "email_taken", f"{body.email} is already registered")
        if not user:
            cur.execute("INSERT INTO users (email, password_hash) VALUES (%s, %s) RETURNING id, status",
                        (body.email, HASHER.hash(_peppered(body.password))))
            user = cur.fetchone()
    token = _service_token("identity", CLIENTS["identity"]["scopes"])
    r = httpx.post(f"{SOURCE_API_URL}{path}", json=profile, timeout=15,
                   headers={"Authorization": f"Bearer {token}", "Idempotency-Key": f"register-{user['id']}"})
    if r.status_code >= 400:
        detail = r.json().get("detail") if r.headers.get("content-type", "").startswith("application/json") else None
        raise HTTPException(r.status_code, detail or {"code": "provisioning_failed", "message": r.text[:200]})
    resource = r.json()
    with store.tx() as cur:
        cur.execute("INSERT INTO memberships (user_id, kind, resource_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                    (user["id"], kind, resource["id"]))
        cur.execute("UPDATE users SET status = 'ACTIVE' WHERE id = %s", (user["id"],))
        store.audit(cur, f"register_{kind}", "ok", user_id=user["id"])
        return {**_issue_user_tokens(cur, user["id"]), "user_id": str(user["id"]), kind: resource}


@app.post("/v1/auth/register", status_code=201)
def register(body: RegisterCustomer):
    return _register(body, body.model_dump(exclude={"password"}), "/v1/internal/customers", "customer")


@app.post("/v1/auth/register/seller", status_code=201)
def register_seller(body: RegisterSeller):
    return _register(body, body.model_dump(exclude={"password"}), "/v1/internal/sellers", "shop")


# ------------------------------------------------------------------ login, refresh, logout

class Login(BaseModel):
    email: str
    password: str


@app.post("/v1/auth/login")
def login(body: Login, request: Request):
    _limit(request.client.host if request.client else "unknown")
    failure = None                      # raised after the transaction, so failed-attempt counters are committed
    with store.tx() as cur:
        cur.execute("SELECT * FROM users WHERE lower(email) = lower(%s) FOR UPDATE", (body.email,))
        user = cur.fetchone()
        if user and user["locked_until"] and user["locked_until"].timestamp() > time.time():
            store.audit(cur, "login", "locked", user_id=user["id"])
            LOGINS.labels("locked").inc()
            failure = (423, "account_locked", "Too many failed attempts; try again later")
        else:
            failure = _check_password(cur, user, body.password)
        if not failure:
            return _logged_in(cur, user, body.password)
    _error(*failure)


def _check_password(cur, user, password: str):
    try:
        if not user or user["status"] != "ACTIVE":
            HASHER.verify(DUMMY_HASH, _peppered(password) + "x")
            raise VerificationError()
        HASHER.verify(user["password_hash"], _peppered(password))
        return None
    except VerificationError:
        if user:
            cur.execute("""UPDATE users SET
                               failed_logins = CASE WHEN first_failed_at > now() - %(w)s * interval '1 second'
                                                    THEN failed_logins + 1 ELSE 1 END,
                               first_failed_at = CASE WHEN first_failed_at > now() - %(w)s * interval '1 second'
                                                      THEN first_failed_at ELSE now() END
                           WHERE id = %(id)s RETURNING failed_logins""", {"w": LOCKOUT_WINDOW_S, "id": user["id"]})
            if cur.fetchone()["failed_logins"] >= LOCKOUT_FAILURES:
                cur.execute("UPDATE users SET locked_until = now() + %s * interval '1 second', failed_logins = 0 "
                            "WHERE id = %s", (LOCKOUT_WINDOW_S, user["id"]))
            store.audit(cur, "login", "bad_password", user_id=user["id"])
        LOGINS.labels("failed").inc()
        # the same answer whether the email exists or not
        return (401, "invalid_credentials", "Email or password is wrong")


def _logged_in(cur, user, password: str) -> dict:
    if HASHER.check_needs_rehash(user["password_hash"]):
        cur.execute("UPDATE users SET password_hash = %s WHERE id = %s", (HASHER.hash(_peppered(password)), user["id"]))
    cur.execute("UPDATE users SET failed_logins = 0, first_failed_at = NULL, locked_until = NULL WHERE id = %s",
                (user["id"],))
    store.audit(cur, "login", "ok", user_id=user["id"])
    LOGINS.labels("ok").inc()
    return _issue_user_tokens(cur, user["id"])


class RefreshIn(BaseModel):
    refresh_token: str


@app.post("/v1/auth/refresh")
def refresh(body: RefreshIn):
    digest = hashlib.sha256(body.refresh_token.encode()).hexdigest()
    with store.tx() as cur:
        cur.execute("SELECT * FROM refresh_tokens WHERE token_hash = %s FOR UPDATE", (digest,))
        row = cur.fetchone()
        if not row or row["revoked_at"] or row["expires_at"].timestamp() < time.time():
            _error(401, "invalid_grant", "Refresh token is not valid")
        if row["used_at"]:
            # a used token presented again: it was stolen or replayed, so the whole session ends
            cur.execute("UPDATE refresh_tokens SET revoked_at = now() WHERE family = %s AND revoked_at IS NULL",
                        (row["family"],))
            store.audit(cur, "refresh", "reuse_detected", user_id=row["user_id"])
            return JSONResponse({"detail": {"code": "invalid_grant", "message": "Refresh token was already used"}},
                                status_code=401)
        cur.execute("SELECT status FROM users WHERE id = %s", (row["user_id"],))
        if cur.fetchone()["status"] != "ACTIVE":
            _error(401, "invalid_grant", "Account is disabled")
        cur.execute("UPDATE refresh_tokens SET used_at = now() WHERE token_hash = %s", (digest,))
        return _issue_user_tokens(cur, row["user_id"], family=row["family"])


@app.post("/v1/auth/logout", status_code=204)
def logout(body: RefreshIn):
    digest = hashlib.sha256(body.refresh_token.encode()).hexdigest()
    with store.tx() as cur:
        cur.execute("""UPDATE refresh_tokens SET revoked_at = now()
                       WHERE family = (SELECT family FROM refresh_tokens WHERE token_hash = %s) AND revoked_at IS NULL
                       RETURNING user_id""", (digest,))
        row = cur.fetchone()
        store.audit(cur, "logout", "ok" if row else "unknown_token", user_id=row["user_id"] if row else None)
    return PlainTextResponse(status_code=204)


# ------------------------------------------------------------------ service clients (OAuth2 token endpoint)

@app.post("/v1/auth/token")
def token(grant_type: str = Form(...), client_id: str = Form(...), client_secret: str = Form(...),
          scope: str = Form(""), requested_subject: str = Form("")):
    spec = _client(client_id, client_secret)
    if grant_type == "client_credentials":
        wanted = scope.split() or spec["scopes"]
        if not set(wanted) <= set(spec["scopes"]):
            _error(400, "invalid_scope", f"{client_id} may not request {sorted(set(wanted) - set(spec['scopes']))}")
        TOKENS.labels("client_credentials").inc()
        return {"access_token": _service_token(client_id, wanted), "token_type": "Bearer", "expires_in": CLIENT_TTL_S}
    if grant_type == EXCHANGE:
        # act as a customer or a shop, e.g. the simulator playing a buyer; `act` names the real caller
        kind, _, rid = requested_subject.partition(":")
        if not spec["impersonate"] or kind not in ("customer", "shop"):
            _error(400, "invalid_request", "This client may not exchange tokens for that subject")
        try:
            uuid.UUID(rid)
        except ValueError:
            _error(400, "invalid_request", "requested_subject must be customer:<uuid> or shop:<uuid>")
        TOKENS.labels("token_exchange").inc()
        claims = {"kind": "customer", "cid": rid} if kind == "customer" else {"kind": "seller", "shops": [rid]}
        return {"access_token": keys.sign(requested_subject, CLIENT_TTL_S, act={"sub": f"client:{client_id}"},
                                          **claims),
                "token_type": "Bearer", "expires_in": CLIENT_TTL_S,
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token"}
    _error(400, "unsupported_grant_type", grant_type)


# ------------------------------------------------------------------ discovery, admin bootstrap, health

@app.get("/.well-known/jwks.json")
def jwks():
    return keys.JWKS


@app.get("/.well-known/openid-configuration")
def discovery():
    return {"issuer": ISSUER, "jwks_uri": f"{ISSUER}/.well-known/jwks.json", "token_endpoint": f"{ISSUER}/v1/auth/token",
            "grant_types_supported": ["password", "refresh_token", "client_credentials", EXCHANGE],
            "id_token_signing_alg_values_supported": ["EdDSA"]}


def _bootstrap_admin():
    """The platform admin login (secrets/admin_password), created once."""
    with store.tx() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(43)")
        cur.execute("SELECT id FROM users WHERE lower(email) = 'admin@cdc.local'")
        if cur.fetchone():
            return
        cur.execute("INSERT INTO users (email, password_hash, status) VALUES ('admin@cdc.local', %s, 'ACTIVE') RETURNING id",
                    (HASHER.hash(_peppered(secret("admin_password"))),))
        cur.execute("INSERT INTO memberships (user_id, kind, resource_id) VALUES (%s, 'admin', NULL)",
                    (cur.fetchone()["id"],))


@app.get("/health")
def health():
    with store.tx() as cur:
        cur.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)                 # all four workers
    return PlainTextResponse(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

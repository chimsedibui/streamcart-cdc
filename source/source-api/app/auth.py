"""Who is calling, and may they? Bearer JWTs from the identity service (EdDSA, verified against its JWKS) and
HMAC-signed partner webhooks.

AUTH_MODE: off — no checks (the legacy behaviour); permissive — every decision is evaluated and counted in
auth_decisions_total, but a would-be denial is only logged; enforce — denials are answered with 401/403/404.
The principal of the request is kept in a context variable, so handlers and the idempotency layer read it without
passing it through every call.
"""

import contextvars
import hashlib
import hmac
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import jwt
from fastapi import HTTPException, Request
from prometheus_client import Counter

log = logging.getLogger("auth")

MODE = os.getenv("AUTH_MODE", "off")
ISSUER = os.getenv("AUTH_ISSUER", "http://identity:8300")
AUDIENCE = os.getenv("AUTH_AUDIENCE", "source-api")
JWKS_URL = os.getenv("AUTH_JWKS_URL", f"{ISSUER}/.well-known/jwks.json")
WEBHOOK_SECRETS = Path(os.getenv("WEBHOOK_SECRETS_DIR", "/run/secrets/webhooks"))
WEBHOOK_SKEW_S = 300                         # wall clock: the business clock runs faster and must not widen this
LEEWAY_S = 60

DECISIONS = Counter("auth_decisions_total", "Authorization decisions", ["outcome"])

# routes anyone may call: the public catalogue, the clock, probes; webhooks authenticate with a signature instead
PUBLIC = {"/health", "/metrics", "/categories", "/brands", "/vouchers", "/campaigns", "/campaigns/{campaign_id}/items",
          "/skus", "/shops", "/clock", "/docs", "/openapi.json"}
SIGNED = {"/payments/webhook", "/logistics/webhook"}


@dataclass(frozen=True)
class Principal:
    sub: str
    kind: str                                 # customer | seller | ops | admin | service | partner | anonymous | system
    customer_id: str | None = None
    shop_ids: frozenset = field(default_factory=frozenset)
    scopes: frozenset = field(default_factory=frozenset)
    actor: str | None = None                  # the real caller when a client acts as a customer or shop

    @property
    def admin(self) -> bool:
        return self.kind == "system" or "admin" in self.scopes

    def can_read_all(self) -> bool:
        return self.admin or "ops:read" in self.scopes

    @property
    def key_scope(self) -> str:
        """Prefix that keeps one caller's Idempotency-Keys apart from everyone else's."""
        return hashlib.sha256(self.sub.encode()).hexdigest()[:16]


SYSTEM = Principal("system", "system")
ANONYMOUS = Principal("anonymous", "anonymous")
_current: contextvars.ContextVar[Principal] = contextvars.ContextVar("principal", default=SYSTEM)


def current() -> Principal:
    return _current.get()


# ------------------------------------------------------------------ JWT verification

_keys: dict = {}
_keys_at = 0.0
_keys_lock = threading.Lock()


def _key(kid: str):
    """Signing keys by kid, refetched when an unknown kid shows up (rotation), at most every 30 s."""
    global _keys, _keys_at
    with _keys_lock:
        if kid not in _keys and time.monotonic() - _keys_at > 30:
            jwks = httpx.get(JWKS_URL, timeout=5).json()
            _keys = {k["kid"]: jwt.PyJWK(k).key for k in jwks["keys"]}
            _keys_at = time.monotonic()
        return _keys.get(kid)


def _verify(token: str) -> Principal:
    try:
        kid = jwt.get_unverified_header(token).get("kid", "")
        key = _key(kid)
        if key is None:
            raise jwt.InvalidTokenError("unknown signing key")
        c = jwt.decode(token, key, algorithms=["EdDSA"], audience=AUDIENCE, issuer=ISSUER, leeway=LEEWAY_S,
                       options={"require": ["exp", "iat", "sub", "aud", "iss"]})
    except (jwt.InvalidTokenError, httpx.HTTPError, ValueError) as e:
        raise HTTPException(401, {"code": "unauthenticated", "message": f"Invalid token: {e}"},
                            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'})
    return Principal(sub=c["sub"], kind=c.get("kind", "customer"), customer_id=c.get("cid"),
                     shop_ids=frozenset(c.get("shops", [])), scopes=frozenset(c.get("scope", "").split()),
                     actor=(c.get("act") or {}).get("sub"))


async def authenticate(request: Request):
    """App-wide dependency: resolve the caller once per request (async, so the context variable it sets is the
    one the endpoint's thread runs in)."""
    if MODE == "off":
        _current.set(SYSTEM)
        return
    path = request.scope.get("route").path if request.scope.get("route") else request.url.path
    path = path.removeprefix("/v1")
    header = request.headers.get("authorization", "")
    if header.lower().startswith("bearer "):
        principal = _verify(header[7:].strip())
    elif path in PUBLIC or path in SIGNED:
        principal = ANONYMOUS
    else:
        principal = ANONYMOUS
        deny(401, "unauthenticated", "A bearer token is required")
    _current.set(principal)


def deny(status: int, code: str, message: str):
    DECISIONS.labels("denied" if MODE == "enforce" else "would_deny").inc()
    if MODE == "enforce":
        headers = {"WWW-Authenticate": "Bearer"} if status == 401 else None
        raise HTTPException(status, {"code": code, "message": message}, headers=headers)
    log.warning("authz would deny (%s): %s %s for %s", MODE, code, message, current().sub)


def _allow():
    DECISIONS.labels("allowed").inc()


# ------------------------------------------------------------------ decisions used by the routers

def require_scope(scope: str):
    p = current()
    if p.admin or scope in p.scopes:
        return _allow()
    deny(403, "insufficient_scope", f"Requires scope {scope}")


def customer(customer_id, entity: str = "customer") -> None:
    """The caller acts as this customer (writes). Someone else's resource answers 404, not 403: no existence leak."""
    p = current()
    if p.admin or (p.customer_id and str(customer_id) == p.customer_id):
        return _allow()
    deny(404, f"{entity}_not_found", f"{entity} does not exist")


def read_customer(customer_id, entity: str = "customer") -> None:
    if current().can_read_all():
        return _allow()
    customer(customer_id, entity)


def shop(shop_id, entity: str = "shop") -> None:
    p = current()
    if p.admin or str(shop_id) in p.shop_ids:
        return _allow()
    deny(404, f"{entity}_not_found", f"{entity} does not exist")


def own_customer_id(given) -> str:
    """customer_id of a buyer command: the token's, or the body's when it matches (kept for older clients)."""
    p = current()
    if given is None:
        if p.customer_id is None:
            deny(403, "insufficient_scope", "Only a customer can do this")
        return p.customer_id
    customer(given)
    return str(given)


def read_customer_id(given) -> str:
    """customer_id of a buyer read: the token's by default; ops and admins may name anyone."""
    if given is None:
        return own_customer_id(None)
    read_customer(given)
    return str(given)


def read_party(customer_id, shop_id, entity: str) -> None:
    """Reads of something between a buyer and a shop (an order, a return): either side, ops or an admin."""
    p = current()
    if p.can_read_all() or (p.customer_id and str(customer_id) == p.customer_id) or str(shop_id) in p.shop_ids:
        return _allow()
    deny(404, f"{entity}_not_found", f"{entity} does not exist")


def scope_listing(customer_id, shop_id):
    """Listings: ops see everything; a buyer only their own rows, a seller only their shop's."""
    p = current()
    if p.can_read_all():
        _allow()
        return customer_id, shop_id
    if p.customer_id:
        return read_customer_id(customer_id), shop_id
    if p.shop_ids:
        return customer_id, own_shop_id(shop_id)
    deny(403, "insufficient_scope", "Requires a customer, a seller or ops:read")
    return customer_id, shop_id


def own_shop_id(given) -> str:
    p = current()
    if given is None:
        if len(p.shop_ids) != 1:
            deny(403, "insufficient_scope", "Only a seller of exactly one shop can omit shop_id")
            return None
        return next(iter(p.shop_ids))
    shop(given)
    return str(given)


def mask_pii() -> bool:
    """Ops read personal data masked unless they hold pii:read; owners and admins see their own in clear."""
    p = current()
    return MODE != "off" and not p.admin and "pii:read" not in p.scopes and p.kind != "customer"


def mask(value):
    if not isinstance(value, str) or not value:
        return value
    return value[:1] + "***" + (value[value.find("@"):] if "@" in value else value[-2:])


# ------------------------------------------------------------------ partner webhooks

async def verify_webhook(request: Request) -> str | None:
    """X-Partner + X-Signature: t=<unix>,v1=<hex HMAC-SHA256(secret, t + "." + body)>. Returns the partner."""
    if MODE == "off":
        return None
    partner = request.headers.get("x-partner", "")
    sig = dict(part.split("=", 1) for part in request.headers.get("x-signature", "").split(",") if "=" in part)
    body = await request.body()
    try:
        secret = (WEBHOOK_SECRETS / partner).read_bytes().strip() if partner and "/" not in partner else None
    except FileNotFoundError:
        secret = None
    ok = False
    if secret and "t" in sig and "v1" in sig and sig["t"].isdigit():
        fresh = abs(time.time() - int(sig["t"])) <= WEBHOOK_SKEW_S
        expected = hmac.new(secret, f"{sig['t']}.".encode() + body, hashlib.sha256).hexdigest()
        ok = fresh and hmac.compare_digest(expected, sig["v1"])
    if not ok:
        deny(401, "invalid_signature", "Webhook signature missing, stale or wrong")
        return None
    _current.set(Principal(f"partner:{partner}", "partner"))
    _allow()
    return partner


def partner_id(name: str) -> str:
    """Partner names as they appear in X-Partner and as secret file names: MOMO, VNPAY, GHN, JT (J&T)…"""
    return "".join(ch for ch in name.upper() if ch.isalnum())


def partner_is(expected: str, partner: str | None, entity: str) -> None:
    """A partner may only report on its own payments or parcels."""
    if MODE == "off" or partner is None and current().admin:
        return
    if partner != partner_id(expected):
        deny(403, "wrong_partner", f"{partner} may not report on a {entity} handled by {expected}")

"""HTTP client for the Source API — the simulator's only way to change data.

Every call carries a token from the identity service: the orchestrator's own (client credentials: admin work and
the global lists the actors pick from) or one obtained by token exchange for the buyer or shop the actor is
playing, so the API checks ownership exactly as for real users. Partner callbacks are HMAC-signed instead.
"""

import hashlib
import hmac
import json
import os
import threading
import time
import uuid
from collections import OrderedDict
from pathlib import Path

import httpx

SECRETS = Path(os.getenv("SECRETS_DIR", "/run/secrets"))


def partner_id(name: str) -> str:
    """Same normalisation as the API: J&T -> JT."""
    return "".join(ch for ch in name.upper() if ch.isalnum())


class Tokens:
    """The orchestrator's token and exchanged per-subject tokens, cached until a minute before they expire."""

    EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange"

    def __init__(self, identity_url: str, client_id: str):
        self.http = httpx.Client(base_url=identity_url, timeout=10.0)
        self.client_id = client_id
        self.secret = (SECRETS / "clients" / client_id).read_text().strip()
        self.cache: OrderedDict[str, tuple[str, float]] = OrderedDict()
        self.lock = threading.Lock()

    def _fetch(self, subject: str | None) -> tuple[str, float]:
        form = {"client_id": self.client_id, "client_secret": self.secret}
        form |= {"grant_type": self.EXCHANGE, "requested_subject": subject} if subject else \
                {"grant_type": "client_credentials"}
        r = self.http.post("/v1/auth/token", data=form)
        r.raise_for_status()
        body = r.json()
        return body["access_token"], time.time() + body["expires_in"] - 60

    def get(self, subject: str | None = None) -> str:
        key = subject or "_self"
        with self.lock:
            hit = self.cache.get(key)
            if hit and hit[1] > time.time():
                self.cache.move_to_end(key)
                return hit[0]
        token = self._fetch(subject)
        with self.lock:
            self.cache[key] = token
            while len(self.cache) > 20000:
                self.cache.popitem(last=False)
        return token[0]

    def password(self, email: str) -> str:
        """Simulated users' passwords: derived from the client secret, so none has to be stored."""
        return hmac.new(self.secret.encode(), email.lower().encode(), hashlib.sha256).hexdigest()[:24]

    def register(self, path: str, profile: dict) -> dict:
        """Sign a buyer or seller up through the identity service; the API record is created by it."""
        r = self.http.post(path, json={**profile, "password": self.password(profile["email"])})
        if r.status_code >= 400:
            raise ApiError(r.status_code, _json(r))
        return r.json()

    def login(self, subject: str, email: str, password: str | None = None) -> bool:
        """A real password login whose token then serves `subject`, as an app session would."""
        r = self.http.post("/v1/auth/login", json={"email": email, "password": password or self.password(email)})
        if r.status_code != 200:
            return False
        body = r.json()
        with self.lock:
            self.cache[subject] = (body["access_token"], time.time() + body["expires_in"] - 60)
        return True

    def drop(self, subject: str | None):
        with self.lock:
            self.cache.pop(subject or "_self", None)


class ApiError(Exception):
    def __init__(self, status: int, body):
        detail = body.get("detail") if isinstance(body, dict) else None
        self.status = status
        self.code = detail.get("code") if isinstance(detail, dict) else None
        self.detail = detail
        super().__init__(f"{status} {self.code or body}")


class SourceApi:
    def __init__(self, base_url: str, timeout: float = 10.0, retries: int = 3, tokens: Tokens | None = None):
        self.tokens = tokens
        self.http = httpx.Client(base_url=base_url, timeout=timeout,
                                 limits=httpx.Limits(max_connections=32, max_keepalive_connections=16))
        self.retries = retries
        self.lock = threading.Lock()
        self.counters = {"requests": 0, "retries": 0, "replays": 0, "rejected": 0, "failed": 0}

    def _count(self, name: str):
        with self.lock:
            self.counters[name] += 1

    def _send(self, method: str, path: str, as_: str | None = None, signed: str | None = None,
              **kwargs) -> httpx.Response:
        """as_: act as "customer:<id>" or "shop:<id>" (None: the orchestrator). signed: partner whose key signs."""
        path = path if path.startswith("/v1/") else "/v1" + path
        headers = dict(kwargs.pop("headers", {}))
        if signed:
            raw = json.dumps(kwargs.pop("json", {})).encode()
            kwargs["content"], headers["Content-Type"] = raw, "application/json"
        refreshed = False
        for attempt in range(self.retries + 1):
            self._count("requests")
            if signed:                                     # re-signed on every delivery, like a real provider
                ts = str(int(time.time()))
                secret = (SECRETS / "webhooks" / partner_id(signed)).read_bytes().strip()
                headers["X-Partner"] = partner_id(signed)
                headers["X-Signature"] = f"t={ts},v1=" + hmac.new(secret, ts.encode() + b"." + raw,
                                                                   hashlib.sha256).hexdigest()
            elif self.tokens:
                headers["Authorization"] = f"Bearer {self.tokens.get(as_)}"
            try:
                r = self.http.request(method, path, headers=headers, **kwargs)
            except httpx.TransportError:
                if attempt == self.retries:
                    self._count("failed")
                    raise
            else:
                if r.status_code == 401 and self.tokens and not signed and not refreshed:
                    self.tokens.drop(as_)                  # expired or rotated key: one fresh token, then give up
                    refreshed = True
                    continue
                if r.status_code < 500:
                    return r
                if attempt == self.retries:
                    self._count("failed")
                    return r
            self._count("retries")
            time.sleep(0.2 * 2 ** attempt)
        raise RuntimeError("unreachable")

    def command(self, path: str, body: dict | None = None, method: str = "POST", key: str | None = None,
                as_: str | None = None, signed: str | None = None) -> dict:
        """Send a business command; retries reuse the same Idempotency-Key, so they never apply twice.

        Webhook senders pass their own event id as `key`, so a redelivered callback is deduplicated.
        """
        key = key or str(uuid.uuid4())
        kwargs = {"headers": {"Idempotency-Key": key}}
        if method != "DELETE":
            kwargs["json"] = body or {}
        r = self._send(method, path, as_=as_, signed=signed, **kwargs)
        if r.headers.get("Idempotent-Replay"):
            self._count("replays")
        if r.status_code >= 400:
            if r.status_code < 500:
                self._count("rejected")
            raise ApiError(r.status_code, _json(r))
        return r.json()

    def query(self, path: str, as_: str | None = None, **params):
        r = self._send("GET", path, as_=as_, params={k: v for k, v in params.items() if v is not None})
        if r.status_code >= 400:
            raise ApiError(r.status_code, _json(r))
        return r.json()


def _json(r: httpx.Response):
    try:
        return r.json()
    except ValueError:
        return r.text

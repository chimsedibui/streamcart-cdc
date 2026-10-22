"""Ed25519 signing key: generated once into the identity_keys volume, published as a JWKS for verifiers."""

import base64
import fcntl
import hashlib
import time
import uuid

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from .config import ACCESS_TTL_S, AUDIENCE, ISSUER, KEY_FILE


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _load() -> Ed25519PrivateKey:
    KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(KEY_FILE.with_suffix(".lock"), "w") as lock:         # several workers start at once
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not KEY_FILE.exists():
            pem = Ed25519PrivateKey.generate().private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
            KEY_FILE.write_bytes(pem)
            KEY_FILE.chmod(0o600)
    return serialization.load_pem_private_key(KEY_FILE.read_bytes(), password=None)


PRIVATE = _load()
_PUBLIC = PRIVATE.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
KID = _b64(hashlib.sha256(_PUBLIC).digest())[:16]
JWKS = {"keys": [{"kty": "OKP", "crv": "Ed25519", "x": _b64(_PUBLIC), "kid": KID, "use": "sig", "alg": "EdDSA"}]}


def sign(sub: str, ttl_s: int = ACCESS_TTL_S, **claims) -> str:
    now = int(time.time())
    payload = {"iss": ISSUER, "aud": AUDIENCE, "sub": sub, "iat": now, "nbf": now, "exp": now + ttl_s,
               "jti": str(uuid.uuid4()), **{k: v for k, v in claims.items() if v not in (None, [], "")}}
    return jwt.encode(payload, PRIVATE, algorithm="EdDSA", headers={"kid": KID})

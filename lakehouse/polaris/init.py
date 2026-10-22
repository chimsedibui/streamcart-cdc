"""Sets up the Polaris catalog once: the `lakehouse` catalog on s3://warehouse/, its roles, and one principal per
engine, each with only the access its job needs. Idempotent: existing objects are kept; a principal whose
credential file is missing gets its credentials rotated and written again.

Credentials are written as clientId:clientSecret to /secrets/polaris/<principal> (./secrets/polaris on the host),
the form Iceberg's REST catalog `credential` property takes.
"""

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

POLARIS = os.getenv("POLARIS_URL", "http://polaris:8181")
CATALOG = "lakehouse"
WAREHOUSE = "s3://warehouse/"
OUT = Path("/secrets/polaris")
ROOT_SECRET = Path("/run/secrets/polaris_root_secret").read_text().strip()

# catalog role -> catalog-level privileges
ROLES = {
    "writer": ["CATALOG_MANAGE_CONTENT"],
    "reader": ["CATALOG_READ_PROPERTIES", "NAMESPACE_LIST", "NAMESPACE_READ_PROPERTIES", "TABLE_LIST",
               "TABLE_READ_PROPERTIES", "TABLE_READ_DATA"],
}
# principal -> catalog role: the engines write (landing, silver); analysts and BI only read
PRINCIPALS = {"spark": "writer", "flink": "writer", "analyst": "reader"}


def call(method: str, path: str, body=None, token=None, form=None, ok=(200, 201, 204)):
    data = urllib.parse.urlencode(form).encode() if form else json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/x-www-form-urlencoded" if form else "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"{POLARIS}{path}", data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            text = r.read().decode()
            return r.status, json.loads(text) if text else None
    except urllib.error.HTTPError as e:
        if e.code in ok or e.code == 409:          # 409: already exists
            return e.code, None
        raise SystemExit(f"{method} {path}: HTTP {e.code} {e.read().decode()[:500]}")


def token() -> str:
    for _ in range(60):
        try:
            _, body = call("POST", "/api/catalog/v1/oauth/tokens", form={
                "grant_type": "client_credentials", "client_id": "root", "client_secret": ROOT_SECRET,
                "scope": "PRINCIPAL_ROLE:ALL"})
            return body["access_token"]
        except (urllib.error.URLError, ConnectionError):
            time.sleep(2)                             # Polaris still starting
    raise SystemExit("Polaris did not come up")


def main():
    t = token()
    m = "/api/management/v1"
    status, _ = call("POST", f"{m}/catalogs", token=t, body={"catalog": {
        "type": "INTERNAL", "name": CATALOG,
        "properties": {"default-base-location": WAREHOUSE},
        "storageConfigInfo": {"storageType": "S3", "allowedLocations": [WAREHOUSE],
                              "endpoint": os.getenv("S3_ENDPOINT", "http://seaweedfs:8333"),
                              "pathStyleAccess": True, "stsUnavailable": True}}})
    print(f"catalog {CATALOG}: {'created' if status == 201 else 'exists'}")

    for role, privileges in ROLES.items():
        call("POST", f"{m}/catalogs/{CATALOG}/catalog-roles", token=t, body={"catalogRole": {"name": role}})
        for p in privileges:
            call("PUT", f"{m}/catalogs/{CATALOG}/catalog-roles/{role}/grants", token=t,
                 body={"grant": {"type": "catalog", "privilege": p}})

    OUT.mkdir(parents=True, exist_ok=True)
    for name, role in PRINCIPALS.items():
        status, body = call("POST", f"{m}/principals", token=t, body={"principal": {"name": name}})
        path = OUT / name
        if status == 201 or not path.exists() or path.stat().st_size == 0:   # empty: setup-env.sh placeholder
            if status != 201:                         # exists, but its secret was lost: issue a new one
                _, body = call("POST", f"{m}/principals/{name}/rotate", token=t)
            c = body["credentials"]
            path.write_text(f"{c['clientId']}:{c['clientSecret']}")      # in place: the file is bind-mounted
        call("POST", f"{m}/principal-roles", token=t, body={"principalRole": {"name": name}})
        call("PUT", f"{m}/principals/{name}/principal-roles", token=t, body={"principalRole": {"name": name}})
        call("PUT", f"{m}/principal-roles/{name}/catalog-roles/{CATALOG}", token=t,
             body={"catalogRole": {"name": role}})
        print(f"principal {name}: {role}")


if __name__ == "__main__":
    main()

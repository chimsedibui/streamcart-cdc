"""Source API — the only writer to the operational PostgreSQL database.

Every endpoint that changes data is one business command: it runs in a single
transaction, writes a domain event to ecommerce.outbox_events, and accepts an
Idempotency-Key header so clients (and webhook senders) can retry safely.

Callers authenticate with identity-service JWTs (partners with signed webhooks); see auth.py and AUTH_MODE.
Routes live under /v1; the unversioned paths remain only while AUTH_MODE is not enforce.
"""

import os
import threading
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from prometheus_fastapi_instrumentator import Instrumentator, metrics

from . import auth
from .db import close_pool, init_pool, transaction
from .jobs import start_scheduler
from .routers import admin, carts, checkouts, customers, logistics, orders, returns, sellers


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_pool()
    stop = threading.Event()
    if os.getenv("RUN_JOBS", "true").lower() == "true":
        start_scheduler(stop)
    yield
    stop.set()
    close_pool()


app = FastAPI(title="Marketplace Source API", version="1.0.0", lifespan=lifespan,
              dependencies=[Depends(auth.authenticate)])
for module in (admin, sellers, customers, carts, checkouts, orders, logistics, returns):
    app.include_router(module.router, prefix="/v1")
    if auth.MODE != "enforce":
        app.include_router(module.router, include_in_schema=False)
for module in (customers, sellers):
    app.include_router(module.internal, prefix="/v1")      # provisioning by the identity service only


instrumentator = Instrumentator(excluded_handlers=["/health", "/metrics"], should_ignore_untemplated=True)
# Fine per-handler buckets, so checkout p99 can be read against its 300 ms SLO
instrumentator.add(metrics.default(latency_lowr_buckets=(0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1, 2, 5)))
instrumentator.instrument(app).expose(app)


@app.get("/health")
def health():
    with transaction() as cur:
        cur.execute("SELECT 1")
    return {"status": "ok"}

"""Control API of the simulator, used by the Control Center."""

import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .client import SourceApi, Tokens
from .config import load_config
from .engine import Simulator

cfg = load_config()
sim = Simulator(cfg, SourceApi(cfg["source_api_url"], tokens=Tokens(cfg["identity_url"], "sim-orchestrator")))
ACTIONS = {"customer", "order", "cart", "advance", "cancel", "restock", "flash_sale", "mega_sale"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        sim.set_speed(cfg["clock"]["speed"])
    except Exception as exc:  # the API may still be starting; the Control Center can set it later
        sim.logs.emit(logging.makeLogRecord({"msg": f"[clock] could not set speed: {exc}", "levelname": "WARNING"}))
    if cfg["seed"]["auto"]:
        threading.Thread(target=sim.seed, name="seed", daemon=True).start()
    yield
    sim.stop()


app = FastAPI(title="Traffic simulator", lifespan=lifespan)


class TickConfig(BaseModel):
    interval: float | None = Field(None, ge=0.5, le=60)
    # names kept for the Control Center: buyer sessions and new customers per business hour (engine caps the same)
    orders_per_tick: int | None = Field(None, ge=0, le=5000)
    new_customers_per_tick: int | None = Field(None, ge=0, le=1000)


class ActionRequest(BaseModel):
    count: int = Field(1, ge=1, le=50)


class ClockSpeed(BaseModel):
    speed: float = Field(gt=0, le=100_000)


class SeedRequest(BaseModel):
    preset: str | None = None


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/status")
def status(log_since: int = 0):
    return {**sim.status(), "logs": sim.logs.since(log_since)[-100:]}


@app.post("/start")
def start(tick: TickConfig | None = None):
    if tick:
        sim.update_config(**tick.model_dump())
    sim.start()
    return sim.status()


@app.post("/stop")
def stop():
    sim.stop()
    return sim.status()


@app.put("/config")
def config(tick: TickConfig):
    sim.update_config(**tick.model_dump())
    return sim.status()


@app.post("/actions/{name}")
def action(name: str, req: ActionRequest | None = None):
    if name not in ACTIONS:
        raise HTTPException(404, f"Unknown action '{name}'")
    sim.run_action(name, (req or ActionRequest()).count)
    return {"ok": True}


@app.post("/seed")
def seed(req: SeedRequest | None = None):
    preset = (req or SeedRequest()).preset
    if preset and preset not in cfg["scale"]["presets"]:
        raise HTTPException(422, f"Unknown preset '{preset}'")
    if sim.seeding["running"]:
        raise HTTPException(409, "Seeding already in progress")
    threading.Thread(target=sim.seed, args=(preset,), name="seed", daemon=True).start()
    return sim.status()


@app.put("/clock")
def clock(body: ClockSpeed):
    return sim.set_speed(body.speed)

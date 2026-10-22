"""Marketplace traffic simulation on business time. Every change goes through the Source API.

Each tick reads the business clock, measures the business time that elapsed (Δt) and lets every actor act
with probability p = 1 − e^(−rate·Δt) per candidate. Buyer arrivals are Poisson with an intensity that follows
the hour of day, weekends and running campaigns. Behaviours live in actors.py; this module holds control,
caches, seeding and the tick schedule.
"""

import itertools
import logging
import math
import random
import threading
import time
import uuid
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from . import catalog, vn
from .actors import Actors
from .client import ApiError, SourceApi
from .util import VN_TZ, chance, pick, short

log = logging.getLogger("simulator")
log.setLevel(logging.INFO)


class LogBuffer(logging.Handler):
    """Recent log lines for the Control Center activity feed."""

    def __init__(self, maxlen: int = 300):
        super().__init__()
        self.records = deque(maxlen=maxlen)
        self.seq = 0
        # Not `self.lock`: logging.Handler already holds that one while calling emit()
        self.buffer_lock = threading.Lock()

    def emit(self, record):
        with self.buffer_lock:
            self.seq += 1
            self.records.append({
                "seq": self.seq,
                "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
                "level": record.levelname,
                "message": record.getMessage(),
            })

    def since(self, seq: int) -> list:
        with self.buffer_lock:
            return [r for r in self.records if r["seq"] > seq]


class Simulator(Actors):
    def __init__(self, cfg: dict, api: SourceApi):
        self.cfg = cfg
        self.api = api
        self.tick_cfg = {
            "interval": float(cfg["tick"]["interval_s"]),
            "orders_per_tick": int(cfg["tick"]["sessions_per_hour"]),        # API name kept for the Control Center
            "new_customers_per_tick": int(cfg["tick"]["new_customers_per_hour"]),
        }
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = None
        self.ticks = 0
        self.errors = 0
        self.last_error = None
        self.started_at = None
        self.last_tick_at = None
        self.seeding = {"running": False, "phase": None, "done": 0, "target": 0, "preset": cfg["scale"]["preset"]}
        self.metrics = Counter()
        self.pool = ThreadPoolExecutor(max_workers=cfg["traffic"]["max_parallel_calls"])
        self.seq = itertools.count()
        # caches (refreshed periodically)
        self.shops, self.customers, self.customer_weights = {}, [], []
        self.skus, self.sku_weights = [], []
        self.vouchers, self.flash_skus = [], set()
        # actor state
        self.payments = []          # heap (due business epoch, seq, job): the mock gateway's pending callbacks
        self.late_scans = []        # heap (due business epoch, seq, event): stale carrier scans to deliver later
        self.reviewed = set()
        self.campaign_windows = []  # (start, end, kind) business datetimes of campaigns this simulator created
        self.ops_day = None
        self.business_now = None
        self.remeasure = False
        self.accounts = {}          # customer_id -> email of the buyers this simulator signed up (password logins)
        self.clock_speed = cfg["clock"]["speed"]
        self.logs = LogBuffer()
        log.addHandler(self.logs)

    # ------------------------------------------------------------------ helpers shared with actors

    def count(self, name: str, n: int = 1):
        with self.lock:
            self.metrics[name] += n

    def try_call(self, fn, *args):
        """Business rejections (4xx) are expected traffic, not simulator errors."""
        try:
            return fn(*args)
        except ApiError as e:
            if e.status >= 500:
                self.count("errors.api_5xx")
                log.error(f"[error] {e}")
                return None
            self.count(f"rejected.{e.code}")
            if e.code not in ("invalid_transition", "invalid_shipment_event"):   # races between actors are normal
                log.warning(f"[rejected] {e.code}: {short(e.detail)}")
        except Exception as e:  # network trouble after client retries
            self.count("errors.other")
            log.error(f"[error] {type(e).__name__}: {e}")

    def parallel(self, tasks: list):
        """Run (fn, *args) tasks concurrently and wait — one tick's worth of independent API calls."""
        list(self.pool.map(lambda t: self.try_call(*t), tasks))

    def local_hour(self, t: datetime) -> int:
        return t.astimezone(VN_TZ).hour

    # ------------------------------------------------------------------ control

    @property
    def running(self) -> bool:
        return self.thread is not None and self.thread.is_alive()

    def update_config(self, interval=None, orders_per_tick=None, new_customers_per_tick=None):
        with self.lock:
            if interval is not None:
                self.tick_cfg["interval"] = max(0.5, min(float(interval), 60.0))
            if orders_per_tick is not None:
                self.tick_cfg["orders_per_tick"] = max(0, min(int(orders_per_tick), 5000))
            if new_customers_per_tick is not None:
                self.tick_cfg["new_customers_per_tick"] = max(0, min(int(new_customers_per_tick), 1000))

    def set_speed(self, speed: float) -> dict:
        body = self.api.command("/admin/clock", {"speed": speed}, method="PUT")
        self.clock_speed = speed
        self.remeasure = True             # next tick takes Δt from the interval; business_now stays set, a
                                          # tick running in the simulation thread may still read it
        log.info(f"[clock] business time now runs at ×{speed:g}")
        return body

    def start(self):
        if self.running:
            return
        self.stop_event.clear()
        self.started_at = time.time()
        self.business_now = None
        self.thread = threading.Thread(target=self._loop, name="simulation", daemon=True)
        self.thread.start()
        log.info("Simulation started")

    def stop(self):
        if not self.running:
            return
        self.stop_event.set()
        self.thread.join(timeout=60)
        log.info("Simulation stopped")

    def _loop(self):
        while not self.stop_event.is_set():
            with self.lock:
                cfg = dict(self.tick_cfg)
            started = time.time()
            try:
                self.tick(cfg)
                self.ticks += 1
                self.last_tick_at = time.time()
            except Exception as exc:
                self.errors += 1
                self.last_error = str(exc)
                log.error(f"Tick failed: {exc}")
            self.stop_event.wait(max(0.0, cfg["interval"] - (time.time() - started)))

    def status(self) -> dict:
        with self.lock:
            cfg = dict(self.tick_cfg)
            metrics = dict(self.metrics)
        now = self.business_now
        return {
            "running": self.running,
            "config": cfg,
            "ticks": self.ticks,
            "errors": self.errors,
            "last_error": self.last_error,
            "started_at": self.started_at,
            "last_tick_at": self.last_tick_at,
            "seeding": dict(self.seeding),
            "api": dict(self.api.counters),
            "metrics": metrics,
            "clock": {"business_now": now.isoformat() if now else None, "speed": self.clock_speed},
            "campaign": self.active_campaign(now) if now else None,
            "pending_payment_callbacks": len(self.payments),
        }

    # ------------------------------------------------------------------ seeding

    def _phase(self, name: str, target: int):
        self.seeding.update(phase=name, done=0, target=target)

    def _done(self, n: int = 1):
        with self.lock:
            self.seeding["done"] += n

    def seed(self, preset: str | None = None):
        """Catalog, shops (listings, shop vouchers) and customers through the API up to the preset's size."""
        preset = preset or self.cfg["scale"]["preset"]
        size = self.cfg["scale"]["presets"][preset]
        self.seeding = {"running": True, "phase": "catalog", "done": 0, "target": 0, "preset": preset}
        try:
            self._seed_catalog()
            self._seed_shops(size["shops"], size["skus"])
            self._seed_customers(size["customers"])
            log.info(f"[seed] preset={preset} ready: {self.api.query('/stats')}")
        except Exception as exc:
            log.error(f"[seed] failed: {exc}")
        finally:
            self.seeding["running"] = False
            self.seeding["phase"] = "done"

    def _seed_catalog(self):
        self._phase("catalog", len(catalog.ROOTS) + len(catalog.LEAVES) + len(catalog.brands()))
        for root_slug, root_name, leaves in catalog.ROOTS:
            self.api.command("/admin/categories", {"slug": root_slug, "name": root_name})
            self._done()
            for leaf in leaves:
                self.api.command("/admin/categories", {"slug": leaf.slug, "name": leaf.name, "parent_slug": root_slug})
                self._done()
        for brand in catalog.brands():
            self.api.command("/admin/brands", {"name": brand})
            self._done()

    def _seed_shops(self, n_shops: int, n_skus: int):
        existing = self.api.query("/stats")["shops"]
        missing = max(0, n_shops - existing)
        skus_per_shop = math.ceil(n_skus / n_shops)
        self._phase("shops", missing)
        log.info(f"[seed] {existing} shops exist, onboarding {missing} with ~{skus_per_shop} SKUs each")

        def onboard(i):
            tier = pick(self.cfg["scale"]["shop_tiers"])
            shop = None
            for _ in range(5):
                name = vn.shop_name(existing + i + 1)
                try:
                    shop = self.api.tokens.register("/v1/auth/register/seller", {     # sign-up through identity
                        "legal_name": f"Công ty TNHH {name}", "email": f"seller.{uuid.uuid4().hex[:10]}@shop.vn",
                        "shop": {"name": name, "tier": tier, "province": vn.shop_province()},
                    })["shop"]
                    break
                except ApiError as e:
                    if e.code != "seller_exists":
                        raise
            roots = random.sample(catalog.ROOTS, random.choice([1, 1, 2]))
            leaves = [leaf for _, _, ls in roots for leaf in ls]
            listed = 0
            while listed < skus_per_shop:
                product = catalog.make_product(random.choice(leaves), f"S{uuid.uuid4().hex[:10].upper()}")
                listed += len(self.api.command(f"/shops/{shop['id']}/products", product, as_=f"shop:{shop['id']}")["skus"])
            if chance(self.cfg["scale"]["shop_voucher_ratio"]):
                value = random.choice([10_000, 20_000, 30_000, 50_000])
                self.api.command(f"/shops/{shop['id']}/vouchers", {
                    "code": f"SHOP{uuid.uuid4().hex[:8].upper()}", "kind": "FIXED", "value": value,
                    "min_spend": value * random.choice([8, 10, 15]), "usage_limit": random.choice([100, 200, 500]),
                    "duration_s": 60 * 86400}, as_=f"shop:{shop['id']}")
            self._done()

        with ThreadPoolExecutor(max_workers=self.cfg["seed"]["concurrency"]) as pool:
            list(pool.map(onboard, range(missing)))
        self.refresh_shops()

    def _seed_customers(self, target: int):
        missing = max(0, target - self.api.query("/stats")["customers"])
        self._phase("customers", missing)

        def one(_):
            for _attempt in range(5):
                try:
                    self.register_customer(quiet=True)
                    break
                except ApiError as e:
                    if e.code != "email_taken":
                        raise
            self._done()

        with ThreadPoolExecutor(max_workers=self.cfg["seed"]["concurrency"]) as pool:
            list(pool.map(one, range(missing)))

    # ------------------------------------------------------------------ caches

    def refresh_shops(self):
        ratio = self.cfg["scale"]["lazy_shop_ratio"]
        self.shops = {
            s["id"]: {**s, "lazy": s["shop_tier"] == "NORMAL" and int(s["id"].replace("-", ""), 16) % 1000 < ratio * 1000}
            for s in self.api.query("/shops", limit=10000)
        }

    def refresh_customers(self):
        rows = self.api.query("/customers", active=True, random=True, limit=5000)
        activity = self.cfg["buyer"]["segment_activity"]
        self.customers = rows
        self.customer_weights = [activity.get(c["segment"], 1.0) for c in rows]

    def refresh_skus(self):
        """All sellable SKUs with Zipf popularity weights by a stable rank (hash of the SKU id)."""
        rows = self.api.query("/skus", available=True, limit=20000)
        ranked = sorted(rows, key=lambda r: uuid.UUID(r["id"]).int % 1_000_003)
        s = self.cfg["traffic"]["popularity_zipf_s"]
        self.skus = ranked
        self.sku_weights = [1 / (rank + 1) ** s for rank in range(len(ranked))]

    def refresh_promotions(self):
        self.vouchers = self.api.query("/vouchers", active=True)
        flash = set()
        for c in self.api.query("/campaigns", active=True):
            flash.update(i["sku_id"] for i in self.api.query(f"/campaigns/{c['id']}/items") if i["sold"] < i["quota"])
        self.flash_skus = flash

    # ------------------------------------------------------------------ tick

    def tick(self, cfg: dict):
        clk = self.api.query("/clock")
        now = datetime.fromisoformat(clk["now"])
        self.clock_speed = clk["speed"]
        dt_h = cfg["interval"] * self.clock_speed / 3600 if self.business_now is None or self.remeasure \
            else (now - self.business_now).total_seconds() / 3600
        self.remeasure = False
        dt_h = max(0.0, min(dt_h, 6.0))      # a long pause must not unleash a burst of actions
        self.business_now = now

        if not self.shops:
            self.refresh_shops()
        if not self.customers or self.ticks % 30 == 0:
            self.refresh_customers()
        if not self.skus or self.ticks % 10 == 0:
            self.refresh_skus()

        self.ops_daily(now)
        if self.ticks % 5 == 0:
            self.refresh_promotions()

        intensity = self.intensity(now)
        self.buyers_arrive(cfg, dt_h, intensity)
        self.gateway_callbacks(now)
        self.sellers_work(dt_h)
        self.carriers_work(dt_h, now)
        self.buyers_after_sale(dt_h)
        self.returns_work(dt_h)
        self.ops_work(dt_h)

    def intensity(self, now: datetime) -> float:
        t = self.cfg["traffic"]
        local = now.astimezone(VN_TZ)
        level = t["hourly_profile"][local.hour] * (t["weekend_boost"] if local.weekday() >= 5 else 1.0)
        campaign = self.active_campaign(now)
        if campaign == "MEGA_SALE_SPIKE":
            level *= self.cfg["ops"]["mega_sale"]["midnight_spike"]
        elif campaign == "MEGA_SALE":
            level = max(level, 1.0) * self.cfg["ops"]["mega_sale"]["boost"]
        elif campaign == "FLASH_SALE":
            level *= self.cfg["ops"]["flash_boost"]
        return level

    def active_campaign(self, now: datetime | None) -> str | None:
        if now is None:
            return None
        kinds = [(start, kind) for start, end, kind in self.campaign_windows if start <= now < end]
        mega = [start for start, kind in kinds if kind == "MEGA_SALE"]
        if mega:
            return "MEGA_SALE_SPIKE" if now < mega[0] + timedelta(hours=1) else "MEGA_SALE"
        return "FLASH_SALE" if kinds else None

    def run_action(self, name: str, count: int = 1):
        if not self.shops:
            self.refresh_shops()
        if not self.customers:
            self.refresh_customers()
        if not self.skus:
            self.refresh_skus()
        now = datetime.fromisoformat(self.api.query("/clock")["now"])
        self.business_now = self.business_now or now
        actions = {
            "customer":   lambda: self.register_customer(),
            "order":      lambda: self.buyer_session(force_checkout=True),
            "cart":       lambda: self.buyer_session(force_checkout=False),
            "advance":    lambda: self.fast_forward(now),
            "cancel":     lambda: self.buyer_cancel_one("TO_SHIP"),
            "restock":    lambda: self.restock_low(force=True),
            "flash_sale": lambda: self.launch_flash_sale(now, starts_in_s=0),
            "mega_sale":  lambda: self.launch_mega_sale(now),
        }
        for _ in range(count if name not in ("flash_sale", "mega_sale") else 1):
            self.try_call(actions[name])

    def fast_forward(self, now: datetime):
        """'Pay · pack · ship' button: act as if a business day passed for gateway, sellers and carriers."""
        self.gateway_callbacks(now + timedelta(days=1))
        self.sellers_work(24)
        self.carriers_work(24, now)


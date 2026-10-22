"""Behaviour of the simulated actors. Mixed into engine.Simulator, which provides api, cfg, caches and helpers.

Rates come from config.yaml and are per business hour; `dt_h` is the business time since the last tick.
"""

import heapq
import logging
import random
import uuid
from datetime import datetime, timedelta

from . import vn
from .client import ApiError
from .util import VN_TZ, chance, p_event, pick, poisson, vnd

log = logging.getLogger("simulator")


class Actors:
    # ================================================================== buyers

    def register_customer(self, quiet: bool = False):
        name = vn.full_name()
        profile = {"full_name": name, "email": vn.email_for(name), "phone": vn.phone(),
                   "segment": pick(self.cfg["buyer"]["segment_mix"]),
                   "registered_channel": "APP" if chance(0.75) else "WEB", "address": vn.address(name)}
        c = self.api.tokens.register("/v1/auth/register", profile)["customer"]     # sign-up through identity
        with self.lock:
            self.accounts[c["id"]] = profile["email"]
        self.count("customers.registered")
        if not quiet:
            log.info(f"[customer.registered] {name} · {c['segment']} · {c['default_address']['province']}")
        return c

    def buyers_arrive(self, cfg: dict, dt_h: float, intensity: float):
        hour_level = self.cfg["traffic"]["hourly_profile"][self.local_hour(self.business_now)]
        tasks = [(self.register_customer,) for _ in range(poisson(cfg["new_customers_per_tick"] * hour_level * dt_h))]
        tasks += [(self.buyer_session,) for _ in range(poisson(cfg["orders_per_tick"] * intensity * dt_h))]
        returning = poisson(self.cfg["buyer"]["return_to_cart_per_hour"] * intensity * dt_h)
        if returning:
            tasks += [(self.checkout, cart["customer_id"], "REGULAR")
                      for cart in self.api.query("/carts", status="ACTIVE", random=True, limit=returning)]
        self.count("sessions", len(tasks))
        self.parallel(tasks)

    def buyer_session(self, force_checkout: bool | None = None):
        b = self.cfg["buyer"]
        if not self.skus or not self.customers:
            return
        customer = random.choices(self.customers, weights=self.customer_weights)[0]
        me = f"customer:{customer['id']}"
        self.sign_in(customer["id"], me)
        seg = customer["segment"]
        boost = 4 if seg == "BARGAIN_HUNTER" else 2
        weights = self.sku_weights if not self.flash_skus else \
            [w * (boost if s["id"] in self.flash_skus else 1) for s, w in zip(self.skus, self.sku_weights)]
        picks = {s["id"]: s for s in random.choices(self.skus, weights=weights, k=random.randint(*b["items_per_session"]))}
        basket = {}                                       # shop_id -> merchandise value this session
        for sku in picks.values():
            qty = random.randint(1, 5) if seg == "BUSINESS" else random.choices([1, 2, 3], weights=b["quantity_weights"])[0]
            self.api.command("/carts/items", {"sku_id": sku["id"], "quantity": qty}, as_=me)
            basket[sku["shop_id"]] = basket.get(sku["shop_id"], 0) + sku["price"] * qty
            self.count("cart.items_added")

        if chance(b["edit_cart_ratio"]):
            items = self.api.query("/carts/active", as_=me)["items"]
            if len(items) > 1 and chance(0.5):
                self.api.command(f"/carts/items/{random.choice(items)['id']}", method="DELETE", as_=me)
                self.count("cart.items_removed")
            elif items:
                self.api.command(f"/carts/items/{random.choice(items)['id']}", {"quantity": random.randint(1, 3)},
                                 method="PATCH", as_=me)

        ratio = b["checkout_ratio"][seg] * (1.4 if self.flash_skus and seg == "BARGAIN_HUNTER" else 1)
        go = chance(min(ratio, 0.95)) if force_checkout is None else force_checkout
        if go:
            self.checkout(customer["id"], seg, basket)
        else:
            self.count("cart.left")

    def choose_vouchers(self, seg: str, basket: dict) -> list:
        """Like the app's voucher picker: only vouchers the basket qualifies for are offered
        (shop vouchers against that shop's goods, platform vouchers against the whole basket)."""
        if not self.vouchers or not chance(self.cfg["buyer"]["voucher_use"][seg]):
            return []
        total = sum(basket.values())
        eligible = [v for v in self.vouchers if v["scope"] == "PLATFORM" and v["min_spend"] <= total * 0.9]
        platform = [v for v in eligible if v["kind"] != "FREESHIP"]
        freeship = [v for v in eligible if v["kind"] == "FREESHIP"]
        codes = [random.choice(platform)["code"]] if platform else []
        if freeship and chance(0.8):
            codes.append(random.choice(freeship)["code"])
        by_shop = {}
        for v in self.vouchers:
            if v["scope"] == "SHOP" and v["min_spend"] <= basket.get(v["shop_id"], 0):
                by_shop.setdefault(v["shop_id"], v["code"])
        return codes + list(by_shop.values())

    def sign_in(self, customer_id: str, me: str):
        """Some sessions start with a real password login (customers this simulator signed up); the rest use an
        exchanged token. A few mistype their password, which the identity service counts toward a lockout."""
        a = self.cfg["auth"]
        email = self.accounts.get(customer_id)
        if email and chance(a["login_ratio"]):
            if chance(a["wrong_password_ratio"]):
                self.api.tokens.login(me, email, password="mistyped-password")
                self.count("auth.wrong_password")
            if self.api.tokens.login(me, email):
                self.count("auth.logins")

    def checkout(self, customer_id: str, seg: str = "REGULAR", basket: dict | None = None):
        me = f"customer:{customer_id}"
        method = pick(self.cfg["buyer"]["payment_methods"])
        codes = self.choose_vouchers(seg, basket or {})
        res = None
        for attempt in range(4):
            try:
                res = self.api.command("/checkouts", {"payment_method": method, "voucher_codes": codes}, as_=me)
                break
            except ApiError as e:
                if e.code == "voucher_invalid" and e.detail.get("voucher") in codes:
                    codes.remove(e.detail["voucher"])           # buyer drops the voucher the app rejected
                    self.count(f"voucher.rejected.{e.detail.get('why')}")
                    continue
                if e.code == "out_of_stock" and attempt < 3:
                    short = {i["sku_id"] for i in e.detail["items"]}
                    items = self.api.query("/carts/active", as_=me)["items"]
                    for item in items:
                        if item["sku_id"] in short:
                            self.api.command(f"/carts/items/{item['id']}", method="DELETE", as_=me)
                    if len(short) >= len(items):
                        self.count("checkout.gave_up")
                        return
                    continue
                if e.code == "flash_sold_out" and attempt < 3:
                    self.count("checkout.flash_sold_out")              # pays the regular price on the retry
                    continue
                if e.code == "cart_empty":
                    return
                raise
        if res is None:
            return
        co = res["checkout"]
        self.count("checkouts")
        self.count("orders.placed", len(res["orders"]))
        if co["discount_total"]:
            self.count("checkouts.with_voucher")
        log.info(f"[checkout] {len(res['orders'])} order(s) · {vnd(co['grand_total'])} · {method}"
                 + (f" · −{vnd(co['discount_total'])}" if co["discount_total"] else ""))
        if res["payment"]:
            self.schedule_payment(res["payment"]["transaction_ref"], co["id"], res["payment"]["provider"], customer_id)

    def buyers_after_sale(self, dt_h: float):
        b = self.cfg["buyer"]
        tasks = []
        for o in self.api.query("/orders", status="DELIVERED", random=True, limit=3000):
            if chance(p_event(b["return_request_rate"], dt_h)):
                tasks.append((self.request_return, o))
            elif chance(p_event(b["confirm_received_rate"], dt_h)):
                tasks.append((self._confirm, o))
        seen = set()
        for o in self.api.query("/orders", status="UNPAID", random=True, limit=2000):
            if o["checkout_id"] not in seen and chance(p_event(b["cancel_unpaid_rate"], dt_h)):
                seen.add(o["checkout_id"])
                tasks.append((self._cancel, o, "UNPAID"))
        for o in self.api.query("/orders", status="COMPLETED", random=True, limit=3000):
            if o["id"] not in self.reviewed and chance(p_event(b["review_rate"], dt_h)):
                self.reviewed.add(o["id"])
                tasks.append((self.review, o))
        self.parallel(tasks)

    def _confirm(self, o):
        self.api.command(f"/orders/{o['id']}/confirm-received", {}, as_=f"customer:{o['customer_id']}")
        self.count("orders.confirmed")

    def _cancel(self, o, status: str):
        self.api.command(f"/orders/{o['id']}/cancel", {"reason": random.choice(
            ["changed_mind", "found_cheaper", "wrong_address", "ordered_by_mistake"])}, as_=f"customer:{o['customer_id']}")
        self.count(f"orders.buyer_cancelled.{status.lower()}")
        log.info(f"[order.cancelled] {o['order_number']} by buyer ({status})")

    def buyer_cancel_one(self, status: str):
        for o in self.api.query("/orders", status=status, random=True, limit=1):
            self._cancel(o, status)

    def request_return(self, o):
        b = self.cfg["buyer"]
        items = self.api.query(f"/orders/{o['id']}")["items"]
        reason = pick(b["return_reasons"])
        if chance(b["partial_return_ratio"]) and (len(items) > 1 or items[0]["quantity"] > 1):
            line = random.choice(items)
            lines = [{"order_item_id": line["id"], "quantity": random.randint(1, line["quantity"])}]
        else:
            lines = [{"order_item_id": i["id"], "quantity": i["quantity"]} for i in items]
        self.api.command(f"/orders/{o['id']}/returns", {"reason": reason, "items": lines},
                         as_=f"customer:{o['customer_id']}")
        self.count(f"returns.requested.{reason}")
        log.info(f"[return.requested] {o['order_number']} · {reason} · {sum(l['quantity'] for l in lines)} unit(s)")

    def review(self, o):
        items = self.api.query(f"/orders/{o['id']}")["items"]
        shop = self.shops.get(o["shop_id"], {})
        weights = [15, 15, 25, 25, 20] if shop.get("lazy") else self.cfg["buyer"]["ratings"]
        reviews = [{"order_item_id": i["id"], "rating": random.choices([1, 2, 3, 4, 5], weights=weights)[0],
                    "comment": random.choice([None, "Giao nhanh, đóng gói kỹ", "Hàng đúng mô tả", "Tạm ổn",
                                              "Shop tư vấn nhiệt tình", "Giao hơi chậm"])} for i in items]
        self.api.command(f"/orders/{o['id']}/reviews", {"items": reviews}, as_=f"customer:{o['customer_id']}")
        self.count("reviews.posted", len(reviews))

    # ================================================================== payment gateway (mock)

    def schedule_payment(self, ref: str, checkout_id: str, provider: str, customer_id: str, attempt: int = 1):
        g = self.cfg["payment_gateway"]
        if chance(g["never_pay_rate"]):
            self.count("payments.never_paid")
            return
        now = (self.business_now or datetime.now().astimezone()).timestamp()
        if chance(g["late_callback_rate"]):
            due, outcome = now + g["late_delay_s"], "SUCCEEDED"
        else:
            due = now + random.uniform(*g["delay_s"])
            outcome = "SUCCEEDED" if chance(g["success_rate"]) else "FAILED"
        with self.lock:
            heapq.heappush(self.payments, (due, next(self.seq), {"ref": ref, "outcome": outcome, "provider": provider,
                                                                  "checkout_id": checkout_id, "attempt": attempt,
                                                                  "customer_id": customer_id}))

    def gateway_callbacks(self, now: datetime):
        due = []
        with self.lock:
            while self.payments and self.payments[0][0] <= now.timestamp():
                due.append(heapq.heappop(self.payments)[2])
        self.parallel([(self._deliver_callback, job) for job in due])

    def _deliver_callback(self, job):
        g = self.cfg["payment_gateway"]
        body = {"transaction_ref": job["ref"], "status": job["outcome"],
                "failure_reason": None if job["outcome"] == "SUCCEEDED" else random.choice(
                    ["insufficient_funds", "card_declined", "otp_timeout", "bank_maintenance"])}
        event_id = f"gw-{uuid.uuid4()}"
        res = self.api.command("/payments/webhook", body, key=event_id, signed=job["provider"])
        if chance(g["duplicate_callback_rate"]):
            # providers redeliver: sometimes the same event id, sometimes a new one
            self.api.command("/payments/webhook", body, key=event_id if chance(0.5) else f"gw-{uuid.uuid4()}",
                             signed=job["provider"])
            self.count("payments.duplicate_callbacks")
        self.count(f"payments.{res['result']}")
        if res["result"] != "paid":
            log.info(f"[payment.{res['result']}] {job['ref'][:16]}… attempt {job['attempt']}")
        if job["outcome"] == "FAILED" and chance(g["retry_after_failure"]):
            retry = self.api.command(f"/checkouts/{job['checkout_id']}/payments", as_=f"customer:{job['customer_id']}")
            self.schedule_payment(retry["transaction_ref"], job["checkout_id"], retry["provider"], job["customer_id"],
                                  job["attempt"] + 1)

    # ================================================================== sellers

    def sellers_work(self, dt_h: float):
        s, b = self.cfg["seller"], self.cfg["buyer"]
        tasks = []
        for o in self.api.query("/orders", status="TO_SHIP", random=True, limit=3000):
            shop = self.shops.get(o["shop_id"]) or {"shop_tier": "NORMAL", "lazy": False}
            rate = s["pack_rate"]["LAZY" if shop["lazy"] else shop["shop_tier"]]
            if chance(p_event(s["out_of_stock_cancel_rate"], dt_h)):
                tasks.append((self._seller_cancel, o))
            elif chance(p_event(b["cancel_to_ship_rate"], dt_h)):
                tasks.append((self._cancel, o, "TO_SHIP"))
            elif chance(p_event(rate, dt_h)):
                tasks.append((self._pack, o))
        for _ in range(poisson(len(self.skus) * s["price_change_rate"] * dt_h)):
            tasks.append((self._change_price, random.choice(self.skus)))
        self.parallel(tasks)
        self.restock_low(dt_h)

    def _pack(self, o):
        self.api.command(f"/orders/{o['id']}/pack", {}, as_=f"shop:{o['shop_id']}")
        self.count("orders.packed")

    def _seller_cancel(self, o):
        self.api.command(f"/orders/{o['id']}/seller-cancel", {"reason": "out_of_stock"}, as_=f"shop:{o['shop_id']}")
        self.count("orders.seller_cancelled")
        log.info(f"[order.cancelled] {o['order_number']} by seller (out of stock)")

    def _change_price(self, sku):
        new = max(1_000, round(sku["price"] * random.uniform(0.85, 1.15) / 1_000) * 1_000)
        self.api.command(f"/skus/{sku['id']}/price", {"price": new, "reason": "repricing"}, method="PATCH",
                         as_=f"shop:{sku['shop_id']}")
        sku["price"] = new
        self.count("prices.changed")

    def restock_low(self, dt_h: float = 0, force: bool = False):
        s = self.cfg["seller"]
        tasks = [(self._restock, row) for row in self.api.query("/inventory/low-stock", limit=500)
                 if force or chance(p_event(s["restock_rate"], dt_h))]
        self.parallel(tasks)

    def _restock(self, row):
        qty = random.randint(*self.cfg["seller"]["restock_units"])
        self.api.command(f"/skus/{row['sku_id']}/restock", {"quantity": qty}, as_=f"shop:{row['shop_id']}")
        self.count("inventory.restocks")

    def returns_work(self, dt_h: float):
        s, b = self.cfg["seller"], self.cfg["buyer"]
        tasks = []
        for r in self.api.query("/returns", status="REQUESTED", limit=2000):
            if chance(p_event(s["return_response_rate"], dt_h)):
                tasks.append((self._answer_return, r, chance(s["return_approve_ratio"])))
        for r in self.api.query("/returns", status="RECEIVED", limit=2000):
            if chance(p_event(s["inspect_rate"], dt_h)):
                tasks.append((self._inspect, r))
        for r in self.api.query("/returns", status="REJECTED", limit=2000):
            if chance(p_event(b["dispute_rate"], dt_h)):
                tasks.append((self._dispute, r))
        self.parallel(tasks)

    def _answer_return(self, r, approve: bool):
        self.api.command(f"/returns/{r['id']}/{'approve' if approve else 'reject'}", {}, as_=f"shop:{r['shop_id']}")
        self.count(f"returns.{'approved' if approve else 'rejected'}")

    def _inspect(self, r):
        items = self.api.query(f"/returns/{r['id']}")["items"]
        sellable = self.cfg["seller"]["sellable_ratio"]
        conditions = {i["order_item_id"]: "SELLABLE" if chance(sellable) else "DAMAGED" for i in items}
        res = self.api.command(f"/returns/{r['id']}/inspect", {"conditions": conditions}, as_=f"shop:{r['shop_id']}")
        self.count("returns.refunded")
        log.info(f"[return.refunded] {vnd(res['refund_amount'])} · {r['reason']}")

    def _dispute(self, r):
        self.api.command(f"/returns/{r['id']}/dispute", {}, as_=f"customer:{r['customer_id']}")
        self.count("returns.disputed")

    # ================================================================== carriers (mock)

    def carriers_work(self, dt_h: float, now: datetime):
        c = self.cfg["carrier"]
        late = []
        with self.lock:
            while self.late_scans and self.late_scans[0][0] <= now.timestamp():
                late.append(heapq.heappop(self.late_scans)[2])
        tasks = [(self._send_scan, ev) for ev in late]

        def candidates(direction, status):
            return self.api.query("/shipments", direction=direction, status=status, random=True, limit=3000)

        def transit_rate(s):
            return 1 / c["transit_hours"][vn.route(s["from_province"], s["to_province"])]

        for s in candidates("FORWARD", "CREATED"):
            if chance(p_event(c["pickup_rate"], dt_h)):
                tasks.append((self._track, s, "PICKED_UP", now, dt_h))
        for s in candidates("FORWARD", "PICKED_UP"):
            if chance(p_event(0.5, dt_h)):
                tasks.append((self._track, s, "IN_TRANSIT", now, dt_h))
        for s in candidates("FORWARD", "IN_TRANSIT"):
            if chance(p_event(transit_rate(s), dt_h)):
                tasks.append((self._track, s, "OUT_FOR_DELIVERY", now, dt_h))
        for s in candidates("FORWARD", "OUT_FOR_DELIVERY"):
            if chance(p_event(c["out_for_delivery_rate"], dt_h)):
                event = "DELIVERY_FAILED" if chance(c["delivery_failure_ratio"]) else "DELIVERED"
                tasks.append((self._track, s, event, now, dt_h))
        for s in candidates("FORWARD", "DELIVERY_FAILED"):
            if s["attempts"] >= 3:
                if chance(p_event(c["return_to_sender_rate"], dt_h)):
                    tasks.append((self._track, s, "RETURNED_TO_SENDER", now, dt_h))
            elif chance(p_event(c["redelivery_rate"], dt_h)):
                tasks.append((self._track, s, "OUT_FOR_DELIVERY", now, dt_h))
        for s in candidates("RETURN", "CREATED"):
            if chance(p_event(c["pickup_rate"], dt_h)):
                tasks.append((self._track, s, "PICKED_UP", now, dt_h))
        for s in candidates("RETURN", "PICKED_UP"):
            if chance(p_event(0.5, dt_h)):
                tasks.append((self._track, s, "IN_TRANSIT", now, dt_h))
        for s in candidates("RETURN", "IN_TRANSIT"):
            if chance(p_event(transit_rate(s), dt_h)):
                tasks.append((self._track, s, "DELIVERED", now, dt_h))
        self.parallel(tasks)

    def _track(self, s, event: str, now: datetime, dt_h: float):
        c = self.cfg["carrier"]
        at = now - timedelta(seconds=random.uniform(0, dt_h * 3600 * 0.9))   # it happened during the last Δt
        if s["last_event_time"]:
            last = datetime.fromisoformat(s["last_event_time"])
            if at <= last:
                at = last + timedelta(seconds=1)
        hub = s["to_province"] if event in ("OUT_FOR_DELIVERY", "DELIVERED", "DELIVERY_FAILED") else s["from_province"]
        ev = {"tracking_number": s["tracking_number"], "event": event, "event_time": at.isoformat(),
              "location": f"Hub {hub}", "carrier_event_id": f"{s['carrier']}-{uuid.uuid4()}", "carrier": s["carrier"]}
        res = self._send_scan(ev)
        if res and chance(c["duplicate_callback_rate"]):
            self._send_scan(ev)                      # same carrier event redelivered
            self.count("carrier.duplicate_callbacks")
        if res and event in ("OUT_FOR_DELIVERY", "DELIVERED") and chance(c["late_scan_rate"]):
            # an older hub scan reaches us after this newer event: must be stored but not applied
            stale = {**ev, "event": "IN_TRANSIT", "carrier_event_id": f"{s['carrier']}-{uuid.uuid4()}",
                     "event_time": (at - timedelta(minutes=random.uniform(10, 120))).isoformat(),
                     "location": f"Hub {s['from_province']}"}
            with self.lock:
                heapq.heappush(self.late_scans, (now.timestamp() + random.uniform(600, 7200), next(self.seq), stale))
        self.count(f"carrier.{event.lower()}")
        if event in ("DELIVERY_FAILED", "RETURNED_TO_SENDER"):
            log.info(f"[carrier.{event.lower()}] {s['order_number']} · {s['carrier']} {s['tracking_number']}")

    def _send_scan(self, ev):
        body = {k: v for k, v in ev.items() if k != "carrier"}
        res = self.api.command("/logistics/webhook", body, key=ev["carrier_event_id"], signed=ev["carrier"])
        if res["result"] != "applied":
            self.count(f"carrier.{res['result']}")
        return res

    # ================================================================== platform operations

    def ops_work(self, dt_h: float):
        o = self.cfg["ops"]
        tasks = [(self._resolve, r, chance(o["dispute_refund_ratio"]))
                 for r in self.api.query("/returns", status="DISPUTED", limit=2000)
                 if chance(p_event(o["dispute_resolve_rate"], dt_h))]
        self.parallel(tasks)
        self.campaign_windows = [w for w in self.campaign_windows if w[1] > self.business_now - timedelta(days=2)]

    def _resolve(self, r, refund: bool):
        self.api.command(f"/admin/returns/{r['id']}/resolve", {"decision": "REFUND" if refund else "REJECT"})
        self.count(f"returns.dispute_{'refund' if refund else 'reject'}")

    def ops_daily(self, now: datetime):
        """At each business day: issue the day's platform vouchers and schedule its flash-sale slots."""
        local = now.astimezone(VN_TZ)
        if self.ops_day == local.date():
            return
        self.ops_day = local.date()
        o = self.cfg["ops"]
        midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
        remaining = int((midnight + timedelta(days=1) - local).total_seconds())
        for v in o["daily_vouchers"]:
            body = {k: v[k] for k in v if k != "prefix"}
            self.try_call(self.api.command, "/admin/vouchers",
                          {**body, "code": f"{v['prefix']}{local:%y%m%d}", "duration_s": max(remaining, 3600)})
        for hour in o["flash_slots"]:
            start = midnight + timedelta(hours=hour)
            end = start + timedelta(hours=o["flash_hours"])
            if end > local:
                self.try_call(self.launch_flash_sale, now, max(0, int((start - local).total_seconds())),
                              int((end - max(start, local)).total_seconds()), f"Flash Sale {start:%d/%m %Hh}")
        log.info(f"[ops] business day {local:%d/%m/%Y}: vouchers issued, flash slots {o['flash_slots']} scheduled")

    def launch_flash_sale(self, now: datetime, starts_in_s: int = 0, duration_s: int | None = None,
                          name: str | None = None, kind: str = "FLASH_SALE", n_skus: int | None = None):
        o = self.cfg["ops"]
        duration_s = duration_s or o["flash_hours"] * 3600
        pool = [s for s in self.skus[:400] if s["available"] >= 20 and s["price"] >= 30_000]
        items = []
        for s in random.sample(pool, min(n_skus or o["flash_skus"], len(pool))):
            sale = round(s["price"] * (1 - random.uniform(*o["flash_discount"])) / 1_000) * 1_000
            if 1_000 <= sale < s["price"]:
                items.append({"sku_id": s["id"], "sale_price": sale,
                              "quota": min(random.randint(*o["flash_quota"]), s["available"])})
        c = self.api.command("/admin/campaigns", {"name": name or f"Flash Sale {now.astimezone(VN_TZ):%d/%m %Hh%M}",
                                                  "kind": kind, "starts_in_s": starts_in_s, "duration_s": duration_s,
                                                  "items": items})
        self.campaign_windows.append((datetime.fromisoformat(c["starts_at"]), datetime.fromisoformat(c["ends_at"]), kind))
        self.count(f"campaigns.{kind.lower()}")
        log.info(f"[campaign] {c['name']} · {len(items)} SKUs · "
                 f"{'live now' if starts_in_s == 0 else f'opens in {starts_in_s // 3600}h{starts_in_s % 3600 // 60:02d}'}")
        return c

    def launch_mega_sale(self, now: datetime):
        m = self.cfg["ops"]["mega_sale"]
        local = now.astimezone(VN_TZ)
        for v in m["vouchers"]:
            body = {k: v[k] for k in v if k != "prefix"}
            self.try_call(self.api.command, "/admin/vouchers",
                          {**body, "code": f"{v['prefix']}{local:%y%m%d%H}", "duration_s": m["hours"] * 3600})
        c = self.launch_flash_sale(now, 0, m["hours"] * 3600, f"Siêu Sale {local:%d.%m}", "MEGA_SALE", m["skus"])
        self.refresh_promotions()
        log.info(f"[campaign] MEGA SALE live: traffic ×{m['boost']} (×{m['midnight_spike']} in the first hour)")
        return c

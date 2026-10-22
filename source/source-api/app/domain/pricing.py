"""Checkout pricing: flash-sale prices, vouchers and their allocation down to order lines.

Funding: shop vouchers are paid by the seller (orders.shop_discount); platform vouchers and free shipping
are paid by the platform (orders.platform_discount, orders.shipping_subsidy). Every order-level discount is
split over its lines (largest remainder), so a partial return can refund exactly what was paid per unit.
"""

import random
from collections import defaultdict

from ..errors import conflict
from ..settings import MIN_ORDER_PAYABLE, VOUCHER_QUOTA_SLOTS


def allocate(total: int, weights: list) -> list:
    """Split an integer amount proportionally to weights; parts sum exactly to `total`."""
    w = sum(weights)
    if total <= 0 or w <= 0:
        return [0] * len(weights)
    raw = [total * x / w for x in weights]
    parts = [int(r) for r in raw]
    for i in sorted(range(len(raw)), key=lambda i: raw[i] - parts[i], reverse=True)[: total - sum(parts)]:
        parts[i] += 1
    return parts


def load_vouchers(cur, codes: list) -> list:
    """Requested vouchers, unlocked: their quota is claimed slot by slot (claim_quota), never on this row."""
    if not codes:
        return []
    cur.execute("SELECT * FROM ecommerce.vouchers WHERE code = ANY(%s)", (sorted(set(codes)),))
    found = {v["code"]: v for v in cur.fetchall()}
    missing = [c for c in codes if c not in found]
    if missing:
        raise conflict("voucher_invalid", f"Unknown voucher {missing[0]}", voucher=missing[0], why="not_found")
    return sorted((found[c] for c in dict.fromkeys(codes)), key=lambda v: str(v["id"]))   # claim order


def lock_customer_vouchers(cur, customer_id) -> None:
    """Serialise one customer's checkouts so the per-customer limit holds (other customers are not blocked)."""
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 7))", (f"voucher-customer:{customer_id}",))


def _slots(usage_limit: int) -> int:
    return min(VOUCHER_QUOTA_SLOTS, usage_limit)


def create_quota(cur, v: dict) -> None:
    """Split usage_limit over the voucher's slots (sizes differ by at most one)."""
    n = _slots(v["usage_limit"])
    for slot, capacity in enumerate(allocate(v["usage_limit"], [1] * n)):
        cur.execute("INSERT INTO ecommerce.voucher_quota_slots (voucher_id, slot, capacity) VALUES (%s, %s, %s)",
                    (v["id"], slot, capacity))


def check_quota(cur, v: dict) -> None:
    """Fail early, without locking, when a voucher has no redemption left; the claim itself comes last."""
    cur.execute("SELECT 1 FROM ecommerce.voucher_quota_slots WHERE voucher_id = %s AND used < capacity LIMIT 1",
                (v["id"],))
    if not cur.fetchone():
        raise conflict("voucher_invalid", f"Voucher {v['code']} has been fully redeemed", voucher=v["code"],
                       why="exhausted")


def claim_quota(cur, v: dict) -> int:
    """Take one redemption from a random slot that still has room. Returns the slot.

    Concurrent checkouts spread over up to VOUCHER_QUOTA_SLOTS rows instead of queueing on one. Each attempt
    waits on at most one row and keeps no lock when it fails (the slot filled up meanwhile), so a checkout never
    holds one slot of a voucher while waiting for another: no lock cycles.
    """
    while True:
        cur.execute("SELECT slot FROM ecommerce.voucher_quota_slots WHERE voucher_id = %s AND used < capacity",
                    (v["id"],))
        free = [r["slot"] for r in cur.fetchall()]
        if not free:
            raise conflict("voucher_invalid", f"Voucher {v['code']} has been fully redeemed", voucher=v["code"],
                           why="exhausted")
        cur.execute("""UPDATE ecommerce.voucher_quota_slots SET used = used + 1
                       WHERE voucher_id = %s AND slot = %s AND used < capacity RETURNING slot""",
                    (v["id"], random.choice(free)))
        row = cur.fetchone()
        if row:
            return row["slot"]


def release_quota(cur, usages) -> None:
    """Give redemptions back; (voucher_id, slot) order, the same order claims are taken in."""
    for voucher_id, slot in sorted(((u["voucher_id"], u["quota_slot"]) for u in usages), key=lambda x: (str(x[0]), x[1])):
        cur.execute("UPDATE ecommerce.voucher_quota_slots SET used = used - 1 WHERE voucher_id = %s AND slot = %s",
                    (voucher_id, slot))


def flash_prices(cur, wanted: dict, t) -> dict:
    """sku_id -> active flash-sale row with enough quota left for the wanted quantity (read only: the quota is
    taken at the end of the checkout with a conditional update)."""
    cur.execute(
        """SELECT f.* FROM ecommerce.flash_sale_items f JOIN ecommerce.campaigns c ON c.id = f.campaign_id
           WHERE f.sku_id = ANY(%s) AND c.starts_at <= %s AND c.ends_at > %s
           ORDER BY f.sku_id, f.sale_price""",
        (sorted(wanted, key=str), t, t),
    )
    offers = {}
    for f in cur.fetchall():
        if f["sku_id"] not in offers and f["sold"] + wanted[f["sku_id"]] <= f["quota"]:
            offers[f["sku_id"]] = f
    return offers


def _check(cur, v: dict, customer_id, base: int, t):
    def bad(why: str, msg: str):
        raise conflict("voucher_invalid", msg, voucher=v["code"], why=why)

    if not (v["starts_at"] <= t < v["ends_at"]):
        bad("not_active", f"Voucher {v['code']} is not active now")
    if base < v["min_spend"]:
        bad("min_spend", f"Voucher {v['code']} needs a minimum spend of {v['min_spend']}")
    cur.execute("SELECT count(*) AS n FROM ecommerce.voucher_usages WHERE voucher_id = %s AND customer_id = %s "
                "AND status = 'APPLIED'", (v["id"], customer_id))
    if cur.fetchone()["n"] >= v["per_customer_limit"]:
        bad("per_customer_limit", f"Voucher {v['code']} already used by this customer")


def _amount(v: dict, base: int) -> int:
    if v["kind"] == "FIXED":
        amt = v["value"]
    else:  # PERCENT in basis points; FREESHIP covers shipping up to value
        amt = base * v["value"] // 10_000 if v["kind"] == "PERCENT" else v["value"]
    if v["max_discount"]:
        amt = min(amt, v["max_discount"])
    return min(amt, base)


def apply_vouchers(cur, vouchers: list, customer_id, drafts: list, t) -> list:
    """Fill each draft's shop_discount / platform_discount / shipping_subsidy and return usages to record.

    drafts: [{"shop_id", "subtotal", "fee", ...}] — mutated in place.
    """
    for d in drafts:
        d.update(shop_discount=0, platform_discount=0, shipping_subsidy=0)
    usages = []
    by_shop = {d["shop_id"]: d for d in drafts}
    seen = defaultdict(int)

    for v in vouchers:
        slot = ("SHOP", v["shop_id"]) if v["scope"] == "SHOP" else ("PLATFORM", v["kind"] == "FREESHIP")
        seen[slot] += 1
        if seen[slot] > 1:
            raise conflict("voucher_invalid", "Only one voucher of each type per shop / checkout",
                           voucher=v["code"], why="stacking")

    # 1. shop vouchers, against that shop's merchandise
    for v in (v for v in vouchers if v["scope"] == "SHOP"):
        d = by_shop.get(v["shop_id"])
        if d is None:
            raise conflict("voucher_invalid", f"Voucher {v['code']} belongs to a shop not in the cart",
                           voucher=v["code"], why="wrong_shop")
        _check(cur, v, customer_id, d["subtotal"], t)
        amt = min(_amount(v, d["subtotal"]), d["subtotal"] - MIN_ORDER_PAYABLE)
        if amt > 0:
            d["shop_discount"] = amt
            usages.append({"voucher": v, "amount": amt, "shop_id": v["shop_id"]})

    # 2. platform voucher, against merchandise left after shop vouchers, split by order
    for v in (v for v in vouchers if v["scope"] == "PLATFORM" and v["kind"] != "FREESHIP"):
        rooms = [d["subtotal"] - d["shop_discount"] - MIN_ORDER_PAYABLE for d in drafts]
        base = sum(d["subtotal"] - d["shop_discount"] for d in drafts)
        _check(cur, v, customer_id, base, t)
        amt = min(_amount(v, base), max(0, sum(rooms)))
        parts = allocate(amt, [max(0, r) for r in rooms])
        for d, part in zip(drafts, parts):
            d["platform_discount"] = part
        if amt > 0:
            usages.append({"voucher": v, "amount": amt, "shop_id": None})

    # 3. free shipping, against shipping fees, split by order
    for v in (v for v in vouchers if v["kind"] == "FREESHIP"):
        fees = [d["fee"] for d in drafts]
        _check(cur, v, customer_id, sum(d["subtotal"] for d in drafts), t)
        amt = min(_amount(v, sum(fees)), sum(fees))
        for d, part in zip(drafts, allocate(amt, fees)):
            d["shipping_subsidy"] = min(part, d["fee"])
        if amt > 0:
            usages.append({"voucher": v, "amount": sum(d["shipping_subsidy"] for d in drafts), "shop_id": None})
    return usages


def split_to_lines(lines: list, shop_discount: int, platform_discount: int) -> list:
    """Per line: (shop_discount, platform_discount) shares, proportional to line totals."""
    totals = [ln["line_total"] for ln in lines]
    return list(zip(allocate(shop_discount, totals), allocate(platform_discount, totals)))

"""Checkout (cart → one order per shop, with flash prices and vouchers) and online payments."""

import uuid
from collections import defaultdict
from datetime import timedelta
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Header
from pydantic import BaseModel, Field

from .. import auth, outbox
from ..clock import business_now
from ..db import transaction
from ..domain import pricing, stock
from ..domain.orders import issue_refund, lock_order, transition
from ..errors import DomainError, conflict, not_found
from ..idempotency import run_command
from ..settings import PAYMENT_PROVIDERS, PAYMENT_WINDOW_S, SELLER_SLA_S, shipping_fee
from .carts import active_cart

router = APIRouter(tags=["checkout"])

PaymentMethod = Literal["COD", "WALLET", "CARD", "BANK_TRANSFER"]


class CheckoutIn(BaseModel):
    customer_id: UUID | None = None       # the caller's own; may be omitted with a customer token
    payment_method: PaymentMethod
    address_id: UUID | None = None     # default address when omitted
    voucher_codes: list[str] = Field(default_factory=list, max_length=10)


class PaymentWebhook(BaseModel):
    transaction_ref: str
    status: Literal["SUCCEEDED", "FAILED"]
    failure_reason: str | None = Field(None, max_length=40)


class BuyerAction(BaseModel):
    customer_id: UUID | None = None
    reason: str | None = Field(None, max_length=40)


def _new_payment(cur, checkout_id, method: str, amount: int, order_id=None) -> dict:
    prefix = "COD" if method == "COD" else "PAY"
    cur.execute(
        """INSERT INTO ecommerce.payments (checkout_id, order_id, method, provider, transaction_ref, amount, status)
           VALUES (%s, %s, %s, %s, %s, %s, 'PENDING') RETURNING *""",
        (checkout_id, order_id, method, PAYMENT_PROVIDERS[method], f"{prefix}-{uuid.uuid4().hex[:24].upper()}", amount),
    )
    payment = cur.fetchone()
    outbox.emit(cur, "payment", payment["id"], "PaymentCreated",
                {"payment_id": payment["id"], "checkout_id": checkout_id, "order_id": order_id,
                 "method": method, "amount": amount, "transaction_ref": payment["transaction_ref"]})
    return payment


@router.post("/checkouts", status_code=201)
def checkout(body: CheckoutIn, idempotency_key: str | None = Header(None)):
    body.customer_id = auth.own_customer_id(body.customer_id)

    def handler(cur):
        cur.execute("SELECT * FROM ecommerce.customers WHERE id = %s", (body.customer_id,))
        customer = cur.fetchone()
        if not customer:
            raise not_found("customer", body.customer_id)
        if not customer["is_active"]:
            raise conflict("customer_inactive", "Customer account is deactivated")
        cur.execute(
            """SELECT * FROM ecommerce.customer_addresses WHERE customer_id = %s
               AND (id = %s OR (%s::uuid IS NULL AND is_default))""",
            (body.customer_id, body.address_id, body.address_id),
        )
        address = cur.fetchone()
        if not address:
            raise not_found("address", body.address_id or "default")

        # Lock order everywhere: voucher quota slots → inventory (by SKU) → flash-sale items (releases go the same way).
        # Voucher quota, stock and flash quota are only read here; they are all taken at the very end, so a popular
        # voucher or a hot flash SKU is locked for milliseconds, not for the whole checkout.
        if body.voucher_codes:
            pricing.lock_customer_vouchers(cur, body.customer_id)
        vouchers = pricing.load_vouchers(cur, body.voucher_codes)
        for v in vouchers:
            pricing.check_quota(cur, v)

        cart = active_cart(cur, body.customer_id, create=False)
        if not cart:
            raise conflict("cart_empty", "Customer has no active cart")
        cur.execute("SELECT sku_id, quantity FROM ecommerce.cart_items WHERE cart_id = %s", (cart["id"],))
        wanted = {r["sku_id"]: r["quantity"] for r in cur.fetchall()}
        if not wanted:
            raise conflict("cart_empty", "Cart has no items")

        cur.execute(
            """SELECT i.sku_id, i.available, s.price, s.list_price, s.weight_g, s.is_active AS sku_active,
                      p.id AS product_id, p.status AS product_status, sh.id AS shop_id, sh.province, sh.is_active AS shop_active
               FROM ecommerce.inventory i
               JOIN ecommerce.product_skus s ON s.id = i.sku_id
               JOIN ecommerce.products p ON p.id = s.product_id
               JOIN ecommerce.shops sh ON sh.id = p.shop_id
               WHERE i.sku_id = ANY(%s)
               FOR SHARE OF s""",
            (sorted(wanted, key=str),),
        )
        # FOR SHARE: a price change in flight (stamped with its business time before it commits) is waited for and
        # read, and a later one waits for this checkout, so the price read here is the one in force at t below.
        # It conflicts only with the price update, which locks just that one row: no lock cycle.
        rows = {r["sku_id"]: r for r in cur.fetchall()}
        unavailable = [str(s) for s, r in rows.items()
                       if not (r["sku_active"] and r["product_status"] == "ACTIVE" and r["shop_active"])]
        if unavailable:
            raise conflict("sku_unavailable", "Some items are no longer for sale", sku_ids=unavailable)
        short = [{"sku_id": str(s), "requested": q, "available": rows[s]["available"]}
                 for s, q in wanted.items() if rows[s]["available"] < q]
        if short:
            raise conflict("out_of_stock", "Not enough stock for some items", items=short)

        t = business_now(cur)          # the checkout's moment: prices, flash window and vouchers are judged at t,
                                       # and the checkout and its orders are stamped with it
        flash = pricing.flash_prices(cur, wanted, t)

        by_shop = defaultdict(list)
        for sku_id, qty in sorted(wanted.items(), key=lambda x: str(x[0])):
            r = rows[sku_id]
            offer = flash.get(sku_id)
            unit = offer["sale_price"] if offer else r["price"]
            by_shop[r["shop_id"]].append({"sku_id": sku_id, "product_id": r["product_id"], "quantity": qty,
                                          "list_price": r["list_price"], "unit_price": unit, "line_total": unit * qty,
                                          "campaign_item_id": offer["id"] if offer else None})
        drafts = []
        for shop_id, lines in by_shop.items():
            weight = sum(rows[ln["sku_id"]]["weight_g"] * ln["quantity"] for ln in lines)
            drafts.append({"shop_id": shop_id, "lines": lines, "subtotal": sum(ln["line_total"] for ln in lines),
                           "fee": shipping_fee(rows[lines[0]["sku_id"]]["province"], address["province"], weight)})
        usages = pricing.apply_vouchers(cur, vouchers, body.customer_id, drafts, t)   # a voucher worth 0 VND here
                                                                                      # takes no redemption

        online = body.payment_method != "COD"
        items_total = sum(d["subtotal"] for d in drafts)
        shipping_total = sum(d["fee"] for d in drafts)
        discount_total = sum(d["shop_discount"] + d["platform_discount"] + d["shipping_subsidy"] for d in drafts)
        cur.execute(
            """INSERT INTO ecommerce.checkouts (customer_id, cart_id, address_id, payment_method, status, items_total,
                                                shipping_total, discount_total, grand_total, pay_by, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
            (body.customer_id, cart["id"], address["id"], body.payment_method,
             "PENDING_PAYMENT" if online else "COD_CONFIRMED", items_total, shipping_total, discount_total,
             items_total + shipping_total - discount_total, t + timedelta(seconds=PAYMENT_WINDOW_S) if online else None,
             t),
        )
        co = cur.fetchone()

        orders, order_by_shop, reservations = [], {}, []
        for d in drafts:
            cur.execute("SELECT nextval('ecommerce.order_number_seq') AS n")
            number = f"{t:%y%m%d}{cur.fetchone()['n']:08d}"
            status = "UNPAID" if online else "TO_SHIP"
            total = d["subtotal"] - d["shop_discount"] - d["platform_discount"] + d["fee"] - d["shipping_subsidy"]
            cur.execute(
                """INSERT INTO ecommerce.orders
                   (order_number, checkout_id, customer_id, shop_id, status, payment_method, subtotal, shop_discount,
                    platform_discount, shipping_fee, shipping_subsidy, total, ship_to_name, ship_to_address,
                    ship_to_province, confirm_by, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
                (number, co["id"], body.customer_id, d["shop_id"], status, body.payment_method, d["subtotal"],
                 d["shop_discount"], d["platform_discount"], d["fee"], d["shipping_subsidy"], total,
                 address["recipient_name"], f"{address['street']}, {address['district']}, {address['province']}",
                 address["province"], None if online else t + timedelta(seconds=SELLER_SLA_S), t),
            )
            order = cur.fetchone()
            order_by_shop[d["shop_id"]] = order
            shares = pricing.split_to_lines(d["lines"], d["shop_discount"], d["platform_discount"])
            for ln, (sd, pd) in zip(d["lines"], shares):
                cur.execute(
                    """INSERT INTO ecommerce.order_items
                       (order_id, sku_id, product_id, quantity, list_price, unit_price, line_total, shop_discount,
                        platform_discount, net_amount, campaign_item_id)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (order["id"], ln["sku_id"], ln["product_id"], ln["quantity"], ln["list_price"], ln["unit_price"],
                     ln["line_total"], sd, pd, ln["line_total"] - sd - pd, ln["campaign_item_id"]),
                )
                reservations.append((ln["sku_id"], ln["quantity"], order["id"]))
            cur.execute(
                """INSERT INTO ecommerce.order_status_history (order_id, from_status, to_status, actor, reason)
                   VALUES (%s, NULL, %s, 'BUYER', 'checkout')""",
                (order["id"], status),
            )
            outbox.emit(cur, "order", order["id"], "OrderPlaced", {
                "order_id": order["id"], "order_number": number, "checkout_id": co["id"], "shop_id": d["shop_id"],
                "customer_id": body.customer_id, "status": status, "payment_method": body.payment_method,
                "subtotal": d["subtotal"], "shop_discount": d["shop_discount"],
                "platform_discount": d["platform_discount"], "shipping_fee": d["fee"],
                "shipping_subsidy": d["shipping_subsidy"], "total": total,
                "items": [{"sku_id": ln["sku_id"], "quantity": ln["quantity"], "unit_price": ln["unit_price"],
                           "flash_sale": ln["campaign_item_id"] is not None} for ln in d["lines"]],
            })
            if not online:
                _new_payment(cur, co["id"], "COD", total, order_id=order["id"])
            orders.append(order)

        applied = {u["voucher"]["id"]: u["voucher"] for u in usages}          # one redemption per voucher
        claimed = {vid: pricing.claim_quota(cur, applied[vid]) for vid in sorted(applied, key=str)}
        _take_stock_and_flash_quota(cur, reservations, drafts)

        for u in sorted(usages, key=lambda u: str(u["voucher"]["id"])):
            v = u["voucher"]
            cur.execute(
                """INSERT INTO ecommerce.voucher_usages (voucher_id, customer_id, checkout_id, order_id, amount, quota_slot)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (v["id"], body.customer_id, co["id"],
                 order_by_shop[u["shop_id"]]["id"] if u["shop_id"] else None, u["amount"], claimed[v["id"]]),
            )

        payment = _new_payment(cur, co["id"], body.payment_method, co["grand_total"]) if online else None
        cur.execute("UPDATE ecommerce.carts SET status = 'CHECKED_OUT' WHERE id = %s", (cart["id"],))
        outbox.emit(cur, "checkout", co["id"], "CheckoutCreated", {
            "checkout_id": co["id"], "customer_id": body.customer_id, "cart_id": cart["id"],
            "payment_method": body.payment_method, "orders": [o["id"] for o in orders],
            "items_total": items_total, "shipping_total": shipping_total, "discount_total": discount_total,
            "grand_total": co["grand_total"], "vouchers": [{"code": u["voucher"]["code"], "amount": u["amount"]} for u in usages],
        })
        return {"checkout": co, "orders": orders, "payment": payment}

    return run_command(idempotency_key, "POST /checkouts", body.model_dump(), handler, 201)


@router.post("/checkouts/{checkout_id}/payments", status_code=201)
def retry_payment(checkout_id: UUID, idempotency_key: str | None = Header(None)):
    """Buyer tries again after a failed attempt; any still-pending attempt is superseded."""
    def handler(cur):
        co = _lock_checkout(cur, checkout_id)
        auth.customer(co["customer_id"], "checkout")
        if co["status"] != "PENDING_PAYMENT":
            raise conflict("not_awaiting_payment", f"Checkout is {co['status']}")
        if co["pay_by"] < business_now(cur):
            raise conflict("payment_window_closed", "Payment deadline has passed")
        cur.execute("UPDATE ecommerce.payments SET status = 'CANCELLED', failure_reason = 'superseded' "
                    "WHERE checkout_id = %s AND status = 'PENDING'", (checkout_id,))
        return _new_payment(cur, checkout_id, co["payment_method"], co["grand_total"])

    return run_command(idempotency_key, f"POST /checkouts/{checkout_id}/payments", {}, handler, 201)


@router.post("/payments/webhook")
def payment_webhook(body: PaymentWebhook, idempotency_key: str | None = Header(None),
                    partner: str | None = Depends(auth.verify_webhook)):
    """Gateway callback, signed by the provider (X-Partner, X-Signature). Providers retry and duplicate callbacks:
    a repeated outcome is a no-op."""
    def handler(cur):
        cur.execute("SELECT checkout_id FROM ecommerce.payments WHERE transaction_ref = %s", (body.transaction_ref,))
        ref = cur.fetchone()
        if not ref:
            raise not_found("payment", body.transaction_ref)
        co = _lock_checkout(cur, ref["checkout_id"])             # lock order: checkout → payment → orders
        cur.execute("SELECT * FROM ecommerce.payments WHERE transaction_ref = %s FOR UPDATE", (body.transaction_ref,))
        payment = cur.fetchone()
        auth.partner_is(payment["provider"], partner, "payment")

        if payment["status"] == body.status:
            return {"payment": payment, "result": "duplicate_ignored"}
        if payment["status"] not in ("PENDING", "EXPIRED", "CANCELLED"):
            raise conflict("payment_already_final", f"Payment is already {payment['status']}")

        if body.status == "FAILED":
            if payment["status"] != "PENDING":
                return {"payment": payment, "result": "stale_failure_ignored"}
            cur.execute("UPDATE ecommerce.payments SET status = 'FAILED', failure_reason = %s WHERE id = %s RETURNING *",
                        (body.failure_reason or "declined", payment["id"]))
            updated = cur.fetchone()
            outbox.emit(cur, "payment", payment["id"], "PaymentFailed",
                        {"payment_id": payment["id"], "checkout_id": co["id"], "reason": updated["failure_reason"]})
            return {"payment": updated, "result": "failed"}

        # SUCCEEDED: money has been captured by the provider, whatever our side thinks
        cur.execute("UPDATE ecommerce.payments SET status = 'SUCCEEDED', paid_at = %s WHERE id = %s RETURNING *",
                    (business_now(cur), payment["id"]))
        updated = cur.fetchone()
        outbox.emit(cur, "payment", payment["id"], "PaymentSucceeded",
                    {"payment_id": payment["id"], "checkout_id": co["id"], "method": payment["method"],
                     "amount": payment["amount"]})

        if co["status"] != "PENDING_PAYMENT" or payment["status"] != "PENDING":
            # Late or superseded attempt (checkout expired / cancelled / already paid): give the money back
            issue_refund(cur, payment["id"], None, payment["amount"], "late_payment")
            return {"payment": updated, "result": "refunded_late_payment"}

        cur.execute("UPDATE ecommerce.payments SET status = 'CANCELLED', failure_reason = 'superseded' "
                    "WHERE checkout_id = %s AND status = 'PENDING'", (co["id"],))
        cur.execute("UPDATE ecommerce.checkouts SET status = 'PAID' WHERE id = %s", (co["id"],))
        cur.execute("SELECT id FROM ecommerce.orders WHERE checkout_id = %s AND status = 'UNPAID' ORDER BY id", (co["id"],))
        for o in cur.fetchall():
            transition(cur, o["id"], "TO_SHIP", "PAYMENT", "payment_succeeded")
        return {"payment": updated, "result": "paid"}

    return run_command(idempotency_key, "POST /payments/webhook", body.model_dump(), handler)


@router.post("/orders/{order_id}/cancel")
def buyer_cancel(order_id: UUID, body: BuyerAction, idempotency_key: str | None = Header(None)):
    """Before payment the whole checkout is cancelled (orders of one payment go together)."""
    def handler(cur):
        cur.execute("SELECT checkout_id, customer_id, status FROM ecommerce.orders WHERE id = %s", (order_id,))
        o = cur.fetchone()
        if not o:
            raise not_found("order", order_id)
        auth.customer(o["customer_id"], "order")
        if body.customer_id is not None and o["customer_id"] != body.customer_id:
            raise conflict("not_your_order", "Order belongs to another customer")
        reason = body.reason or "buyer_changed_mind"
        if o["status"] != "UNPAID":
            return transition(cur, order_id, "CANCELLED", "BUYER", reason)
        co = _lock_checkout(cur, o["checkout_id"])
        if co["status"] != "PENDING_PAYMENT":
            raise conflict("invalid_transition", f"Checkout is {co['status']}")
        close_checkout(cur, co, "CANCELLED", "BUYER", reason)
        return lock_order(cur, order_id)

    return run_command(idempotency_key, f"POST /orders/{order_id}/cancel", body.model_dump(mode="json"), handler)


def _take_stock_and_flash_quota(cur, reservations: list, drafts: list):
    """Reserve stock and flash quota last, in SKU / item order. Each is one conditional UPDATE, so the checked
    amounts can't be oversold even though they were read without locks."""
    for sku_id, qty, order_id in sorted(reservations, key=lambda r: str(r[0])):
        try:
            stock.move(cur, sku_id, "RESERVE", qty, "ORDER", order_id)
        except DomainError as e:
            if e.detail["code"] != "out_of_stock":
                raise
            cur.execute("SELECT available FROM ecommerce.inventory WHERE sku_id = %s", (sku_id,))
            raise conflict("out_of_stock", "Not enough stock for some items",
                           items=[{"sku_id": str(sku_id), "requested": qty, "available": cur.fetchone()["available"]}])
    flash = sorted(((ln["campaign_item_id"], ln["quantity"], ln["sku_id"]) for d in drafts for ln in d["lines"]
                    if ln["campaign_item_id"]), key=lambda x: str(x[0]))
    for item_id, qty, sku_id in flash:
        cur.execute("UPDATE ecommerce.flash_sale_items SET sold = sold + %s WHERE id = %s AND sold + %s <= quota "
                    "RETURNING id", (qty, item_id, qty))
        if not cur.fetchone():
            raise conflict("flash_sold_out", "Flash sale quota ran out during checkout; the regular price applies now",
                           sku_id=str(sku_id))


def close_checkout(cur, co: dict, status: str, actor: str, reason: str):
    """Unpaid checkout ends (buyer cancel or payment timeout): give back vouchers, void attempts, cancel orders."""
    cur.execute("UPDATE ecommerce.checkouts SET status = %s WHERE id = %s", (status, co["id"]))
    cur.execute("UPDATE ecommerce.voucher_usages SET status = 'RELEASED' WHERE checkout_id = %s AND status = 'APPLIED' "
                "RETURNING voucher_id, quota_slot", (co["id"],))
    pricing.release_quota(cur, cur.fetchall())
    cur.execute(
        "UPDATE ecommerce.payments SET status = %s, failure_reason = %s WHERE checkout_id = %s AND status = 'PENDING'",
        ("EXPIRED" if status == "EXPIRED" else "CANCELLED", reason, co["id"]),
    )
    cur.execute("SELECT id FROM ecommerce.orders WHERE checkout_id = %s AND status = 'UNPAID' ORDER BY id", (co["id"],))
    unpaid = [o["id"] for o in cur.fetchall()]
    # The orders are cancelled one by one, each releasing its own SKUs; lock every SKU and flash item of the
    # checkout first, in the global order checkouts use, so two checkouts never take them in opposite orders.
    # NO KEY UPDATE is the mode the UPDATEs take anyway; it doesn't block the KEY SHARE locks that a concurrent
    # checkout holds on flash items through its order_items foreign key (FOR UPDATE would, and deadlock).
    cur.execute("""SELECT 1 FROM ecommerce.inventory WHERE sku_id IN (
                       SELECT sku_id FROM ecommerce.order_items WHERE order_id = ANY(%s::uuid[]))
                   ORDER BY sku_id FOR NO KEY UPDATE""", ([str(o) for o in unpaid],))
    cur.execute("""SELECT 1 FROM ecommerce.flash_sale_items WHERE id IN (
                       SELECT campaign_item_id FROM ecommerce.order_items WHERE order_id = ANY(%s::uuid[]))
                   ORDER BY id FOR NO KEY UPDATE""", ([str(o) for o in unpaid],))
    for order_id in unpaid:
        transition(cur, order_id, "CANCELLED", actor, reason)
    outbox.emit(cur, "checkout", co["id"], "CheckoutClosed", {"checkout_id": co["id"], "status": status, "reason": reason})


def _lock_checkout(cur, checkout_id) -> dict:
    cur.execute("SELECT * FROM ecommerce.checkouts WHERE id = %s FOR UPDATE", (checkout_id,))
    co = cur.fetchone()
    if not co:
        raise not_found("checkout", checkout_id)
    return co


@router.get("/checkouts/{checkout_id}")
def get_checkout(checkout_id: UUID):
    with transaction() as cur:
        cur.execute("SELECT * FROM ecommerce.checkouts WHERE id = %s", (checkout_id,))
        co = cur.fetchone()
        if not co:
            raise not_found("checkout", checkout_id)
        auth.read_customer(co["customer_id"], "checkout")
        cur.execute("SELECT * FROM ecommerce.payments WHERE checkout_id = %s ORDER BY created_at", (checkout_id,))
        co["payments"] = cur.fetchall()
        cur.execute("SELECT id, order_number, shop_id, status, total, shop_discount, platform_discount, shipping_subsidy "
                    "FROM ecommerce.orders WHERE checkout_id = %s", (checkout_id,))
        co["orders"] = cur.fetchall()
        cur.execute("SELECT v.code, u.amount, u.status FROM ecommerce.voucher_usages u "
                    "JOIN ecommerce.vouchers v ON v.id = u.voucher_id WHERE u.checkout_id = %s", (checkout_id,))
        co["vouchers"] = cur.fetchall()
    return co

"""Order state machine: the one place where an order changes status.

Every caller (buyer, seller, payment and carrier webhooks, return flow, background jobs) goes through
`transition`, which validates the move and the actor, applies the side effects on stock, flash-sale quota,
payments, refunds, loyalty and seller settlement, and records history + event.
"""

from datetime import timedelta

from .. import outbox
from ..clock import business_now
from ..errors import conflict, not_found
from ..settings import MAX_DELIVERY_ATTEMPTS, SELLER_SLA_S, tier_for
from . import settlement, stock

# (from, to) -> actors allowed to make that move
TRANSITIONS = {
    ("UNPAID", "TO_SHIP"):                     {"PAYMENT"},
    ("UNPAID", "CANCELLED"):                   {"BUYER", "SYSTEM"},
    ("TO_SHIP", "READY_TO_SHIP"):              {"SELLER"},
    ("TO_SHIP", "CANCELLED"):                  {"BUYER", "SELLER", "SYSTEM"},
    ("READY_TO_SHIP", "SHIPPED"):              {"LOGISTICS"},
    ("SHIPPED", "DELIVERED"):                  {"LOGISTICS"},
    ("SHIPPED", "DELIVERY_FAILED"):            {"LOGISTICS"},
    ("DELIVERY_FAILED", "SHIPPED"):            {"LOGISTICS"},
    ("DELIVERY_FAILED", "RETURNED_TO_SELLER"): {"LOGISTICS"},
    ("DELIVERED", "COMPLETED"):                {"BUYER", "SYSTEM"},
    ("DELIVERED", "RETURN_REFUND"):            {"BUYER"},
    ("RETURN_REFUND", "COMPLETED"):            {"SYSTEM"},      # return rejected or only partly refunded
}


def lock_order(cur, order_id) -> dict:
    cur.execute("SELECT * FROM ecommerce.orders WHERE id = %s FOR UPDATE", (order_id,))
    order = cur.fetchone()
    if not order:
        raise not_found("order", order_id)
    return order


def record_status(cur, order: dict, frm, to, actor, reason):
    cur.execute(
        """INSERT INTO ecommerce.order_status_history (order_id, from_status, to_status, actor, reason)
           VALUES (%s, %s, %s, %s, %s)""",
        (order["id"], frm, to, actor, reason),
    )
    outbox.emit(cur, "order", order["id"], "OrderStatusChanged", {
        "order_id": order["id"], "order_number": order["order_number"], "shop_id": order["shop_id"],
        "from": frm, "to": to, "actor": actor, "reason": reason,
    })


def transition(cur, order_id, to: str, actor: str, reason: str | None = None, order: dict | None = None) -> dict:
    order = order or lock_order(cur, order_id)
    frm = order["status"]
    allowed = TRANSITIONS.get((frm, to))
    if allowed is None:
        raise conflict("invalid_transition", f"Cannot move an order from {frm} to {to}", current=frm, requested=to)
    if actor not in allowed:
        raise conflict("actor_not_allowed", f"{actor} cannot move an order from {frm} to {to}")
    if (frm, to) == ("DELIVERY_FAILED", "SHIPPED") and order["delivery_attempts"] >= MAX_DELIVERY_ATTEMPTS:
        raise conflict("max_attempts_reached", f"Order already failed delivery {MAX_DELIVERY_ATTEMPTS} times")

    cur.execute("SELECT id, sku_id, quantity, campaign_item_id FROM ecommerce.order_items "
                "WHERE order_id = %s ORDER BY sku_id", (order["id"],))
    items = cur.fetchall()
    t = business_now(cur)
    changes = {"status": to}

    if to == "TO_SHIP":
        changes.update(paid_at=t, confirm_by=t + timedelta(seconds=SELLER_SLA_S))
    elif to == "READY_TO_SHIP":
        changes["ready_at"] = t
    elif to == "SHIPPED" and frm == "READY_TO_SHIP":
        changes["shipped_at"] = t
        for it in items:
            stock.move(cur, it["sku_id"], "SHIP", it["quantity"], "ORDER", order["id"])
    elif to == "DELIVERY_FAILED":
        changes["delivery_attempts"] = order["delivery_attempts"] + 1
    elif to == "DELIVERED":
        changes["delivered_at"] = t
        _collect_cod(cur, order, t)
    elif to == "COMPLETED":
        changes["completed_at"] = t
    elif to == "CANCELLED":
        changes.update(cancelled_at=t, cancel_reason=reason, cancelled_by=actor)
        for it in items:
            stock.move(cur, it["sku_id"], "RELEASE", it["quantity"], "ORDER", order["id"])
        _release_flash_quota(cur, items)
        _settle_unfulfilled(cur, order, reason or "cancelled")
    elif to == "RETURNED_TO_SELLER":
        for it in items:
            stock.move(cur, it["sku_id"], "RETURN_TO_SENDER", it["quantity"], "ORDER", order["id"])
        _settle_unfulfilled(cur, order, "returned_to_seller")

    sets = ", ".join(f"{k} = %({k})s" for k in changes)
    cur.execute(
        f"UPDATE ecommerce.orders SET {sets}, version = version + 1 WHERE id = %(id)s RETURNING *",
        {**changes, "id": order["id"]},
    )
    updated = cur.fetchone()
    record_status(cur, order, frm, to, actor, reason)

    if to == "COMPLETED":
        _credit_customer(cur, updated)
        settlement.settle_order(cur, updated)
    return updated


def _release_flash_quota(cur, items):
    for it in sorted((i for i in items if i["campaign_item_id"]), key=lambda i: str(i["campaign_item_id"])):
        cur.execute("UPDATE ecommerce.flash_sale_items SET sold = sold - %s WHERE id = %s",
                    (it["quantity"], it["campaign_item_id"]))


def _collect_cod(cur, order, t):
    cur.execute(
        """UPDATE ecommerce.payments SET status = 'SUCCEEDED', paid_at = %s
           WHERE order_id = %s AND method = 'COD' AND status = 'PENDING' RETURNING id, amount""",
        (t, order["id"]),
    )
    p = cur.fetchone()
    if p:
        outbox.emit(cur, "payment", p["id"], "PaymentSucceeded",
                    {"payment_id": p["id"], "checkout_id": order["checkout_id"], "order_id": order["id"],
                     "method": "COD", "amount": p["amount"]})


def paid_payment(cur, order: dict) -> dict | None:
    """The payment that holds this order's money: its COD collection, or the checkout's kept online payment."""
    if order["payment_method"] == "COD":
        cur.execute("SELECT * FROM ecommerce.payments WHERE order_id = %s AND status = 'SUCCEEDED'", (order["id"],))
    else:
        cur.execute(
            """SELECT p.* FROM ecommerce.payments p
               WHERE p.checkout_id = %s AND p.order_id IS NULL AND p.status = 'SUCCEEDED'
                 AND NOT EXISTS (SELECT 1 FROM ecommerce.refunds r WHERE r.payment_id = p.id AND r.reason = 'late_payment')""",
            (order["checkout_id"],),
        )
    return cur.fetchone()


def _settle_unfulfilled(cur, order, reason: str):
    """Order will not reach the buyer: void its COD collection or refund an online payment."""
    if order["payment_method"] == "COD":
        cur.execute(
            """UPDATE ecommerce.payments SET status = 'CANCELLED', failure_reason = %s
               WHERE order_id = %s AND status = 'PENDING'""",
            (reason, order["id"]),
        )
        return
    paid = paid_payment(cur, order)
    if paid:   # an UNPAID order has nothing to refund
        issue_refund(cur, paid["id"], order["id"], order["total"], reason)


def issue_refund(cur, payment_id, order_id, amount: int, reason: str, return_id=None):
    cur.execute(
        """INSERT INTO ecommerce.refunds (payment_id, order_id, return_id, amount, reason)
           VALUES (%s, %s, %s, %s, %s) RETURNING id""",
        (payment_id, order_id, return_id, amount, reason),
    )
    refund_id = cur.fetchone()["id"]
    outbox.emit(cur, "payment", payment_id, "RefundIssued",
                {"refund_id": refund_id, "payment_id": payment_id, "order_id": order_id, "return_id": return_id,
                 "amount": amount, "reason": reason})


def _credit_customer(cur, order):
    """Loyalty counts what the buyer finally paid: order total minus anything refunded."""
    cur.execute("SELECT coalesce(sum(amount), 0) AS r FROM ecommerce.refunds WHERE order_id = %s", (order["id"],))
    amount = order["total"] - cur.fetchone()["r"]
    cur.execute("SELECT total_spent, tier FROM ecommerce.customers WHERE id = %s FOR UPDATE", (order["customer_id"],))
    c = cur.fetchone()
    spent = c["total_spent"] + amount
    tier = tier_for(spent)
    cur.execute("UPDATE ecommerce.customers SET total_spent = %s, tier = %s WHERE id = %s",
                (spent, tier, order["customer_id"]))
    outbox.emit(cur, "customer", order["customer_id"], "CustomerSpendUpdated",
                {"customer_id": order["customer_id"], "order_id": order["id"], "amount": amount,
                 "total_spent": spent, "tier_before": c["tier"], "tier_after": tier})

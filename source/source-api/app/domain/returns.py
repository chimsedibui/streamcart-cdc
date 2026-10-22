"""Return / refund flow after delivery.

    REQUESTED ─seller approves (or no answer in time)─▶ APPROVED ─carrier picks up─▶ RETURNING ─delivered─▶ RECEIVED
        │                                                   ▲                                                  │
        └─seller rejects─▶ REJECTED ─buyer disputes─▶ DISPUTED ─platform: refund                seller inspects
                              │                           └─platform: reject─▶ CLOSED                          ▼
                              └─no dispute in time─▶ CLOSED                                                 REFUNDED

A refund pays back exactly what the buyer paid per returned unit (line net amount, after vouchers).
After the return ends the order completes, unless every unit came back (then it stays RETURN_REFUND).
"""

from .. import outbox
from ..errors import conflict
from . import stock
from .orders import issue_refund, lock_order, paid_payment, transition

RETURN_MOVES = {
    ("REQUESTED", "APPROVED"): {"SELLER", "SYSTEM"},
    ("REQUESTED", "REJECTED"): {"SELLER"},
    ("REJECTED", "DISPUTED"):  {"BUYER"},
    ("REJECTED", "CLOSED"):    {"SYSTEM"},
    ("DISPUTED", "APPROVED"):  {"PLATFORM"},
    ("DISPUTED", "CLOSED"):    {"PLATFORM"},
    ("APPROVED", "RETURNING"): {"LOGISTICS"},
    ("RETURNING", "RECEIVED"): {"LOGISTICS"},
    ("RECEIVED", "REFUNDED"):  {"SELLER"},
}


def unit_refund(net_amount: int, qty: int, of_qty: int) -> int:
    return net_amount * qty // of_qty


def set_return_status(cur, ret: dict, to: str, actor: str, reason: str | None = None) -> dict:
    allowed = RETURN_MOVES.get((ret["status"], to))
    if allowed is None:
        raise conflict("invalid_return_transition", f"Cannot move a return from {ret['status']} to {to}",
                       current=ret["status"], requested=to)
    if actor not in allowed:
        raise conflict("actor_not_allowed", f"{actor} cannot move a return from {ret['status']} to {to}")
    final = to in ("REFUNDED", "CLOSED")
    cur.execute(
        "UPDATE ecommerce.returns SET status = %s, resolved_at = CASE WHEN %s THEN ecommerce.business_now() END "
        "WHERE id = %s RETURNING *",
        (to, final, ret["id"]),
    )
    updated = cur.fetchone()
    outbox.emit(cur, "return", ret["id"], "ReturnStatusChanged",
                {"return_id": ret["id"], "order_id": ret["order_id"], "from": ret["status"], "to": to,
                 "actor": actor, "reason": reason})
    return updated


def close_rejected(cur, ret: dict, actor: str, reason: str):
    """Return ends without refund: the buyer keeps everything and the order completes."""
    set_return_status(cur, ret, "CLOSED", actor, reason)
    transition(cur, ret["order_id"], "COMPLETED", "SYSTEM", "return_rejected")


def refund_received(cur, ret: dict, conditions: dict) -> dict:
    """Seller inspected the parcel: restock sellable units, refund the buyer, finish the order."""
    order = lock_order(cur, ret["order_id"])
    cur.execute(
        """SELECT ri.id, ri.order_item_id, ri.quantity, ri.refund_amount, oi.sku_id, oi.quantity AS ordered
           FROM ecommerce.return_items ri JOIN ecommerce.order_items oi ON oi.id = ri.order_item_id
           WHERE ri.return_id = %s ORDER BY oi.sku_id""",
        (ret["id"],),
    )
    items = cur.fetchall()
    for it in items:
        condition = conditions.get(str(it["order_item_id"]), "SELLABLE")
        cur.execute("UPDATE ecommerce.return_items SET condition = %s WHERE id = %s", (condition, it["id"]))
        if condition == "SELLABLE":
            stock.move(cur, it["sku_id"], "CUSTOMER_RETURN", it["quantity"], "RETURN", ret["id"])
    amount = sum(it["refund_amount"] for it in items)
    if amount:
        issue_refund(cur, paid_payment(cur, order)["id"], order["id"], amount, "return", return_id=ret["id"])
    cur.execute("UPDATE ecommerce.returns SET refund_amount = %s WHERE id = %s", (amount, ret["id"]))
    updated = set_return_status(cur, {**ret, "refund_amount": amount}, "REFUNDED", "SELLER", "inspected")

    cur.execute("SELECT sum(quantity) AS units FROM ecommerce.order_items WHERE order_id = %s",
                (order["id"],))
    all_back = cur.fetchone()["units"] == sum(it["quantity"] for it in items)
    if not all_back:
        transition(cur, order["id"], "COMPLETED", "SYSTEM", "partial_refund", order=order)
    return updated


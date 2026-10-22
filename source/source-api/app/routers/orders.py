"""Order queries and buyer confirmation of receipt. Carrier callbacks live in logistics.py."""

from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Header, Query
from pydantic import BaseModel

from .. import auth
from ..db import transaction
from ..domain.orders import transition
from ..errors import conflict, not_found
from ..idempotency import run_command

router = APIRouter(tags=["orders"])

OrderStatus = Literal["UNPAID", "TO_SHIP", "READY_TO_SHIP", "SHIPPED", "DELIVERY_FAILED", "DELIVERED",
                      "COMPLETED", "CANCELLED", "RETURNED_TO_SELLER", "RETURN_REFUND"]

class ConfirmReceived(BaseModel):
    customer_id: UUID | None = None


@router.post("/orders/{order_id}/confirm-received")
def confirm_received(order_id: UUID, body: ConfirmReceived, idempotency_key: str | None = Header(None)):
    def handler(cur):
        cur.execute("SELECT customer_id FROM ecommerce.orders WHERE id = %s", (order_id,))
        o = cur.fetchone()
        if not o:
            raise not_found("order", order_id)
        auth.customer(o["customer_id"], "order")
        if body.customer_id is not None and o["customer_id"] != body.customer_id:
            raise conflict("not_your_order", "Order belongs to another customer")
        return transition(cur, order_id, "COMPLETED", "BUYER", "buyer_confirmed")

    return run_command(idempotency_key, f"POST /orders/{order_id}/confirm-received", body.model_dump(), handler)


@router.get("/orders")
def list_orders(status: OrderStatus | None = None, shop_id: UUID | None = None, customer_id: UUID | None = None,
                random: bool = False, limit: int = Query(100, ge=1, le=10000)):
    customer_id, shop_id = auth.scope_listing(customer_id, shop_id)
    order = "RANDOM()" if random else "created_at DESC"
    with transaction() as cur:
        cur.execute(
            f"""SELECT id, order_number, checkout_id, customer_id, shop_id, status, payment_method, total,
                       delivery_attempts, created_at
                FROM ecommerce.orders
                WHERE (%(status)s IS NULL OR status = %(status)s)
                  AND (%(shop)s::uuid IS NULL OR shop_id = %(shop)s::uuid)
                  AND (%(customer)s::uuid IS NULL OR customer_id = %(customer)s::uuid)
                ORDER BY {order} LIMIT %(limit)s""",
            {"status": status, "shop": shop_id, "customer": customer_id, "limit": limit},
        )
        return cur.fetchall()


@router.get("/orders/{order_id}")
def get_order(order_id: UUID):
    with transaction() as cur:
        cur.execute("SELECT * FROM ecommerce.orders WHERE id = %s", (order_id,))
        order = cur.fetchone()
        if not order:
            raise not_found("order", order_id)
        auth.read_party(order["customer_id"], order["shop_id"], "order")
        if auth.mask_pii():
            order["ship_to_name"], order["ship_to_address"] = (auth.mask(order["ship_to_name"]),
                                                               auth.mask(order["ship_to_address"]))
        cur.execute("SELECT * FROM ecommerce.order_items WHERE order_id = %s ORDER BY sku_id", (order_id,))
        order["items"] = cur.fetchall()
        cur.execute("SELECT from_status, to_status, actor, reason, created_at FROM ecommerce.order_status_history "
                    "WHERE order_id = %s ORDER BY id", (order_id,))
        order["history"] = cur.fetchall()
        cur.execute("SELECT direction, carrier, tracking_number, status, attempts FROM ecommerce.shipments "
                    "WHERE order_id = %s", (order_id,))
        order["shipments"] = cur.fetchall()
        cur.execute("SELECT * FROM ecommerce.returns WHERE order_id = %s", (order_id,))
        order["return"] = cur.fetchone()
    return order


@router.get("/stats")
def stats():
    auth.require_scope("ops:read")
    with transaction() as cur:
        cur.execute(
            """SELECT (SELECT count(*) FROM ecommerce.customers)    AS customers,
                      (SELECT count(*) FROM ecommerce.shops)        AS shops,
                      (SELECT count(*) FROM ecommerce.products)     AS products,
                      (SELECT count(*) FROM ecommerce.product_skus) AS skus,
                      (SELECT count(*) FROM ecommerce.categories)   AS categories,
                      (SELECT count(*) FROM ecommerce.orders)       AS orders,
                      (SELECT count(*) FROM ecommerce.returns)      AS returns"""
        )
        return cur.fetchone()

"""Returns & refunds (buyer, seller, platform) and product reviews."""

from datetime import timedelta
from typing import Literal
from uuid import UUID

import psycopg2
from fastapi import APIRouter, Header, Query
from pydantic import BaseModel, Field

from .. import auth, outbox
from ..clock import business_now
from ..db import transaction
from ..domain.orders import lock_order, transition
from ..domain.returns import close_rejected, refund_received, set_return_status, unit_refund
from ..errors import conflict, not_found
from ..idempotency import run_command
from ..settings import RETURN_RESPONSE_S
from .sellers import create_shipment

router = APIRouter(tags=["returns & reviews"])


class ReturnLine(BaseModel):
    order_item_id: UUID
    quantity: int = Field(ge=1)


class RequestReturn(BaseModel):
    customer_id: UUID | None = None
    reason: Literal["DAMAGED", "WRONG_ITEM", "NOT_AS_DESCRIBED", "MISSING_PARTS", "CHANGED_MIND"]
    items: list[ReturnLine] = Field(min_length=1)


class BuyerRef(BaseModel):
    customer_id: UUID | None = None


class ShopRef(BaseModel):
    shop_id: UUID | None = None


class Inspection(ShopRef):
    conditions: dict[str, Literal["SELLABLE", "DAMAGED"]] = Field(default_factory=dict)   # order_item_id -> condition


class Resolution(BaseModel):
    decision: Literal["REFUND", "REJECT"]


class ReviewLine(BaseModel):
    order_item_id: UUID
    rating: int = Field(ge=1, le=5)
    comment: str | None = Field(None, max_length=300)


class PostReviews(BaseModel):
    customer_id: UUID | None = None
    items: list[ReviewLine] = Field(min_length=1)


def _lock_return(cur, return_id) -> dict:
    cur.execute("SELECT * FROM ecommerce.returns WHERE id = %s FOR UPDATE", (return_id,))
    ret = cur.fetchone()
    if not ret:
        raise not_found("return", return_id)
    return ret


def approve(cur, ret: dict, actor: str, reason: str) -> dict:
    """Approve and book the return parcel (buyer → seller)."""
    updated = set_return_status(cur, ret, "APPROVED", actor, reason)
    cur.execute("SELECT o.*, s.province AS shop_province FROM ecommerce.orders o JOIN ecommerce.shops s ON s.id = o.shop_id "
                "WHERE o.id = %s", (ret["order_id"],))
    order = cur.fetchone()
    create_shipment(cur, order, "RETURN", order["ship_to_province"], order["shop_province"], return_id=ret["id"])
    return updated


@router.post("/orders/{order_id}/returns", status_code=201)
def request_return(order_id: UUID, body: RequestReturn, idempotency_key: str | None = Header(None)):
    def handler(cur):
        order = lock_order(cur, order_id)
        auth.customer(order["customer_id"], "order")
        if body.customer_id is not None and order["customer_id"] != body.customer_id:
            raise conflict("not_your_order", "Order belongs to another customer")
        cur.execute("SELECT id, quantity, net_amount FROM ecommerce.order_items WHERE order_id = %s", (order_id,))
        lines = {r["id"]: r for r in cur.fetchall()}
        wanted = {}
        for it in body.items:
            line = lines.get(it.order_item_id)
            if not line or it.quantity > line["quantity"] or it.order_item_id in wanted:
                raise conflict("invalid_return_items", "Return lines must be distinct items of this order, "
                               "within the ordered quantity")
            wanted[it.order_item_id] = it.quantity
        transition(cur, order_id, "RETURN_REFUND", "BUYER", body.reason.lower(), order=order)

        respond_by = business_now(cur) + timedelta(seconds=RETURN_RESPONSE_S)
        cur.execute(
            """INSERT INTO ecommerce.returns (order_id, customer_id, shop_id, reason, status, respond_by)
               VALUES (%s, %s, %s, %s, 'REQUESTED', %s) RETURNING *""",
            (order_id, order["customer_id"], order["shop_id"], body.reason, respond_by),
        )
        ret = cur.fetchone()
        expected = 0
        for item_id, qty in wanted.items():
            refund = unit_refund(lines[item_id]["net_amount"], qty, lines[item_id]["quantity"])
            expected += refund
            cur.execute("INSERT INTO ecommerce.return_items (return_id, order_item_id, quantity, refund_amount) "
                        "VALUES (%s, %s, %s, %s)", (ret["id"], item_id, qty, refund))
        outbox.emit(cur, "return", ret["id"], "ReturnRequested",
                    {"return_id": ret["id"], "order_id": order_id, "shop_id": order["shop_id"], "reason": body.reason,
                     "units": sum(wanted.values()), "expected_refund": expected})
        return ret

    return run_command(idempotency_key, f"POST /orders/{order_id}/returns", body.model_dump(mode="json"), handler, 201)


def _seller_return(cur, return_id, shop_id) -> dict:
    ret = _lock_return(cur, return_id)
    auth.shop(ret["shop_id"], "return")
    if shop_id is not None and ret["shop_id"] != shop_id:
        raise conflict("not_your_return", "Return belongs to another shop")
    return ret


@router.post("/returns/{return_id}/approve")
def seller_approve(return_id: UUID, body: ShopRef, idempotency_key: str | None = Header(None)):
    def handler(cur):
        return approve(cur, _seller_return(cur, return_id, body.shop_id), "SELLER", "seller_approved")

    return run_command(idempotency_key, f"POST /returns/{return_id}/approve", body.model_dump(), handler)


@router.post("/returns/{return_id}/reject")
def seller_reject(return_id: UUID, body: ShopRef, idempotency_key: str | None = Header(None)):
    def handler(cur):
        return set_return_status(cur, _seller_return(cur, return_id, body.shop_id), "REJECTED", "SELLER", "seller_rejected")

    return run_command(idempotency_key, f"POST /returns/{return_id}/reject", body.model_dump(), handler)


@router.post("/returns/{return_id}/inspect")
def seller_inspect(return_id: UUID, body: Inspection, idempotency_key: str | None = Header(None)):
    def handler(cur):
        return refund_received(cur, _seller_return(cur, return_id, body.shop_id), body.conditions)

    return run_command(idempotency_key, f"POST /returns/{return_id}/inspect", body.model_dump(mode="json"), handler)


@router.post("/returns/{return_id}/dispute")
def buyer_dispute(return_id: UUID, body: BuyerRef, idempotency_key: str | None = Header(None)):
    def handler(cur):
        ret = _lock_return(cur, return_id)
        auth.customer(ret["customer_id"], "return")
        if body.customer_id is not None and ret["customer_id"] != body.customer_id:
            raise conflict("not_your_return", "Return belongs to another customer")
        return set_return_status(cur, ret, "DISPUTED", "BUYER", "buyer_disputed")

    return run_command(idempotency_key, f"POST /returns/{return_id}/dispute", body.model_dump(), handler)


@router.post("/admin/returns/{return_id}/resolve")
def platform_resolve(return_id: UUID, body: Resolution, idempotency_key: str | None = Header(None)):
    auth.require_scope("admin")

    def handler(cur):
        ret = _lock_return(cur, return_id)
        if ret["status"] != "DISPUTED":
            raise conflict("invalid_return_transition", f"Return is {ret['status']}, not DISPUTED")
        if body.decision == "REFUND":
            return approve(cur, ret, "PLATFORM", "dispute_won_by_buyer")
        close_rejected(cur, ret, "PLATFORM", "dispute_won_by_seller")
        return _lock_return(cur, return_id)

    return run_command(idempotency_key, f"POST /admin/returns/{return_id}/resolve", body.model_dump(), handler)


@router.get("/returns")
def list_returns(status: str | None = None, customer_id: UUID | None = None, shop_id: UUID | None = None,
                 random: bool = False, limit: int = Query(100, ge=1, le=5000)):
    customer_id, shop_id = auth.scope_listing(customer_id, shop_id)
    order = "RANDOM()" if random else "created_at DESC"
    with transaction() as cur:
        cur.execute(f"""SELECT * FROM ecommerce.returns WHERE (%(s)s IS NULL OR status = %(s)s)
                          AND (%(c)s::uuid IS NULL OR customer_id = %(c)s::uuid)
                          AND (%(h)s::uuid IS NULL OR shop_id = %(h)s::uuid)
                        ORDER BY {order} LIMIT %(l)s""",
                    {"s": status, "c": customer_id, "h": shop_id, "l": limit})
        return cur.fetchall()


@router.get("/returns/{return_id}")
def get_return(return_id: UUID):
    with transaction() as cur:
        cur.execute("SELECT * FROM ecommerce.returns WHERE id = %s", (return_id,))
        ret = cur.fetchone()
        if not ret:
            raise not_found("return", return_id)
        auth.read_party(ret["customer_id"], ret["shop_id"], "return")
        cur.execute("SELECT * FROM ecommerce.return_items WHERE return_id = %s", (return_id,))
        ret["items"] = cur.fetchall()
    return ret


@router.post("/orders/{order_id}/reviews", status_code=201)
def post_reviews(order_id: UUID, body: PostReviews, idempotency_key: str | None = Header(None)):
    def handler(cur):
        cur.execute("SELECT id, customer_id, shop_id, status FROM ecommerce.orders WHERE id = %s", (order_id,))
        order = cur.fetchone()
        if not order:
            raise not_found("order", order_id)
        auth.customer(order["customer_id"], "order")
        if body.customer_id is not None and order["customer_id"] != body.customer_id:
            raise conflict("not_your_order", "Order belongs to another customer")
        if order["status"] != "COMPLETED":
            raise conflict("not_reviewable", "Only completed orders can be reviewed")
        cur.execute("SELECT id, product_id FROM ecommerce.order_items WHERE order_id = %s", (order_id,))
        lines = {r["id"]: r for r in cur.fetchall()}
        posted = []
        try:
            cur.execute("SAVEPOINT reviews")
            for r in body.items:
                if r.order_item_id not in lines:
                    raise conflict("invalid_review_item", "Item is not part of this order")
                cur.execute(
                    """INSERT INTO ecommerce.product_reviews (order_item_id, order_id, product_id, shop_id, customer_id,
                                                               rating, comment)
                       VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                    (r.order_item_id, order_id, lines[r.order_item_id]["product_id"], order["shop_id"],
                     order["customer_id"], r.rating, r.comment),
                )
                posted.append({"review_id": cur.fetchone()["id"], "rating": r.rating})
        except psycopg2.errors.UniqueViolation:
            cur.execute("ROLLBACK TO SAVEPOINT reviews")
            raise conflict("already_reviewed", "An item was already reviewed")
        outbox.emit(cur, "review", order_id, "ReviewsPosted",
                    {"order_id": order_id, "shop_id": order["shop_id"], "reviews": posted})
        return {"reviews": posted}

    return run_command(idempotency_key, f"POST /orders/{order_id}/reviews", body.model_dump(mode="json"), handler, 201)

"""Seller center: onboarding, listings, prices, stock, shop vouchers and order fulfilment."""

import random
from datetime import timedelta
from typing import Literal
from uuid import UUID

import psycopg2
from fastapi import APIRouter, Header, Query
from pydantic import BaseModel, Field, model_validator

from .. import auth, outbox
from ..clock import business_now
from ..db import transaction
from ..domain import pricing, stock
from ..domain.orders import lock_order, transition
from ..errors import conflict, not_found
from ..idempotency import run_command
from ..settings import COMMISSION_BPS, REGIONS

router = APIRouter(tags=["seller"])
internal = APIRouter(tags=["internal"])                     # called by the identity service when a seller registers

CARRIERS = {"GHN": ("GHN", 30), "GHTK": ("S", 25), "SPX": ("SPXVN", 25), "J&T": ("JT", 15), "VTP": ("VTP", 5)}


class ShopIn(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    tier: Literal["MALL", "PREFERRED", "NORMAL"]
    province: str


class OnboardSeller(BaseModel):
    legal_name: str = Field(min_length=2, max_length=120)
    email: str = Field(pattern=r"^[^@\s]+@[^@\s]+$", max_length=80)
    shop: ShopIn


class SkuIn(BaseModel):
    sku_code: str = Field(min_length=3, max_length=60)
    variant_name: str = Field(min_length=1, max_length=80)
    list_price: int = Field(gt=0)
    price: int = Field(gt=0)
    weight_g: int = Field(gt=0, le=100_000)
    initial_stock: int = Field(ge=0, le=100_000)
    reorder_point: int = Field(10, ge=0)

    @model_validator(mode="after")
    def price_not_above_list(self):
        if self.price > self.list_price:
            raise ValueError("price must not exceed list_price")
        return self


class ListProduct(BaseModel):
    category_slug: str
    brand: str | None = None
    name: str = Field(min_length=2, max_length=200)
    description: str = Field("", max_length=2000)
    skus: list[SkuIn] = Field(min_length=1, max_length=20)


class Restock(BaseModel):
    quantity: int = Field(ge=1, le=100_000)


class PriceChange(BaseModel):
    price: int = Field(gt=0)
    list_price: int | None = Field(None, gt=0)
    reason: str = Field("seller_update", max_length=30)


class ShopVoucher(BaseModel):
    code: str = Field(pattern=r"^[A-Z0-9]{4,30}$")
    kind: Literal["FIXED", "PERCENT"]
    value: int = Field(gt=0)
    max_discount: int | None = Field(None, gt=0)
    min_spend: int = Field(0, ge=0)
    usage_limit: int = Field(gt=0)
    per_customer_limit: int = Field(1, gt=0)
    duration_s: int = Field(gt=0)                 # business seconds from now


class SellerAction(BaseModel):
    shop_id: UUID | None = None           # the caller's own shop; may be omitted with a seller token


class SellerCancel(SellerAction):
    reason: Literal["out_of_stock", "buyer_request", "pricing_error"] = "out_of_stock"


@internal.post("/internal/sellers", status_code=201)
@router.post("/sellers", status_code=201)
def onboard_seller(body: OnboardSeller, idempotency_key: str | None = Header(None)):
    auth.require_scope("sellers:provision")         # sellers register through the identity service
    if body.shop.province not in REGIONS:
        raise conflict("unknown_province", f"Unsupported province {body.shop.province}")

    def handler(cur):
        try:
            cur.execute("SAVEPOINT onboard")
            cur.execute("INSERT INTO ecommerce.sellers (legal_name, email) VALUES (%s, %s) RETURNING id",
                        (body.legal_name, body.email))
            seller_id = cur.fetchone()["id"]
            cur.execute(
                """INSERT INTO ecommerce.shops (seller_id, name, shop_tier, province, commission_rate_bps)
                   VALUES (%s, %s, %s, %s, %s) RETURNING *""",
                (seller_id, body.shop.name, body.shop.tier, body.shop.province, COMMISSION_BPS[body.shop.tier]),
            )
        except psycopg2.errors.UniqueViolation:
            cur.execute("ROLLBACK TO SAVEPOINT onboard")
            raise conflict("seller_exists", "Seller email or shop name already registered")
        shop = cur.fetchone()
        cur.execute("INSERT INTO ecommerce.shop_wallets (shop_id) VALUES (%s)", (shop["id"],))
        outbox.emit(cur, "shop", shop["id"], "SellerOnboarded",
                    {"seller_id": seller_id, "shop_id": shop["id"], "shop_name": shop["name"],
                     "tier": shop["shop_tier"], "province": shop["province"]})
        return shop

    return run_command(idempotency_key, "POST /sellers", body.model_dump(), handler, 201)


@router.post("/shops/{shop_id}/products", status_code=201)
def list_product(shop_id: UUID, body: ListProduct, idempotency_key: str | None = Header(None)):
    auth.shop(shop_id)

    def handler(cur):
        cur.execute("SELECT id, is_active FROM ecommerce.shops WHERE id = %s", (shop_id,))
        shop = cur.fetchone()
        if not shop:
            raise not_found("shop", shop_id)
        if not shop["is_active"]:
            raise conflict("shop_inactive", "Shop is not active")
        cur.execute("SELECT id FROM ecommerce.categories WHERE slug = %s", (body.category_slug,))
        category = cur.fetchone()
        if not category:
            raise not_found("category", body.category_slug)
        brand_id = None
        if body.brand:
            cur.execute("SELECT id FROM ecommerce.brands WHERE name = %s", (body.brand,))
            brand = cur.fetchone()
            if not brand:
                raise not_found("brand", body.brand)
            brand_id = brand["id"]

        cur.execute(
            """INSERT INTO ecommerce.products (shop_id, category_id, brand_id, name, description)
               VALUES (%s, %s, %s, %s, %s) RETURNING id, name""",
            (shop_id, category["id"], brand_id, body.name, body.description),
        )
        product = cur.fetchone()
        skus = []
        try:
            cur.execute("SAVEPOINT skus")
            for s in body.skus:
                cur.execute(
                    """INSERT INTO ecommerce.product_skus (product_id, sku_code, variant_name, list_price, price, weight_g)
                       VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
                    (product["id"], s.sku_code, s.variant_name, s.list_price, s.price, s.weight_g),
                )
                skus.append({"sku_id": cur.fetchone()["id"], **s.model_dump()})
        except psycopg2.errors.UniqueViolation:
            cur.execute("ROLLBACK TO SAVEPOINT skus")
            raise conflict("sku_code_taken", "A SKU code is already in use")
        for s in skus:
            cur.execute("INSERT INTO ecommerce.price_history (sku_id, old_price, new_price, reason) "
                        "VALUES (%s, NULL, %s, 'listing')", (s["sku_id"], s["price"]))
            stock.list_sku(cur, s["sku_id"], shop_id, s["initial_stock"], s["reorder_point"], product["id"])

        outbox.emit(cur, "product", product["id"], "ProductListed",
                    {"product_id": product["id"], "shop_id": shop_id, "category": body.category_slug,
                     "name": body.name, "skus": skus})
        return {"product_id": product["id"], "skus": skus}

    return run_command(idempotency_key, f"POST /shops/{shop_id}/products", body.model_dump(), handler, 201)


@router.post("/skus/{sku_id}/restock")
def restock(sku_id: UUID, body: Restock, idempotency_key: str | None = Header(None)):
    def handler(cur):
        cur.execute("SELECT sku_id, shop_id FROM ecommerce.inventory WHERE sku_id = %s", (sku_id,))
        inv = cur.fetchone()
        if not inv:
            raise not_found("sku", sku_id)
        auth.shop(inv["shop_id"], "sku")
        cur.execute("SELECT gen_random_uuid() AS id")
        restock_id = cur.fetchone()["id"]
        row = stock.move(cur, sku_id, "RESTOCK", body.quantity, "RESTOCK", restock_id)
        cur.execute("UPDATE ecommerce.inventory SET last_restocked_at = ecommerce.business_now() WHERE sku_id = %s",
                    (sku_id,))
        outbox.emit(cur, "inventory", sku_id, "InventoryRestocked",
                    {"sku_id": sku_id, "shop_id": row["shop_id"], "restock_id": restock_id,
                     "quantity": body.quantity, "on_hand_after": row["on_hand"]})
        return row

    return run_command(idempotency_key, f"POST /skus/{sku_id}/restock", body.model_dump(), handler)


@router.patch("/skus/{sku_id}/price")
def change_price(sku_id: UUID, body: PriceChange, idempotency_key: str | None = Header(None)):
    def handler(cur):
        cur.execute("""SELECT s.*, p.shop_id FROM ecommerce.product_skus s JOIN ecommerce.products p ON p.id = s.product_id
                       WHERE s.id = %s FOR UPDATE OF s""", (sku_id,))
        sku = cur.fetchone()
        if not sku:
            raise not_found("sku", sku_id)
        auth.shop(sku["shop_id"], "sku")
        list_price = max(body.list_price or sku["list_price"], body.price)
        if body.price == sku["price"] and list_price == sku["list_price"]:
            return sku
        cur.execute("UPDATE ecommerce.product_skus SET price = %s, list_price = %s WHERE id = %s RETURNING *",
                    (body.price, list_price, sku_id))
        updated = cur.fetchone()
        if body.price != sku["price"]:
            cur.execute("INSERT INTO ecommerce.price_history (sku_id, old_price, new_price, reason) VALUES (%s, %s, %s, %s)",
                        (sku_id, sku["price"], body.price, body.reason))
        outbox.emit(cur, "product", sku["product_id"], "SkuPriceChanged",
                    {"sku_id": sku_id, "old_price": sku["price"], "new_price": body.price, "list_price": list_price,
                     "reason": body.reason})
        return updated

    return run_command(idempotency_key, f"PATCH /skus/{sku_id}/price", body.model_dump(), handler)


@router.post("/shops/{shop_id}/vouchers", status_code=201)
def create_shop_voucher(shop_id: UUID, body: ShopVoucher, idempotency_key: str | None = Header(None)):
    auth.shop(shop_id)

    def handler(cur):
        cur.execute("SELECT id FROM ecommerce.shops WHERE id = %s", (shop_id,))
        if not cur.fetchone():
            raise not_found("shop", shop_id)
        return create_voucher(cur, "SHOP", shop_id, body)

    return run_command(idempotency_key, f"POST /shops/{shop_id}/vouchers", body.model_dump(), handler, 201)


def create_voucher(cur, scope: str, shop_id, body) -> dict:
    if body.kind == "PERCENT" and not 0 < body.value <= 10_000:
        raise conflict("invalid_voucher", "PERCENT value is in basis points (1–10000)")
    t = business_now(cur)
    try:
        cur.execute("SAVEPOINT voucher")
        cur.execute(
            """INSERT INTO ecommerce.vouchers (code, scope, shop_id, kind, value, max_discount, min_spend, usage_limit,
                                               per_customer_limit, starts_at, ends_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
            (body.code, scope, shop_id, body.kind, body.value, body.max_discount, body.min_spend, body.usage_limit,
             body.per_customer_limit, t, t + timedelta(seconds=body.duration_s)),
        )
    except psycopg2.errors.UniqueViolation:
        cur.execute("ROLLBACK TO SAVEPOINT voucher")
        raise conflict("voucher_code_taken", f"Voucher code {body.code} already exists")
    v = cur.fetchone()
    pricing.create_quota(cur, v)
    outbox.emit(cur, "promotion", v["id"], "VoucherCreated",
                {"voucher_id": v["id"], "code": v["code"], "scope": scope, "shop_id": shop_id, "kind": v["kind"],
                 "value": v["value"], "usage_limit": v["usage_limit"], "ends_at": v["ends_at"]})
    return v


def _seller_order(cur, order_id, shop_id) -> dict:
    order = lock_order(cur, order_id)
    auth.shop(order["shop_id"], "order")
    if shop_id is not None and order["shop_id"] != shop_id:
        raise conflict("not_your_order", "Order belongs to another shop")
    return order


def create_shipment(cur, order: dict, direction: str, from_province: str, to_province: str, return_id=None) -> dict:
    carrier = random.choices(list(CARRIERS), weights=[w for _, w in CARRIERS.values()])[0]
    tracking = f"{CARRIERS[carrier][0]}{random.randint(10**11, 10**12 - 1)}"
    cur.execute(
        """INSERT INTO ecommerce.shipments (order_id, return_id, direction, carrier, tracking_number, status,
                                            from_province, to_province)
           VALUES (%s, %s, %s, %s, %s, 'CREATED', %s, %s) RETURNING *""",
        (order["id"], return_id, direction, carrier, tracking, from_province, to_province),
    )
    s = cur.fetchone()
    outbox.emit(cur, "shipment", s["id"], "ShipmentCreated",
                {"shipment_id": s["id"], "order_id": order["id"], "return_id": return_id, "direction": direction,
                 "carrier": carrier, "tracking_number": tracking})
    return s


@router.post("/orders/{order_id}/pack")
def pack_order(order_id: UUID, body: SellerAction, idempotency_key: str | None = Header(None)):
    """Seller packs the parcel: order READY_TO_SHIP and a carrier booking with a tracking number."""
    def handler(cur):
        order = _seller_order(cur, order_id, body.shop_id)
        updated = transition(cur, order_id, "READY_TO_SHIP", "SELLER", order=order)
        cur.execute("SELECT province FROM ecommerce.shops WHERE id = %s", (order["shop_id"],))
        updated["shipment"] = create_shipment(cur, order, "FORWARD", cur.fetchone()["province"], order["ship_to_province"])
        return updated

    return run_command(idempotency_key, f"POST /orders/{order_id}/pack", body.model_dump(), handler)


@router.post("/orders/{order_id}/seller-cancel")
def seller_cancel(order_id: UUID, body: SellerCancel, idempotency_key: str | None = Header(None)):
    def handler(cur):
        order = _seller_order(cur, order_id, body.shop_id)
        return transition(cur, order_id, "CANCELLED", "SELLER", body.reason, order=order)

    return run_command(idempotency_key, f"POST /orders/{order_id}/seller-cancel", body.model_dump(), handler)


@router.get("/shops")
def list_shops(limit: int = Query(1000, ge=1, le=10000)):
    with transaction() as cur:
        # wallet = rolled-up balance + entries still pending; rating comes from the reviews themselves
        cur.execute(
            """SELECT s.*, w.balance + coalesce(p.amount, 0) AS wallet_balance,
                      coalesce(r.n, 0) AS rating_count, coalesce(r.total, 0) AS rating_sum
               FROM ecommerce.shops s
               JOIN ecommerce.shop_wallets w ON w.shop_id = s.id
               LEFT JOIN (SELECT shop_id, sum(amount) AS amount FROM ecommerce.wallet_pending GROUP BY 1) p
                   ON p.shop_id = s.id
               LEFT JOIN (SELECT shop_id, count(*) AS n, sum(rating) AS total FROM ecommerce.product_reviews GROUP BY 1) r
                   ON r.shop_id = s.id
               ORDER BY s.created_at LIMIT %s""",
            (limit,),
        )
        shops = cur.fetchall()
    p = auth.current()
    if not p.can_read_all():                 # public directory: a wallet is only for ops and the shop itself
        for row in shops:
            if str(row["id"]) not in p.shop_ids:
                row.pop("wallet_balance", None)
    return shops


@router.get("/skus")
def list_skus(available: bool = False, shop_id: UUID | None = None, random: bool = False,
              limit: int = Query(200, ge=1, le=20000)):
    order = "RANDOM()" if random else "s.sku_code"
    with transaction() as cur:
        cur.execute(
            f"""SELECT s.id, s.sku_code, s.variant_name, s.price, s.list_price, s.weight_g,
                       p.id AS product_id, p.name AS product_name, p.shop_id, c.slug AS category,
                       i.on_hand, i.reserved, i.available, i.reorder_point
                FROM ecommerce.product_skus s
                JOIN ecommerce.products p ON p.id = s.product_id
                JOIN ecommerce.categories c ON c.id = p.category_id
                JOIN ecommerce.inventory i ON i.sku_id = s.id
                WHERE (NOT %(available)s OR (s.is_active AND p.status = 'ACTIVE' AND i.available > 0))
                  AND (%(shop)s::uuid IS NULL OR p.shop_id = %(shop)s::uuid)
                ORDER BY {order} LIMIT %(limit)s""",
            {"available": available, "shop": shop_id, "limit": limit},
        )
        return cur.fetchall()


@router.get("/inventory/low-stock")
def low_stock(shop_id: UUID | None = None, limit: int = Query(20, ge=1, le=1000)):
    if not auth.current().can_read_all():
        shop_id = auth.own_shop_id(shop_id)
    with transaction() as cur:
        cur.execute(
            """SELECT sku_id, shop_id, on_hand, reserved, available, reorder_point
               FROM ecommerce.inventory WHERE available <= reorder_point
                 AND (%s::uuid IS NULL OR shop_id = %s::uuid)
               ORDER BY available, sku_id LIMIT %s""",
            (shop_id, shop_id, limit),
        )
        return cur.fetchall()

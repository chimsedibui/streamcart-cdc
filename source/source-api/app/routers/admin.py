"""Platform back office: master data, business clock, platform vouchers and sale campaigns."""

from datetime import timedelta
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Header
from pydantic import BaseModel, Field

from .. import auth, clock, outbox
from ..db import transaction
from ..errors import conflict, not_found
from ..idempotency import run_command
from .sellers import create_voucher

router = APIRouter(tags=["admin"])


class Category(BaseModel):
    slug: str = Field(pattern=r"^[a-z0-9-]{2,80}$")
    name: str = Field(min_length=2, max_length=80)
    parent_slug: str | None = None


class Brand(BaseModel):
    name: str = Field(min_length=1, max_length=80)


class ClockSpeed(BaseModel):
    speed: float = Field(gt=0, le=100_000)


class PlatformVoucher(BaseModel):
    code: str = Field(pattern=r"^[A-Z0-9]{4,30}$")
    kind: Literal["FIXED", "PERCENT", "FREESHIP"]
    value: int = Field(gt=0)
    max_discount: int | None = Field(None, gt=0)
    min_spend: int = Field(0, ge=0)
    usage_limit: int = Field(gt=0)
    per_customer_limit: int = Field(1, gt=0)
    duration_s: int = Field(gt=0)


class CampaignItem(BaseModel):
    sku_id: UUID
    sale_price: int = Field(gt=0)
    quota: int = Field(gt=0, le=100_000)


class Campaign(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    kind: Literal["FLASH_SALE", "MEGA_SALE"]
    starts_in_s: int = Field(0, ge=0)          # business seconds from now
    duration_s: int = Field(gt=0)
    items: list[CampaignItem] = Field(default_factory=list, max_length=500)


@router.post("/admin/categories")
def upsert_category(body: Category):
    auth.require_scope("admin")
    with transaction() as cur:
        parent_id = None
        if body.parent_slug:
            cur.execute("SELECT id FROM ecommerce.categories WHERE slug = %s", (body.parent_slug,))
            parent = cur.fetchone()
            if not parent:
                raise not_found("category", body.parent_slug)
            parent_id = parent["id"]
        cur.execute(
            """INSERT INTO ecommerce.categories (slug, name, parent_id) VALUES (%s, %s, %s)
               ON CONFLICT (slug) DO NOTHING RETURNING id, slug, name, parent_id""",
            (body.slug, body.name, parent_id),
        )
        created = cur.fetchone()
        if created:
            outbox.emit(cur, "catalog", created["id"], "CategoryCreated", dict(created))
            return created
        cur.execute("SELECT id, slug, name, parent_id FROM ecommerce.categories WHERE slug = %s", (body.slug,))
        return cur.fetchone()


@router.post("/admin/brands")
def upsert_brand(body: Brand):
    auth.require_scope("admin")
    with transaction() as cur:
        cur.execute("INSERT INTO ecommerce.brands (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id, name",
                    (body.name,))
        created = cur.fetchone()
        if created:
            outbox.emit(cur, "catalog", created["id"], "BrandCreated", dict(created))
            return created
        cur.execute("SELECT id, name FROM ecommerce.brands WHERE name = %s", (body.name,))
        return cur.fetchone()


@router.get("/categories")
def list_categories():
    with transaction() as cur:
        cur.execute("SELECT id, slug, name, parent_id FROM ecommerce.categories ORDER BY slug")
        return cur.fetchall()


@router.get("/brands")
def list_brands():
    with transaction() as cur:
        cur.execute("SELECT id, name FROM ecommerce.brands ORDER BY name")
        return cur.fetchall()


@router.get("/clock")
def get_clock():
    with transaction() as cur:
        return clock.read(cur)


@router.put("/admin/clock")
def set_clock(body: ClockSpeed):
    auth.require_scope("sim:clock")
    with transaction() as cur:
        return clock.set_speed(cur, body.speed)


@router.post("/admin/vouchers", status_code=201)
def create_platform_voucher(body: PlatformVoucher, idempotency_key: str | None = Header(None)):
    auth.require_scope("admin")

    def handler(cur):
        return create_voucher(cur, "PLATFORM", None, body)

    return run_command(idempotency_key, "POST /admin/vouchers", body.model_dump(), handler, 201)


@router.post("/admin/campaigns", status_code=201)
def create_campaign(body: Campaign, idempotency_key: str | None = Header(None)):
    auth.require_scope("admin")

    def handler(cur):
        t = clock.business_now(cur) + timedelta(seconds=body.starts_in_s)
        cur.execute("INSERT INTO ecommerce.campaigns (name, kind, starts_at, ends_at) VALUES (%s, %s, %s, %s) RETURNING *",
                    (body.name, body.kind, t, t + timedelta(seconds=body.duration_s)))
        c = cur.fetchone()
        if body.items:
            cur.execute("SELECT id, price FROM ecommerce.product_skus WHERE id = ANY(%s)",
                        ([i.sku_id for i in body.items],))
            prices = {r["id"]: r["price"] for r in cur.fetchall()}
            for i in body.items:
                if i.sku_id not in prices:
                    raise not_found("sku", i.sku_id)
                if i.sale_price >= prices[i.sku_id]:
                    raise conflict("invalid_sale_price", "Flash price must be below the current price",
                                   sku_id=str(i.sku_id))
                cur.execute("INSERT INTO ecommerce.flash_sale_items (campaign_id, sku_id, sale_price, quota) "
                            "VALUES (%s, %s, %s, %s)", (c["id"], i.sku_id, i.sale_price, i.quota))
        outbox.emit(cur, "promotion", c["id"], "CampaignScheduled",
                    {"campaign_id": c["id"], "name": c["name"], "kind": c["kind"], "starts_at": c["starts_at"],
                     "ends_at": c["ends_at"], "items": len(body.items)})
        return c

    return run_command(idempotency_key, "POST /admin/campaigns", body.model_dump(mode="json"), handler, 201)


@router.get("/vouchers")
def list_vouchers(active: bool = True):
    with transaction() as cur:
        cur.execute(
            """SELECT * FROM (
                   SELECT v.id, v.code, v.scope, v.shop_id, v.kind, v.value, v.max_discount, v.min_spend, v.usage_limit,
                          (SELECT coalesce(sum(q.used), 0) FROM ecommerce.voucher_quota_slots q WHERE q.voucher_id = v.id)
                              AS used_count,
                          v.per_customer_limit, v.starts_at, v.ends_at, v.created_at
                   FROM ecommerce.vouchers v) v
               WHERE NOT %(active)s OR (starts_at <= ecommerce.business_now() AND ends_at > ecommerce.business_now()
                                        AND used_count < usage_limit)
               ORDER BY created_at DESC LIMIT 2000""",
            {"active": active},
        )
        return cur.fetchall()


@router.get("/campaigns")
def list_campaigns(active: bool = True):
    with transaction() as cur:
        cur.execute(
            """SELECT c.*, count(f.id) AS items, coalesce(sum(f.sold), 0) AS sold, coalesce(sum(f.quota), 0) AS quota
               FROM ecommerce.campaigns c LEFT JOIN ecommerce.flash_sale_items f ON f.campaign_id = c.id
               WHERE NOT %(active)s OR (c.starts_at <= ecommerce.business_now() AND c.ends_at > ecommerce.business_now())
               GROUP BY c.id ORDER BY c.starts_at DESC LIMIT 200""",
            {"active": active},
        )
        return cur.fetchall()


@router.get("/campaigns/{campaign_id}/items")
def campaign_items(campaign_id: UUID):
    with transaction() as cur:
        cur.execute("SELECT * FROM ecommerce.flash_sale_items WHERE campaign_id = %s", (campaign_id,))
        return cur.fetchall()

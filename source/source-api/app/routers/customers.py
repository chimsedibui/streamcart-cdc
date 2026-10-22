from typing import Literal
from uuid import UUID

import psycopg2
from fastapi import APIRouter, Header, Query
from pydantic import BaseModel, Field

from .. import auth, outbox
from ..db import transaction
from ..errors import conflict, not_found
from ..idempotency import run_command
from ..settings import REGIONS

router = APIRouter(prefix="/customers", tags=["customers"])
internal = APIRouter(tags=["internal"])                     # called by the identity service when a buyer registers


class AddressIn(BaseModel):
    recipient_name: str = Field(min_length=2, max_length=80)
    phone: str = Field(min_length=9, max_length=15)
    street: str = Field(min_length=3, max_length=120)
    district: str = Field(min_length=2, max_length=60)
    province: str
    make_default: bool = False


class RegisterCustomer(BaseModel):
    full_name: str = Field(min_length=2, max_length=80)
    email: str = Field(pattern=r"^[^@\s]+@[^@\s]+$", max_length=80)
    phone: str = Field(min_length=9, max_length=15)
    segment: Literal["NEW", "REGULAR", "BARGAIN_HUNTER", "LOYAL", "BUSINESS"] = "NEW"
    registered_channel: Literal["APP", "WEB"] = "APP"
    address: AddressIn


def _add_address(cur, customer_id, a: AddressIn, default: bool) -> dict:
    if a.province not in REGIONS:
        raise conflict("unknown_province", f"Unsupported province {a.province}")
    if default:
        cur.execute("UPDATE ecommerce.customer_addresses SET is_default = FALSE WHERE customer_id = %s AND is_default",
                    (customer_id,))
    cur.execute(
        """INSERT INTO ecommerce.customer_addresses
           (customer_id, recipient_name, phone, street, district, province, is_default)
           VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING *""",
        (customer_id, a.recipient_name, a.phone, a.street, a.district, a.province, default),
    )
    address = cur.fetchone()
    outbox.emit(cur, "customer", customer_id, "AddressAdded",
                {"customer_id": customer_id, "address_id": address["id"], "province": a.province,
                 "is_default": default})
    return address


@internal.post("/internal/customers", status_code=201)
@router.post("", status_code=201)
def register_customer(body: RegisterCustomer, idempotency_key: str | None = Header(None)):
    auth.require_scope("customers:provision")       # buyers register through the identity service

    def handler(cur):
        try:
            cur.execute("SAVEPOINT register")
            cur.execute(
                """INSERT INTO ecommerce.customers (full_name, email, phone, segment, registered_channel)
                   VALUES (%s, %s, %s, %s, %s) RETURNING *""",
                (body.full_name, body.email, body.phone, body.segment, body.registered_channel),
            )
        except psycopg2.errors.UniqueViolation:
            cur.execute("ROLLBACK TO SAVEPOINT register")
            raise conflict("email_taken", f"{body.email} is already registered")
        customer = cur.fetchone()
        outbox.emit(cur, "customer", customer["id"], "CustomerRegistered",
                    {"customer_id": customer["id"], "segment": body.segment, "channel": body.registered_channel})
        customer["default_address"] = _add_address(cur, customer["id"], body.address, default=True)
        return customer

    return run_command(idempotency_key, "POST /customers", body.model_dump(), handler, 201)


@router.post("/{customer_id}/addresses", status_code=201)
def add_address(customer_id: UUID, body: AddressIn, idempotency_key: str | None = Header(None)):
    auth.customer(customer_id)

    def handler(cur):
        cur.execute("SELECT id FROM ecommerce.customers WHERE id = %s FOR UPDATE", (customer_id,))
        if not cur.fetchone():
            raise not_found("customer", customer_id)
        return _add_address(cur, customer_id, body, default=body.make_default)

    return run_command(idempotency_key, f"POST /customers/{customer_id}/addresses", body.model_dump(), handler, 201)


@router.post("/{customer_id}/deactivate")
def deactivate_customer(customer_id: UUID, idempotency_key: str | None = Header(None)):
    auth.require_scope("admin")

    def handler(cur):
        cur.execute("SELECT id, is_active FROM ecommerce.customers WHERE id = %s FOR UPDATE", (customer_id,))
        customer = cur.fetchone()
        if not customer:
            raise not_found("customer", customer_id)
        if not customer["is_active"]:
            raise conflict("already_inactive", "Customer is already deactivated")
        cur.execute("UPDATE ecommerce.customers SET is_active = FALSE WHERE id = %s RETURNING id, is_active",
                    (customer_id,))
        updated = cur.fetchone()
        outbox.emit(cur, "customer", customer_id, "CustomerDeactivated", {"customer_id": customer_id})
        return updated

    return run_command(idempotency_key, f"POST /customers/{customer_id}/deactivate", {}, handler)


@router.get("")
def list_customers(active: bool | None = None, segment: str | None = None, random: bool = False,
                   limit: int = Query(100, ge=1, le=5000)):
    auth.require_scope("ops:read")
    order = "RANDOM()" if random else "c.created_at DESC"
    with transaction() as cur:
        cur.execute(
            f"""SELECT c.id, c.full_name, c.segment, c.tier, c.total_spent, c.is_active, a.id AS default_address_id
                FROM ecommerce.customers c
                LEFT JOIN ecommerce.customer_addresses a ON a.customer_id = c.id AND a.is_default
                WHERE (%(active)s IS NULL OR c.is_active = %(active)s)
                  AND (%(segment)s IS NULL OR c.segment = %(segment)s)
                ORDER BY {order} LIMIT %(limit)s""",
            {"active": active, "segment": segment, "limit": limit},
        )
        rows = cur.fetchall()
    if auth.mask_pii():
        for r in rows:
            r["full_name"] = auth.mask(r["full_name"])
    return rows


@router.get("/{customer_id}")
def get_customer(customer_id: UUID):
    auth.read_customer(customer_id)
    with transaction() as cur:
        cur.execute("SELECT * FROM ecommerce.customers WHERE id = %s", (customer_id,))
        customer = cur.fetchone()
        if not customer:
            raise not_found("customer", customer_id)
        cur.execute("SELECT * FROM ecommerce.customer_addresses WHERE customer_id = %s ORDER BY created_at",
                    (customer_id,))
        customer["addresses"] = cur.fetchall()
    if auth.mask_pii():
        for row in (customer, *customer["addresses"]):
            for col in ("full_name", "email", "phone", "recipient_name", "street"):
                if col in row:
                    row[col] = auth.mask(row[col])
    return customer

"""Shopping cart: one ACTIVE cart per customer; removing a line is a hard DELETE."""

from uuid import UUID

from fastapi import APIRouter, Header, Query
from pydantic import BaseModel, Field

from .. import auth, outbox
from ..db import transaction
from ..errors import conflict, not_found
from ..idempotency import run_command

router = APIRouter(prefix="/carts", tags=["cart"])


class AddItem(BaseModel):
    customer_id: UUID | None = None       # the caller's own; may be omitted with a customer token
    sku_id: UUID
    quantity: int = Field(1, ge=1, le=100)


class UpdateItem(BaseModel):
    quantity: int = Field(ge=1, le=100)


def active_cart(cur, customer_id, create: bool) -> dict | None:
    """Lock (and optionally create) the customer's ACTIVE cart."""
    # A concurrent checkout can close the existing cart between the skipped INSERT and the lock; the lock then
    # finds no ACTIVE row, and the next INSERT succeeds.
    for _ in range(3):
        if create:
            cur.execute(
                """INSERT INTO ecommerce.carts (customer_id) VALUES (%s)
                   ON CONFLICT (customer_id) WHERE status = 'ACTIVE' DO NOTHING RETURNING id""",
                (customer_id,),
            )
            created = cur.fetchone()
            if created:
                outbox.emit(cur, "cart", created["id"], "CartCreated", {"cart_id": created["id"], "customer_id": customer_id})
        cur.execute("SELECT * FROM ecommerce.carts WHERE customer_id = %s AND status = 'ACTIVE' FOR UPDATE",
                    (customer_id,))
        cart = cur.fetchone()
        if cart or not create:
            return cart
    raise RuntimeError(f"no active cart for customer {customer_id} after 3 attempts")


def _touch(cur, cart_id):
    cur.execute("UPDATE ecommerce.carts SET updated_at = clock_timestamp() WHERE id = %s", (cart_id,))


def _locked_item(cur, item_id) -> dict:
    cur.execute(
        """SELECT ci.*, c.customer_id FROM ecommerce.cart_items ci
           JOIN ecommerce.carts c ON c.id = ci.cart_id
           WHERE ci.id = %s AND c.status = 'ACTIVE' FOR UPDATE OF c""",
        (item_id,),
    )
    item = cur.fetchone()
    if item:
        auth.customer(item["customer_id"], "cart_item")
    if not item:
        raise not_found("cart_item", item_id)
    return item


@router.post("/items", status_code=201)
def add_item(body: AddItem, idempotency_key: str | None = Header(None)):
    body.customer_id = auth.own_customer_id(body.customer_id)

    def handler(cur):
        cur.execute("SELECT is_active FROM ecommerce.customers WHERE id = %s", (body.customer_id,))
        customer = cur.fetchone()
        if not customer:
            raise not_found("customer", body.customer_id)
        if not customer["is_active"]:
            raise conflict("customer_inactive", "Customer account is deactivated")
        cur.execute(
            """SELECT s.is_active AND p.status = 'ACTIVE' AS sellable
               FROM ecommerce.product_skus s JOIN ecommerce.products p ON p.id = s.product_id WHERE s.id = %s""",
            (body.sku_id,),
        )
        sku = cur.fetchone()
        if not sku:
            raise not_found("sku", body.sku_id)
        if not sku["sellable"]:
            raise conflict("sku_unavailable", "SKU is not for sale")

        cart = active_cart(cur, body.customer_id, create=True)
        cur.execute(
            """INSERT INTO ecommerce.cart_items (cart_id, sku_id, quantity) VALUES (%s, %s, %s)
               ON CONFLICT (cart_id, sku_id) DO UPDATE SET quantity = LEAST(ecommerce.cart_items.quantity + EXCLUDED.quantity, 100)
               RETURNING *""",
            (cart["id"], body.sku_id, body.quantity),
        )
        item = cur.fetchone()
        _touch(cur, cart["id"])
        outbox.emit(cur, "cart", cart["id"], "CartItemAdded",
                    {"cart_id": cart["id"], "customer_id": body.customer_id, "sku_id": body.sku_id,
                     "quantity": body.quantity, "line_quantity": item["quantity"]})
        return item

    return run_command(idempotency_key, "POST /carts/items", body.model_dump(), handler, 201)


@router.patch("/items/{item_id}")
def update_item(item_id: UUID, body: UpdateItem, idempotency_key: str | None = Header(None)):
    def handler(cur):
        item = _locked_item(cur, item_id)
        cur.execute("UPDATE ecommerce.cart_items SET quantity = %s WHERE id = %s RETURNING *", (body.quantity, item_id))
        updated = cur.fetchone()
        _touch(cur, item["cart_id"])
        outbox.emit(cur, "cart", item["cart_id"], "CartItemUpdated",
                    {"cart_id": item["cart_id"], "sku_id": item["sku_id"], "quantity": body.quantity})
        return updated

    return run_command(idempotency_key, f"PATCH /carts/items/{item_id}", body.model_dump(), handler)


@router.delete("/items/{item_id}")
def remove_item(item_id: UUID, idempotency_key: str | None = Header(None)):
    def handler(cur):
        item = _locked_item(cur, item_id)
        cur.execute("DELETE FROM ecommerce.cart_items WHERE id = %s", (item_id,))
        _touch(cur, item["cart_id"])
        outbox.emit(cur, "cart", item["cart_id"], "CartItemRemoved",
                    {"cart_id": item["cart_id"], "sku_id": item["sku_id"]})
        return {"removed": item_id}

    return run_command(idempotency_key, f"DELETE /carts/items/{item_id}", {}, handler)


@router.get("")
def list_carts(status: str = "ACTIVE", random: bool = False, limit: int = Query(50, ge=1, le=1000)):
    auth.require_scope("ops:read")
    order = "RANDOM()" if random else "c.updated_at DESC"
    with transaction() as cur:
        cur.execute(
            f"""SELECT c.id, c.customer_id, c.status, c.updated_at, count(ci.id) AS lines
                FROM ecommerce.carts c LEFT JOIN ecommerce.cart_items ci ON ci.cart_id = c.id
                WHERE c.status = %s GROUP BY c.id HAVING count(ci.id) > 0
                ORDER BY {order} LIMIT %s""",
            (status, limit),
        )
        return cur.fetchall()


@router.get("/active")
def get_active_cart(customer_id: UUID | None = None):
    customer_id = auth.read_customer_id(customer_id)
    with transaction() as cur:
        cur.execute("SELECT * FROM ecommerce.carts WHERE customer_id = %s AND status = 'ACTIVE'", (customer_id,))
        cart = cur.fetchone()
        if not cart:
            raise not_found("cart", customer_id)
        cur.execute(
            """SELECT ci.id, ci.sku_id, ci.quantity, s.price, s.variant_name, p.name AS product_name
               FROM ecommerce.cart_items ci
               JOIN ecommerce.product_skus s ON s.id = ci.sku_id
               JOIN ecommerce.products p ON p.id = s.product_id
               WHERE ci.cart_id = %s ORDER BY ci.created_at""",
            (cart["id"],),
        )
        cart["items"] = cur.fetchall()
    return cart

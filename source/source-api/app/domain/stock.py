"""The only code that changes stock: every change updates `inventory` and appends one ledger row.

available is always on_hand − reserved (enforced by a CHECK on the table).
"""

from ..errors import conflict

# movement type -> (on_hand sign, reserved sign) applied to a positive quantity
MOVES = {
    "LISTED":           (+1, 0),
    "RESTOCK":          (+1, 0),
    "RESERVE":          (0, +1),    # checkout holds stock
    "RELEASE":          (0, -1),    # cancelled before shipping
    "SHIP":             (-1, -1),   # parcel leaves the warehouse
    "RETURN_TO_SENDER": (+1, 0),    # undeliverable parcel back on the shelf
    "CUSTOMER_RETURN":  (+1, 0),    # returned item inspected as sellable
}


def move(cur, sku_id, movement: str, qty: int, ref_type: str, ref_id) -> dict:
    oh, rs = (sign * qty for sign in MOVES[movement])
    cur.execute(
        """UPDATE ecommerce.inventory
           SET on_hand = on_hand + %(oh)s, reserved = reserved + %(rs)s, available = available + %(oh)s - %(rs)s
           WHERE sku_id = %(sku)s AND available + %(oh)s - %(rs)s >= 0
           RETURNING sku_id, shop_id, on_hand, reserved, available""",
        {"oh": oh, "rs": rs, "sku": sku_id},
    )
    row = cur.fetchone()
    if not row:
        raise conflict("out_of_stock", "Not enough stock", sku_id=str(sku_id))
    cur.execute(
        """INSERT INTO ecommerce.inventory_movements
           (sku_id, movement_type, on_hand_delta, reserved_delta, on_hand_after, reserved_after, ref_type, ref_id)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (sku_id, movement, oh, rs, row["on_hand"], row["reserved"], ref_type, ref_id),
    )
    return row


def list_sku(cur, sku_id, shop_id, initial: int, reorder_point: int, product_id):
    """New SKU: stock row at zero, then the initial quantity as a LISTED movement."""
    cur.execute(
        """INSERT INTO ecommerce.inventory (sku_id, shop_id, on_hand, reserved, available, reorder_point, last_restocked_at)
           VALUES (%s, %s, 0, 0, 0, %s, ecommerce.business_now())""",
        (sku_id, shop_id, reorder_point),
    )
    if initial:
        move(cur, sku_id, "LISTED", initial, "LISTING", product_id)

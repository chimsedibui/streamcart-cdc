"""Carrier integration: tracking callbacks for forward parcels and customer-return parcels.

Carriers redeliver callbacks (deduplicated by carrier_event_id), send them late and out of order (an event
older than the parcel's latest applied one is stored but not applied), and may skip scan events (a parcel can
be delivered straight from a hub). Applying an event moves the order — or, for a return parcel, the return.
"""

from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, Header, Query
from pydantic import BaseModel, Field

from .. import auth, outbox
from ..db import transaction
from ..domain.orders import transition
from ..domain.returns import set_return_status
from ..errors import conflict, not_found
from ..idempotency import run_command

router = APIRouter(tags=["logistics"])

CarrierEvent = Literal["PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "DELIVERY_FAILED",
                       "RETURNED_TO_SENDER"]

# event -> shipment statuses it may follow (FORWARD); scan events in between may be missing
FOLLOWS = {
    "PICKED_UP":          {"CREATED"},
    "IN_TRANSIT":         {"PICKED_UP", "IN_TRANSIT"},
    "OUT_FOR_DELIVERY":   {"PICKED_UP", "IN_TRANSIT", "DELIVERY_FAILED"},
    "DELIVERED":          {"PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY"},
    "DELIVERY_FAILED":    {"OUT_FOR_DELIVERY"},
    "RETURNED_TO_SENDER": {"DELIVERY_FAILED"},
}
RETURN_PARCEL_EVENTS = {"PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED"}


class TrackingEvent(BaseModel):
    tracking_number: str
    carrier_event_id: str = Field(min_length=4, max_length=60)
    event: CarrierEvent
    event_time: datetime
    location: str | None = Field(None, max_length=40)


@router.post("/logistics/webhook")
def tracking_webhook(body: TrackingEvent, idempotency_key: str | None = Header(None),
                     partner: str | None = Depends(auth.verify_webhook)):
    """Carrier callback, signed by the carrier (X-Partner, X-Signature); a carrier reports only its own parcels."""
    def handler(cur):
        cur.execute("SELECT * FROM ecommerce.shipments WHERE tracking_number = %s FOR UPDATE", (body.tracking_number,))
        s = cur.fetchone()
        if not s:
            raise not_found("shipment", body.tracking_number)
        auth.partner_is(s["carrier"], partner, "shipment")
        cur.execute("SELECT 1 FROM ecommerce.shipment_events WHERE carrier_event_id = %s", (body.carrier_event_id,))
        if cur.fetchone():
            return {"shipment": s, "result": "duplicate_ignored"}

        def record(applied: bool):
            cur.execute(
                """INSERT INTO ecommerce.shipment_events (shipment_id, carrier_event_id, event_type, event_time, location, applied)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (s["id"], body.carrier_event_id, body.event, body.event_time, body.location, applied),
            )

        if s["last_event_time"] and body.event_time <= s["last_event_time"]:
            record(False)
            return {"shipment": s, "result": "stale_ignored"}
        if s["direction"] == "RETURN" and body.event not in RETURN_PARCEL_EVENTS:
            raise conflict("invalid_shipment_event", f"{body.event} is not supported for return parcels")
        if s["status"] not in FOLLOWS[body.event]:
            raise conflict("invalid_shipment_event", f"{body.event} cannot follow {s['status']}",
                           current=s["status"], event=body.event)

        changes = {"status": body.event, "last_event_time": body.event_time}
        if body.event == "PICKED_UP":
            changes["picked_up_at"] = body.event_time
        elif body.event == "DELIVERED":
            changes["delivered_at"] = body.event_time
        elif body.event == "DELIVERY_FAILED":
            changes["attempts"] = s["attempts"] + 1

        if s["direction"] == "FORWARD":
            _move_order(cur, s, body.event)
        else:
            _move_return(cur, s, body.event)

        sets = ", ".join(f"{k} = %({k})s" for k in changes)
        cur.execute(f"UPDATE ecommerce.shipments SET {sets} WHERE id = %(id)s RETURNING *", {**changes, "id": s["id"]})
        updated = cur.fetchone()
        record(True)
        outbox.emit(cur, "shipment", s["id"], "ShipmentTracked",
                    {"shipment_id": s["id"], "order_id": s["order_id"], "direction": s["direction"],
                     "event": body.event, "event_time": body.event_time, "location": body.location})
        return {"shipment": updated, "result": "applied"}

    return run_command(idempotency_key, "POST /logistics/webhook", body.model_dump(mode="json"), handler)


def _move_order(cur, s: dict, event: str):
    cur.execute("SELECT status FROM ecommerce.orders WHERE id = %s", (s["order_id"],))
    status = cur.fetchone()["status"]
    target = {
        "PICKED_UP": "SHIPPED",
        "DELIVERED": "DELIVERED",
        "DELIVERY_FAILED": "DELIVERY_FAILED",
        "RETURNED_TO_SENDER": "RETURNED_TO_SELLER",
        "OUT_FOR_DELIVERY": "SHIPPED" if status == "DELIVERY_FAILED" else None,   # redelivery attempt
    }.get(event)
    if target:
        transition(cur, s["order_id"], target, "LOGISTICS", event.lower())


def _move_return(cur, s: dict, event: str):
    cur.execute("SELECT * FROM ecommerce.returns WHERE id = %s FOR UPDATE", (s["return_id"],))
    ret = cur.fetchone()
    if event == "PICKED_UP":
        set_return_status(cur, ret, "RETURNING", "LOGISTICS", "parcel_picked_up")
    elif event == "DELIVERED":
        set_return_status(cur, ret, "RECEIVED", "LOGISTICS", "parcel_delivered_to_seller")


@router.get("/shipments")
def list_shipments(direction: Literal["FORWARD", "RETURN"] = "FORWARD", status: str | None = None,
                   random: bool = False, limit: int = Query(100, ge=1, le=5000)):
    auth.require_scope("ops:read")
    order = "RANDOM()" if random else "s.updated_at DESC"
    with transaction() as cur:
        cur.execute(
            f"""SELECT s.id, s.order_id, s.return_id, s.carrier, s.tracking_number, s.status, s.attempts,
                       s.from_province, s.to_province, s.last_event_time, s.created_at, o.order_number
                FROM ecommerce.shipments s JOIN ecommerce.orders o ON o.id = s.order_id
                WHERE s.direction = %(dir)s AND (%(status)s IS NULL OR s.status = %(status)s)
                ORDER BY {order} LIMIT %(limit)s""",
            {"dir": direction, "status": status, "limit": limit},
        )
        return cur.fetchall()


@router.get("/shipments/{tracking_number}")
def get_shipment(tracking_number: str):
    with transaction() as cur:
        cur.execute("""SELECT s.*, o.customer_id, o.shop_id FROM ecommerce.shipments s
                       JOIN ecommerce.orders o ON o.id = s.order_id WHERE s.tracking_number = %s""", (tracking_number,))
        s = cur.fetchone()
        if not s:
            raise not_found("shipment", tracking_number)
        auth.read_party(s["customer_id"], s["shop_id"], "shipment")
        cur.execute("SELECT carrier_event_id, event_type, event_time, location, applied, created_at "
                    "FROM ecommerce.shipment_events WHERE shipment_id = %s ORDER BY event_time, id", (s["id"],))
        s["events"] = cur.fetchall()
    return s


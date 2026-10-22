"""Domain events, written in the same transaction as the change they describe.

The row is deleted again before commit. Debezium reads the insert from the WAL (the Outbox Event Router turns it
into a message on events.<aggregatetype> and ignores the delete), so the table never grows.
"""

import json


def emit(cur, aggregate_type: str, aggregate_id, event_type: str, payload: dict):
    # business_now() reads the wall clock at call time (not the transaction start), so events of one
    # aggregate are ordered by when they happened, even if their transactions began out of order
    cur.execute(
        """INSERT INTO ecommerce.outbox_events (aggregatetype, aggregateid, type, payload, created_at)
           VALUES (%s, %s, %s, %s, ecommerce.business_now()) RETURNING id""",
        (aggregate_type, str(aggregate_id), event_type, json.dumps(payload, default=str)),
    )
    cur.execute("DELETE FROM ecommerce.outbox_events WHERE id = %s", (cur.fetchone()["id"],))

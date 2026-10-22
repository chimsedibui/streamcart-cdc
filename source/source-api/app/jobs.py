"""Time-based platform rules, run as SYSTEM actor on business time (ecommerce.business_now()).

Each item is its own transaction and re-checks state under lock, so running the scheduler in several API
workers at once is safe (losers get a DomainError and skip).
"""

import logging
import threading

import psycopg2

from . import outbox
from .db import transaction
from .domain import settlement
from .domain.orders import lock_order, transition
from .domain.returns import close_rejected
from .errors import DomainError
from .routers.checkouts import close_checkout
from .routers.returns import approve
from .settings import (AUTO_COMPLETE_S, CART_ABANDON_S, DISPUTE_WINDOW_S, IDEMPOTENCY_TTL_S, JOB_INTERVAL_S,
                       PAYOUT_INTERVAL_S)

log = logging.getLogger("jobs")
NOW = "ecommerce.business_now()"


def _candidates(sql: str, params=()) -> list:
    with transaction() as cur:
        cur.execute(sql, params)
        return [r["id"] for r in cur.fetchall()]


def _each(ids, fn):
    for item_id in ids:
        try:
            with transaction() as cur:
                fn(cur, item_id)
        except (DomainError, psycopg2.errors.DeadlockDetected):
            pass  # state moved on (another worker or a concurrent command got there first)
        except Exception:
            log.exception("job item %s failed", item_id)


def expire_unpaid_checkouts():
    def expire(cur, checkout_id):
        cur.execute(f"SELECT *, pay_by < {NOW} AS due FROM ecommerce.checkouts WHERE id = %s FOR UPDATE", (checkout_id,))
        co = cur.fetchone()
        if co["status"] == "PENDING_PAYMENT" and co["due"]:
            close_checkout(cur, co, "EXPIRED", "SYSTEM", "payment_timeout")

    _each(_candidates(f"SELECT id FROM ecommerce.checkouts WHERE status = 'PENDING_PAYMENT' AND pay_by < {NOW} "
                      "ORDER BY pay_by LIMIT 500"), expire)


def cancel_sla_breaches():
    def breach(cur, order_id):
        order = lock_order(cur, order_id)
        if order["status"] != "TO_SHIP":
            return
        transition(cur, order_id, "CANCELLED", "SYSTEM", "seller_sla_breach", order=order)
        cur.execute("UPDATE ecommerce.shops SET penalty_points = penalty_points + 1 WHERE id = %s "
                    "RETURNING penalty_points", (order["shop_id"],))
        outbox.emit(cur, "shop", order["shop_id"], "ShopPenalized",
                    {"shop_id": order["shop_id"], "order_id": order_id, "reason": "seller_sla_breach",
                     "penalty_points": cur.fetchone()["penalty_points"]})

    _each(_candidates(f"SELECT id FROM ecommerce.orders WHERE status = 'TO_SHIP' AND confirm_by < {NOW} "
                      "ORDER BY confirm_by LIMIT 500"), breach)


def auto_complete_delivered():
    def complete(cur, order_id):
        # re-check under the lock: the buyer may have opened a return since the candidate was picked, and
        # RETURN_REFUND → COMPLETED is also a legal SYSTEM move (when a return ends)
        order = lock_order(cur, order_id)
        if order["status"] != "DELIVERED":
            return
        transition(cur, order_id, "COMPLETED", "SYSTEM", "auto_complete", order=order)

    _each(_candidates(f"SELECT id FROM ecommerce.orders WHERE status = 'DELIVERED' "
                      f"AND delivered_at < {NOW} - make_interval(secs => %s) ORDER BY delivered_at LIMIT 500",
                      (AUTO_COMPLETE_S,)), complete)


def abandon_idle_carts():
    def abandon(cur, cart_id):
        cur.execute(f"UPDATE ecommerce.carts SET status = 'ABANDONED' WHERE id = %s AND status = 'ACTIVE' "
                    f"AND updated_at < {NOW} - make_interval(secs => %s) RETURNING customer_id",
                    (cart_id, CART_ABANDON_S))
        row = cur.fetchone()
        if row:
            outbox.emit(cur, "cart", cart_id, "CartAbandoned", {"cart_id": cart_id, "customer_id": row["customer_id"]})

    _each(_candidates(f"""SELECT c.id FROM ecommerce.carts c
                          WHERE c.status = 'ACTIVE' AND c.updated_at < {NOW} - make_interval(secs => %s)
                            AND EXISTS (SELECT 1 FROM ecommerce.cart_items ci WHERE ci.cart_id = c.id)
                          LIMIT 500""", (CART_ABANDON_S,)), abandon)


def auto_approve_unanswered_returns():
    def auto(cur, return_id):
        cur.execute(f"SELECT *, respond_by < {NOW} AS due FROM ecommerce.returns WHERE id = %s FOR UPDATE", (return_id,))
        ret = cur.fetchone()
        if ret["status"] == "REQUESTED" and ret["due"]:
            approve(cur, ret, "SYSTEM", "seller_no_response")

    _each(_candidates(f"SELECT id FROM ecommerce.returns WHERE status = 'REQUESTED' AND respond_by < {NOW} LIMIT 500"),
          auto)


def close_undisputed_rejections():
    def close(cur, return_id):
        cur.execute(f"SELECT *, updated_at < {NOW} - make_interval(secs => %s) AS due FROM ecommerce.returns "
                    "WHERE id = %s FOR UPDATE", (DISPUTE_WINDOW_S, return_id))
        ret = cur.fetchone()
        if ret["status"] == "REJECTED" and ret["due"]:
            close_rejected(cur, ret, "SYSTEM", "no_dispute")

    _each(_candidates(f"SELECT id FROM ecommerce.returns WHERE status = 'REJECTED' "
                      f"AND updated_at < {NOW} - make_interval(secs => %s) LIMIT 500", (DISPUTE_WINDOW_S,)), close)


def pay_out_sellers():
    def pay(cur, shop_id):
        settlement.payout(cur, shop_id)

    _each(_candidates(f"""SELECT s.id FROM ecommerce.shops s JOIN ecommerce.shop_wallets w ON w.shop_id = s.id
                          WHERE w.balance + coalesce((SELECT sum(amount) FROM ecommerce.wallet_pending p
                                                      WHERE p.shop_id = s.id), 0) > 0
                            AND NOT EXISTS (SELECT 1 FROM ecommerce.seller_payouts p WHERE p.shop_id = s.id
                                            AND p.created_at > {NOW} - make_interval(secs => %s))
                            AND EXISTS (SELECT 1 FROM ecommerce.seller_ledger_entries e WHERE e.shop_id = s.id
                                        AND e.created_at < {NOW} - make_interval(secs => %s))
                          LIMIT 500""", (PAYOUT_INTERVAL_S, PAYOUT_INTERVAL_S)), pay)


def roll_up_wallets():
    """Settled entries reach shop_wallets.balance within one scheduler run."""
    while True:
        with transaction() as cur:
            if not settlement.roll_up(cur):
                return


def purge_idempotency_keys():
    """Stored responses only matter while a client may still retry (wall clock)."""
    with transaction() as cur:
        cur.execute("""DELETE FROM ecommerce.idempotency_keys WHERE key IN (
                           SELECT key FROM ecommerce.idempotency_keys
                           WHERE created_at < now() - make_interval(secs => %s) LIMIT 5000)""", (IDEMPOTENCY_TTL_S,))


JOBS = [expire_unpaid_checkouts, cancel_sla_breaches, auto_complete_delivered, abandon_idle_carts,
        auto_approve_unanswered_returns, close_undisputed_rejections, roll_up_wallets, pay_out_sellers,
        purge_idempotency_keys]


def start_scheduler(stop: threading.Event):
    def loop():
        while not stop.wait(JOB_INTERVAL_S):
            for job in JOBS:
                try:
                    job()
                except Exception:
                    log.exception("job %s failed", job.__name__)

    threading.Thread(target=loop, name="jobs", daemon=True).start()

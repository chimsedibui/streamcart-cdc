"""Seller wallet: an append-only ledger, summed into shop_wallets.balance in batches.

Completing an order only inserts ledger entries (plus a wallet_pending row each), so it never waits on the
shop's wallet row; roll_up() moves pending entries into the balance. Lock order: wallet_pending → shop_wallets.

When an order completes the seller earns what the buyer paid for the goods it kept, minus the seller-funded
shop voucher (platform vouchers and free shipping are paid by the platform), and is charged commission and a
payment fee on that amount. Payouts empty the wallet.
"""

from .. import outbox
from ..settings import PAYMENT_FEE_BPS


def post(cur, shop_id, entry_type: str, amount: int, order_id=None, payout_id=None, applied=False) -> None:
    """Append a ledger entry. applied=True: the caller holds the wallet row and adds the amount itself."""
    cur.execute(
        """INSERT INTO ecommerce.seller_ledger_entries (shop_id, order_id, payout_id, entry_type, amount)
           VALUES (%s, %s, %s, %s, %s) RETURNING id""",
        (shop_id, order_id, payout_id, entry_type, amount),
    )
    if not applied:
        cur.execute("INSERT INTO ecommerce.wallet_pending (entry_id, shop_id, amount) VALUES (%s, %s, %s)",
                    (cur.fetchone()["id"], shop_id, amount))


def roll_up(cur, shop_id=None, batch: int = 5000) -> int:
    """Add pending entries to wallet balances: one shop's (before a payout), or a batch of any shops'."""
    if shop_id:
        cur.execute("DELETE FROM ecommerce.wallet_pending WHERE shop_id = %s RETURNING shop_id, amount", (shop_id,))
    else:
        cur.execute("""DELETE FROM ecommerce.wallet_pending WHERE entry_id IN (
                           SELECT entry_id FROM ecommerce.wallet_pending ORDER BY entry_id LIMIT %s FOR UPDATE SKIP LOCKED)
                       RETURNING shop_id, amount""", (batch,))
    totals = {}
    for r in cur.fetchall():
        totals[r["shop_id"]] = totals.get(r["shop_id"], 0) + r["amount"]
    for sid, amount in sorted(totals.items(), key=lambda x: str(x[0])):
        cur.execute("UPDATE ecommerce.shop_wallets SET balance = balance + %s WHERE shop_id = %s", (amount, sid))
    return len(totals)


def seller_share(line_total: int, shop_discount: int, qty: int, of_qty: int) -> int:
    """Seller's revenue for `qty` of a line of `of_qty` units (same rounding everywhere)."""
    return (line_total - shop_discount) * qty // of_qty


def settle_order(cur, order: dict):
    cur.execute(
        """SELECT oi.quantity, oi.line_total, oi.shop_discount,
                  coalesce((SELECT ri.quantity FROM ecommerce.return_items ri
                            JOIN ecommerce.returns r ON r.id = ri.return_id
                            WHERE ri.order_item_id = oi.id AND r.status = 'REFUNDED'), 0) AS returned
           FROM ecommerce.order_items oi
           WHERE oi.order_id = %s""",
        (order["id"],),
    )
    sale = 0
    for ln in cur.fetchall():
        returned = ln["returned"] if ln["returned"] else 0
        sale += seller_share(ln["line_total"], ln["shop_discount"], ln["quantity"], ln["quantity"])
        if returned:
            sale -= seller_share(ln["line_total"], ln["shop_discount"], returned, ln["quantity"])
    cur.execute("SELECT commission_rate_bps FROM ecommerce.shops WHERE id = %s", (order["shop_id"],))
    commission = sale * cur.fetchone()["commission_rate_bps"] // 10_000
    fee = sale * PAYMENT_FEE_BPS // 10_000
    for entry_type, amount in (("SALE", sale), ("COMMISSION", -commission), ("PAYMENT_FEE", -fee)):
        if amount:
            post(cur, order["shop_id"], entry_type, amount, order_id=order["id"])
    outbox.emit(cur, "shop", order["shop_id"], "OrderSettled",
                {"order_id": order["id"], "shop_id": order["shop_id"], "sale": sale, "commission": commission,
                 "payment_fee": fee, "net": sale - commission - fee})


def payout(cur, shop_id) -> dict | None:
    roll_up(cur, shop_id)
    cur.execute("SELECT balance FROM ecommerce.shop_wallets WHERE shop_id = %s FOR UPDATE", (shop_id,))
    balance = cur.fetchone()["balance"]
    if balance <= 0:
        return None
    cur.execute("INSERT INTO ecommerce.seller_payouts (shop_id, amount) VALUES (%s, %s) RETURNING *", (shop_id, balance))
    p = cur.fetchone()
    post(cur, shop_id, "PAYOUT", -balance, payout_id=p["id"], applied=True)
    cur.execute("UPDATE ecommerce.shop_wallets SET balance = balance - %s WHERE shop_id = %s", (balance, shop_id))
    outbox.emit(cur, "shop", shop_id, "PayoutPaid", {"payout_id": p["id"], "shop_id": shop_id, "amount": balance})
    return p

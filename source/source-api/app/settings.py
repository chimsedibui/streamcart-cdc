"""Business parameters. Durations are in BUSINESS seconds (see clock.py): realistic marketplace values,
which a demo compresses by raising the clock speed rather than by shortening the rules."""

import math
import os

MINUTE, DAY = 60, 86400


def _seconds(name: str, default: float) -> float:
    return float(os.getenv(name, default))


PAYMENT_WINDOW_S = _seconds("PAYMENT_WINDOW_S", 15 * MINUTE)   # online payment deadline after checkout
SELLER_SLA_S = _seconds("SELLER_SLA_S", 2 * DAY)               # seller must pack within this after payment
AUTO_COMPLETE_S = _seconds("AUTO_COMPLETE_S", 3 * DAY)         # delivered orders auto-complete after this
CART_ABANDON_S = _seconds("CART_ABANDON_S", 1 * DAY)           # idle carts with items become ABANDONED
RETURN_RESPONSE_S = _seconds("RETURN_RESPONSE_S", 2 * DAY)     # seller must answer a return request (else auto-approve)
DISPUTE_WINDOW_S = _seconds("DISPUTE_WINDOW_S", 1 * DAY)       # buyer may dispute a rejected return within this
PAYOUT_INTERVAL_S = _seconds("PAYOUT_INTERVAL_S", 1 * DAY)     # sellers are paid out at most once per interval
JOB_INTERVAL_S = float(os.getenv("JOB_INTERVAL_S", "2"))       # wall-clock seconds between scheduler runs
VOUCHER_QUOTA_SLOTS = int(os.getenv("VOUCHER_QUOTA_SLOTS", "16"))  # rows a voucher's quota is split into
IDEMPOTENCY_TTL_S = float(os.getenv("IDEMPOTENCY_TTL_S", str(72 * 3600)))  # wall clock: retries are real-time
MAX_DELIVERY_ATTEMPTS = 3

# Loyalty tier by lifetime spend on completed orders (VND), highest first
TIER_THRESHOLDS = [(50_000_000, "PLATINUM"), (20_000_000, "GOLD"), (5_000_000, "SILVER"), (0, "BRONZE")]

COMMISSION_BPS = {"MALL": 800, "PREFERRED": 600, "NORMAL": 400}
PAYMENT_FEE_BPS = 200                                           # transaction fee charged to the seller
MIN_ORDER_PAYABLE = 1_000                                       # vouchers never take an order below this
PAYMENT_PROVIDERS = {"WALLET": "MOMO", "CARD": "VNPAY", "BANK_TRANSFER": "NAPAS", "COD": "COD"}

REGIONS = {
    "Hà Nội": "NORTH", "Hải Phòng": "NORTH", "Quảng Ninh": "NORTH", "Bắc Ninh": "NORTH", "Thanh Hóa": "NORTH",
    "Nghệ An": "CENTRAL", "Thừa Thiên Huế": "CENTRAL", "Đà Nẵng": "CENTRAL", "Khánh Hòa": "CENTRAL",
    "Lâm Đồng": "CENTRAL",
    "TP.HCM": "SOUTH", "Bình Dương": "SOUTH", "Đồng Nai": "SOUTH", "Bà Rịa - Vũng Tàu": "SOUTH",
    "Long An": "SOUTH", "Cần Thơ": "SOUTH", "An Giang": "SOUTH",
}


def tier_for(total_spent: int) -> str:
    return next(tier for threshold, tier in TIER_THRESHOLDS if total_spent >= threshold)


def shipping_fee(from_province: str, to_province: str, weight_g: int) -> int:
    """Carrier rate card: base price for the first 500 g, then a step per extra 500 g."""
    extra = max(0, math.ceil(weight_g / 500) - 1)
    if from_province == to_province:
        return 15_000 + 2_500 * extra
    if REGIONS.get(from_province, "?") == REGIONS.get(to_province, "!"):
        return 22_000 + 4_000 * extra
    return 32_000 + 5_000 * extra

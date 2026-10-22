"""Small helpers shared by the engine and the actors."""

import math
import random
from datetime import timedelta, timezone

VN_TZ = timezone(timedelta(hours=7))


def chance(p: float) -> bool:
    return random.random() < p


def p_event(rate_per_hour: float, dt_h: float) -> float:
    """Probability that an event with this rate per business hour happens within dt_h hours."""
    return 1 - math.exp(-rate_per_hour * dt_h)


def poisson(lam: float) -> int:
    if lam <= 0:
        return 0
    if lam > 30:
        return max(0, round(random.gauss(lam, math.sqrt(lam))))
    k, p, limit = 0, 1.0, math.exp(-lam)
    while True:
        p *= random.random()
        if p <= limit:
            return k
        k += 1


def pick(weights: dict):
    return random.choices(list(weights), weights=list(weights.values()))[0]


def vnd(v) -> str:
    return f"{int(v):,}₫".replace(",", ".")


def short(detail) -> str:
    if isinstance(detail, dict):
        return str(detail.get("message", detail))[:120]
    return str(detail)[:120]

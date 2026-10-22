"""Business clock: `ecommerce.business_now()` runs at `speed` × wall-clock time.

Changing the speed re-anchors the clock at the current business time, so business time never jumps.
"""

from datetime import datetime


def business_now(cur) -> datetime:
    cur.execute("SELECT ecommerce.business_now() AS t")
    return cur.fetchone()["t"]


def read(cur) -> dict:
    cur.execute("SELECT ecommerce.business_now() AS now, speed FROM ecommerce.sim_clock")
    return cur.fetchone()


def set_speed(cur, speed: float) -> dict:
    cur.execute(
        """UPDATE ecommerce.sim_clock
           SET anchor_sim = ecommerce.business_now(), anchor_real = clock_timestamp(), speed = %s""",
        (speed,),
    )
    return read(cur)

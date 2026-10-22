import json
import time
from collections import defaultdict, deque
from datetime import datetime

import pandas as pd
import streamlit as st
from confluent_kafka import Consumer

import os
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPICS = [
    "cdc.ecommerce.customers",
    "cdc.ecommerce.customer_addresses",
    "cdc.ecommerce.products",
    "cdc.ecommerce.orders",
    "cdc.ecommerce.order_items",
    "cdc.ecommerce.inventory",
    "cdc.ecommerce.inventory_movements",
    "cdc.ecommerce.carts",
    "cdc.ecommerce.cart_items",
    "cdc.ecommerce.payments",
    "cdc.ecommerce.shipments",
]
MAX_EVENTS = 500
OP_MAP = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "READ"}

st.set_page_config(
    page_title="CDC Dashboard",
    page_icon="🔄",
    layout="wide",
)


def make_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": f"cdc-dashboard-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )


# --- Session state init ---
if "consumer" not in st.session_state:
    c = make_consumer()
    c.subscribe(TOPICS)
    st.session_state.consumer  = c
    st.session_state.events    = deque(maxlen=MAX_EVENTS)
    st.session_state.counters  = defaultdict(lambda: defaultdict(int))
    st.session_state.revenue   = 0.0
    st.session_state.started   = datetime.now().strftime("%H:%M:%S")

# --- Poll Kafka ---
consumer = st.session_state.consumer
for _ in range(300):
    msg = consumer.poll(0.0)
    if msg is None:
        break
    if msg.error():
        continue
    try:
        raw = msg.value()
        if raw is None:
            continue
        value = json.loads(raw.decode("utf-8"))
        topic = msg.topic()
        table = topic.split(".")[-1]
        op    = OP_MAP.get(value.get("op", ""), value.get("op", "?"))
        ts_ms = value.get("ts_ms", int(time.time() * 1000))

        st.session_state.events.appendleft(
            {
                "Time":      datetime.fromtimestamp(ts_ms / 1000).strftime("%H:%M:%S"),
                "Table":     table,
                "Operation": op,
            }
        )
        st.session_state.counters[table][op] += 1

        # Track revenue from orders as they become DELIVERED (total is VND, BIGINT)
        if table == "orders" and op == "UPDATE":
            before = value.get("before") or {}
            after = value.get("after") or {}
            if after.get("status") == "DELIVERED" and before.get("status") != "DELIVERED":
                st.session_state.revenue += int(after.get("total", 0))

    except Exception:
        continue

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
st.title("🔄 Real-time CDC Dashboard")
st.caption(
    f"Kafka broker: `{KAFKA_BROKER}` · "
    f"Started: {st.session_state.started} · "
    f"Last refresh: {datetime.now().strftime('%H:%M:%S')}"
)

counters = st.session_state.counters

# --- Top metrics ---
total  = sum(sum(ops.values()) for ops in counters.values())
orders = sum(counters["orders"].values())
custs  = sum(counters["customers"].values())
items  = sum(counters["order_items"].values())
inv    = sum(counters["inventory"].values())

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total Events",  total)
c2.metric("Orders",        orders)
c3.metric("Customers",     custs)
c4.metric("Order Items",   items)
c5.metric("Inventory",     inv)
c6.metric("Revenue (delivered)", f"{st.session_state.revenue:,} ₫")

st.divider()

# --- Two columns: breakdown table | recent events feed ---
left, right = st.columns([1, 2])

with left:
    st.subheader("Events by Table & Operation")
    rows = [
        {"Table": tbl, "Operation": op, "Count": cnt}
        for tbl, ops in counters.items()
        for op, cnt in ops.items()
    ]
    if rows:
        df_counts = (
            pd.DataFrame(rows)
            .sort_values(["Table", "Count"], ascending=[True, False])
            .reset_index(drop=True)
        )
        st.dataframe(df_counts, use_container_width=True, hide_index=True)
    else:
        st.info("Waiting for events from Kafka…")

with right:
    st.subheader("Recent Events Feed")
    if st.session_state.events:
        df_feed = pd.DataFrame(list(st.session_state.events))
        st.dataframe(df_feed, use_container_width=True, hide_index=True, height=420)
    else:
        st.info("No events yet. Make sure the Debezium connector is registered.")

# --- Operation distribution bar chart ---
if rows:
    st.divider()
    st.subheader("Operation Distribution")
    df_ops = (
        pd.DataFrame(rows)
        .groupby("Operation")["Count"]
        .sum()
        .reset_index()
        .sort_values("Count", ascending=False)
    )
    st.bar_chart(df_ops.set_index("Operation"))

# Auto-refresh every 3 seconds
time.sleep(3)
st.rerun()

"""Sets Superset up for the lakehouse through its REST API: the `Lakehouse` database (Trino, read-only catalog), a
dataset per gold table and the "Marketplace KPIs" dashboard. Idempotent: what exists (by name) is kept, so charts
edited in the UI survive a restart.
"""

import json
import sys
import time

import requests

BASE = "http://localhost:8088"
DATABASE = "Lakehouse"
URI = "trino://superset@trino:8080/lakehouse"     # Trino user superset: catalog `lakehouse` only (read-only)
GOLD = ["fct_orders", "daily_gmv", "orders_by_status", "seller_sla_daily"]
DASHBOARD = "Marketplace KPIs"


def metric(column: str, label: str, aggregate: str = "SUM") -> dict:
    return {"expressionType": "SIMPLE", "column": {"column_name": column}, "aggregate": aggregate, "label": label}


def sql_metric(sql: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


CHARTS = [  # (name, dataset, viz_type, params, width)
    ("GMV (all days)", "daily_gmv", "big_number_total",
     {"metric": metric("gmv", "GMV"), "y_axis_format": ",d", "subheader": "VND, orders not cancelled"}, 3),
    ("Average order value", "daily_gmv", "big_number_total",
     {"metric": sql_metric("SUM(buyer_paid) / NULLIF(SUM(valid_orders), 0)", "AOV"), "y_axis_format": ",.0f",
      "subheader": "VND per valid order"}, 3),
    ("Packing SLA met", "seller_sla_daily", "big_number_total",
     {"metric": sql_metric("CAST(SUM(packed_on_time) AS DOUBLE) / NULLIF(SUM(packed_on_time + packed_late + "
                           "sla_cancelled), 0)", "on time"), "y_axis_format": ".1%",
      "subheader": "orders packed by confirm_by"}, 3),
    ("Cancel rate", "daily_gmv", "big_number_total",
     {"metric": sql_metric("CAST(SUM(cancelled_orders) AS DOUBLE) / NULLIF(SUM(orders), 0)", "cancel rate"),
      "y_axis_format": ".1%", "subheader": "of orders placed"}, 3),
    ("GMV and buyer paid per business day", "daily_gmv", "echarts_timeseries_bar",
     {"x_axis": "order_date", "time_grain_sqla": "P1D", "metrics": [metric("gmv", "GMV"),
                                                                    metric("buyer_paid", "Buyer paid")],
      "groupby": [], "y_axis_format": ",d", "show_legend": True, "row_limit": 1000}, 8),
    ("Orders by status", "orders_by_status", "pie",
     {"groupby": ["status"], "metric": metric("orders", "orders"), "show_legend": True, "label_type": "key_percent",
      "row_limit": 100}, 4),
    ("Shops by packing SLA", "seller_sla_daily", "table",
     {"query_mode": "aggregate", "groupby": ["shop_name"],
      "metrics": [metric("orders_due", "due"), metric("packed_on_time", "on time"), metric("packed_late", "late"),
                  metric("sla_cancelled", "SLA cancelled"),
                  sql_metric("CAST(SUM(packed_on_time) AS DOUBLE) / NULLIF(SUM(packed_on_time + packed_late + "
                             "sla_cancelled), 0)", "on-time rate")],
      "order_desc": True, "row_limit": 100, "timeseries_limit_metric": metric("orders_due", "due")}, 12),
]


def query_context(ds: int, viz: str, params: dict) -> dict:
    """What the explore view would save with the chart, so the chart data API (and thumbnails, reports) work."""
    metrics = params.get("metrics") or [params["metric"]]
    columns = list(params.get("groupby", []))
    if "x_axis" in params:
        columns.insert(0, {"columnType": "BASE_AXIS", "sqlExpression": params["x_axis"], "label": params["x_axis"],
                           "expressionType": "SQL", "timeGrain": params.get("time_grain_sqla")})
    query = {"columns": columns, "metrics": metrics, "filters": [], "extras": {}, "row_limit": params.get("row_limit"),
             "orderby": [[metrics[0], False]] if columns else []}
    return {"datasource": {"id": ds, "type": "table"}, "force": False, "queries": [query],
            "form_data": {"viz_type": viz, "datasource": f"{ds}__table", **params},
            "result_format": "json", "result_type": "full"}


class Api:
    def __init__(self, password: str):
        self.s = requests.Session()
        r = self.s.post(f"{BASE}/api/v1/security/login",
                        json={"username": "admin", "password": password, "provider": "db", "refresh": False})
        r.raise_for_status()
        self.s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
        self.s.headers["X-CSRFToken"] = self.s.get(f"{BASE}/api/v1/security/csrf_token/").json()["result"]
        self.s.headers["Referer"] = BASE

    def find(self, kind: str, col: str, value: str) -> dict | None:
        q = json.dumps({"filters": [{"col": col, "opr": "eq", "value": value}]})
        rows = self.s.get(f"{BASE}/api/v1/{kind}/", params={"q": q}).json().get("result", [])
        return rows[0] if rows else None

    def create(self, kind: str, body: dict) -> int:
        r = self.s.post(f"{BASE}/api/v1/{kind}/", json=body)
        if r.status_code >= 400:
            raise SystemExit(f"POST {kind}: HTTP {r.status_code} {r.text[:500]}")
        return r.json()["id"]


def main():
    api = Api(open("/run/secrets/superset_admin_password").read().strip())
    db = api.find("database", "database_name", DATABASE)
    db_id = db["id"] if db else api.create("database", {
        "database_name": DATABASE, "sqlalchemy_uri": URI, "expose_in_sqllab": True, "allow_dml": False,
        "allow_ctas": False, "allow_cvas": False, "allow_file_upload": False})
    datasets = {}
    for table in GOLD:
        found = api.find("dataset", "table_name", table)
        datasets[table] = found["id"] if found else api.create("dataset", {
            "database": db_id, "schema": "gold", "table_name": table})
    if api.find("dashboard", "dashboard_title", DASHBOARD):
        print("superset: dashboard exists, kept")
        return
    dash_id = api.create("dashboard", {"dashboard_title": DASHBOARD, "slug": "marketplace-kpis", "published": True})
    layout = {"DASHBOARD_VERSION_KEY": "v2", "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
              "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
              "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": DASHBOARD}}}
    row, width = None, 12
    for i, (name, table, viz, params, w) in enumerate(CHARTS):
        chart_id = api.create("chart", {
            "slice_name": name, "viz_type": viz, "datasource_id": datasets[table], "datasource_type": "table",
            "dashboards": [dash_id],
            "params": json.dumps({"viz_type": viz, "datasource": f"{datasets[table]}__table", **params}),
            "query_context": json.dumps(query_context(datasets[table], viz, params))})
        if width + w > 12:
            row = f"ROW-{i}"
            layout[row] = {"type": "ROW", "id": row, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                           "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            layout["GRID_ID"]["children"].append(row)
            width = 0
        cell = f"CHART-{chart_id}"
        layout[cell] = {"type": "CHART", "id": cell, "children": [], "parents": ["ROOT_ID", "GRID_ID", row],
                        "meta": {"width": w, "height": 30 if viz == "big_number_total" else 60,
                                 "chartId": chart_id, "sliceName": name}}
        layout[row]["children"].append(cell)
        width += w
    r = api.s.put(f"{BASE}/api/v1/dashboard/{dash_id}", json={"position_json": json.dumps(layout)})
    r.raise_for_status()
    print(f"superset: database {db_id}, {len(datasets)} datasets, dashboard {dash_id} with {len(CHARTS)} charts")


if __name__ == "__main__":
    for attempt in range(10):
        try:
            main()
            break
        except requests.ConnectionError:
            time.sleep(5)
    else:
        sys.exit("superset: bootstrap gave up")

#!/usr/bin/env python3
"""Reconciles lakehouse.silver.* (Iceberg, via Spark SQL) with the source tables in PostgreSQL.

Per table: row count, Σ version and max(updated_at) where the table has them, so a missed update shows up as well as
a missed row. The source must hold still: stopping the simulator is not enough, because the Source API's scheduled
jobs (auto-complete, cart abandonment, payouts) keep acting on the business clock. Slow the clock to a crawl too:

    curl -X POST localhost:8200/stop
    curl -X PUT localhost:8200/clock -H 'Content-Type: application/json' -d '{"speed":0.001}'
    quality/reconcile.py --wait 120      # exit 0 = silver equals the source; retries while silver catches up
    curl -X PUT localhost:8200/clock -H 'Content-Type: application/json' -d '{"speed":240}'
"""

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "flink"))
from gen_silver import PSQL, captured, source_tables  # noqa: E402

SPARK_SQL = [str(ROOT / "lakehouse" / "spark-sql.sh")]


def measures(table: dict, spark: bool) -> str:
    names = {c["name"] for c in table["columns"]}
    version = "coalesce(sum(version), 0)" if "version" in names else "0"
    if "updated_at" not in names:
        latest = "0"
    elif spark:
        latest = "coalesce(unix_micros(max(updated_at)), 0)"
    else:
        latest = "coalesce((extract(epoch FROM max(updated_at)) * 1000000)::bigint, 0)"
    where = f"lakehouse.silver.`{table['table_name']}`" if spark else f"ecommerce.{table['table_name']}"
    return f"SELECT '{table['table_name']}', count(*), {version}, {latest} FROM {where}"


def query_pg(tables) -> dict:
    sql = "\nUNION ALL ".join(measures(t, spark=False) for t in tables)
    out = subprocess.run(PSQL.split() + ["-At", "-F", "\t", "-v", "ON_ERROR_STOP=1"], input=sql,
                         capture_output=True, text=True, check=True).stdout
    return parse(out)


def query_lake(tables) -> dict:
    sql = "\nUNION ALL ".join(measures(t, spark=True) for t in tables)
    out = subprocess.run(SPARK_SQL + ["-e", sql], capture_output=True, text=True, check=True).stdout
    return parse(out)


def parse(out: str) -> dict:
    rows = {}
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) == 4 and parts[1].isdigit():
            rows[parts[0]] = tuple(int(p) for p in parts[1:])
    return rows


def compare(tables) -> list[str]:
    pg, lake = query_pg(tables), query_lake(tables)
    bad = []
    for t in sorted(pg):
        if pg[t] != lake.get(t):
            bad.append(f"{t:24} source rows={pg[t][0]:>7} Σversion={pg[t][1]:>8} latest={pg[t][2]}\n"
                       f"{'':24} silver {('rows=%7d Σversion=%8d latest=%d' % lake[t]) if t in lake else 'missing'}")
    print(f"{len(pg) - len(bad)}/{len(pg)} tables match; {sum(r[0] for r in pg.values())} source rows")
    return bad


def main():
    wait = float(sys.argv[sys.argv.index("--wait") + 1]) if "--wait" in sys.argv else 0
    tables = captured(source_tables())
    deadline = time.monotonic() + wait
    while True:
        bad = compare(tables)
        if not bad or time.monotonic() >= deadline:
            break
        time.sleep(15)
    for line in bad:
        print(line)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()

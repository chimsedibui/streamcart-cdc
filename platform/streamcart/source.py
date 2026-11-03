"""The source system as the platform sees it: the ecommerce schema in PostgreSQL and the Debezium capture filter.

PSQL is the command that runs a query against the source database, by default the compose one:
    PSQL="psql postgresql://admin:admin@localhost:5432/enterprise_db" platform/cli.py validate
In CI it points at a throwaway PostgreSQL loaded with source/postgres/init.sql (.github/workflows/contract.yml).
"""

import json
import os
import re
import shlex
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONNECTOR = ROOT / "debezium" / "postgres-connector.json"
PSQL = os.getenv("PSQL", "docker exec -i streamcart-postgres-1 psql -U admin -d enterprise_db")

# Captured, but not business state: the outbox is rerouted to events.* and the signal table drives snapshots
NOT_SILVER = {"outbox_events", "debezium_signals"}

CATALOG_QUERY = """
SELECT json_agg(t ORDER BY t.table_name) FROM (
  SELECT c.table_name,
         json_agg(json_build_object('name', c.column_name, 'type', c.data_type) ORDER BY c.ordinal_position) AS columns,
         (SELECT json_agg(k.column_name ORDER BY k.ordinal_position)
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage k USING (constraint_schema, constraint_name)
           WHERE tc.table_schema = 'ecommerce' AND tc.table_name = c.table_name
             AND tc.constraint_type = 'PRIMARY KEY') AS primary_key,
         (SELECT CASE relreplident WHEN 'f' THEN 'full' WHEN 'd' THEN 'default' WHEN 'n' THEN 'nothing'
                                   ELSE 'index' END
            FROM pg_class WHERE oid = format('ecommerce.%I', c.table_name)::regclass) AS replica_identity
    FROM information_schema.columns c
    JOIN information_schema.tables tb USING (table_schema, table_name)
   WHERE c.table_schema = 'ecommerce' AND tb.table_type = 'BASE TABLE'
   GROUP BY c.table_name) t
"""


def psql(sql: str, *args: str) -> str:
    try:
        return subprocess.run(shlex.split(PSQL) + ["-At", "-v", "ON_ERROR_STOP=1", *args], input=sql,
                              capture_output=True, text=True, check=True).stdout
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        detail = e.stderr.strip() if isinstance(e, subprocess.CalledProcessError) else str(e)
        raise SystemExit(f"cannot query the source with PSQL='{PSQL}': {detail}") from None


def source_tables() -> list[dict]:
    """Every base table of the ecommerce schema: columns (name, PostgreSQL type), primary key, replica identity."""
    return json.loads(psql(CATALOG_QUERY))


def captured(tables: list[dict]) -> list[dict]:
    """The tables that become silver: what the connector captures, minus the CDC plumbing."""
    include = json.loads(CONNECTOR.read_text())["config"]["table.include.list"]
    pattern = re.compile(include)    # the connector's Java regex; its lookahead is valid Python too
    return [t for t in tables if pattern.fullmatch(f"ecommerce.{t['table_name']}") and t["table_name"] not in NOT_SILVER]

"""`platform/cli.py validate`: everything that can be checked without the lakehouse running.

1. Specs lint: required keys, expectations, freshness, inputs that resolve, no cycles.
2. SQL: every batch dataset's query and every expectation parses (Trino dialect), a query reads exactly the datasets
   its spec declares, and expectations only name columns the dataset has.
3. Generated code: flink/sql/silver.sql equals what the silver specs render to.
4. Source contract: the silver specs equal the source schema (tables, columns, types, keys, replica identity). A new
   source column, a type change, a dropped table or a lost REPLICA IDENTITY FULL fails here, before anything runs.
"""

import sqlglot
from sqlglot import exp

from . import silver
from .source import captured, source_tables
from .specs import Spec, SpecError, load_all, ordered


def lint() -> tuple[dict[str, Spec], list[str]]:
    try:
        specs = load_all()
        ordered(specs)
    except SpecError as e:
        return {}, [str(e)]
    problems = []
    for s in specs.values():
        if s.mode == "streaming":
            if s.topic != f"cdc.ecommerce.{s.name}":
                problems.append(f"{s.dataset}: reads {s.topic}, expected cdc.ecommerce.{s.name}")
            for col, ty in s.columns.items():
                if ty not in silver.FLINK_TYPES:
                    problems.append(f"{s.dataset}.{col}: no Flink mapping for PostgreSQL type '{ty}'")
            if s.replica_identity != "full" and ("updated_at" in s.columns or "version" in s.columns):
                problems.append(f"{s.dataset}: rows are updated (updated_at/version) but replica_identity is "
                                f"{s.replica_identity}; Flink's debezium-json needs the full before-image")
        else:
            problems += check_query(s, specs)
        columns = set(s.columns) | ({"_source_ts"} if s.mode == "streaming" else set())
        for e in s.expectations:
            try:
                tree = sqlglot.parse_one(e.sql, read="trino")
            except sqlglot.errors.ParseError as err:
                problems.append(f"{s.dataset}: expectation {e.name} does not parse: {err}")
                continue
            unknown = {c.name for c in tree.find_all(exp.Column)} - columns
            if unknown:
                problems.append(f"{s.dataset}: expectation {e.name} names unknown columns {sorted(unknown)}")
    return specs, problems


def referenced_tables(sql: str) -> set[str]:
    tree = sqlglot.parse_one(sql, read="trino")
    ctes = {c.alias_or_name for c in tree.find_all(exp.CTE)}
    return {f"{t.db}.{t.name}" if t.db else t.name for t in tree.find_all(exp.Table) if t.name not in ctes}


def check_query(s: Spec, specs: dict[str, Spec]) -> list[str]:
    try:
        tables = referenced_tables(s.sql)
    except sqlglot.errors.ParseError as err:
        return [f"{s.dataset}: {s.sql_file.name} does not parse: {err}"]
    problems = []
    if tables != set(s.upstream):
        problems.append(f"{s.dataset}: {s.sql_file.name} reads {sorted(tables)}, the spec declares "
                        f"{sorted(s.upstream)}")
    unqualified = [t for t in tables if "." not in t]
    if unqualified:
        problems.append(f"{s.dataset}: name tables as <layer>.<table>, not {unqualified}")
    for up in s.upstream:
        if up in specs and specs[up].layer == "gold" and s.layer != "gold":
            problems.append(f"{s.dataset}: {s.layer} may not read gold ({up})")
    return problems


def generated(specs: dict[str, Spec]) -> list[str]:
    want = silver.render([s for s in specs.values() if s.mode == "streaming"])
    if not silver.OUT.exists() or silver.OUT.read_text() != want:
        return [f"{silver.OUT.relative_to(silver.ROOT)} is stale: run platform/cli.py generate"]
    return []


def contract(specs: dict[str, Spec]) -> list[str]:
    source = {t["table_name"]: t for t in captured(source_tables())}
    ours = {s.name: s for s in specs.values() if s.layer == "silver"}
    problems = []
    for name in sorted(set(source) - set(ours)):
        problems.append(f"source table ecommerce.{name} is captured but has no spec: platform/cli.py new silver.{name}")
    for name in sorted(set(ours) - set(source)):
        problems.append(f"silver.{name}: ecommerce.{name} is gone from the source (or no longer captured)")
    for name in sorted(set(ours) & set(source)):
        s, t = ours[name], source[name]
        src_cols = {c["name"]: c["type"] for c in t["columns"]}
        for col in src_cols.keys() - s.columns.keys():
            problems.append(f"silver.{name}: new source column {col} ({src_cols[col]})")
        for col in s.columns.keys() - src_cols.keys():
            problems.append(f"silver.{name}: column {col} was dropped from the source")
        for col in s.columns.keys() & src_cols.keys():
            if s.columns[col] != src_cols[col]:
                problems.append(f"silver.{name}.{col}: the spec says {s.columns[col]}, the source has {src_cols[col]}")
        if list(s.columns) != [c for c in src_cols if c in s.columns]:
            problems.append(f"silver.{name}: column order differs from the source")
        if s.primary_key != t["primary_key"]:
            problems.append(f"silver.{name}: primary key {s.primary_key}, source has {t['primary_key']}")
        if s.replica_identity != t["replica_identity"]:
            problems.append(f"silver.{name}: replica identity {s.replica_identity}, source has "
                            f"{t['replica_identity']}")
    return problems


def run(with_source: bool = True) -> list[str]:
    specs, problems = lint()
    if not specs:
        return problems
    problems += generated(specs)
    if with_source:
        problems += contract(specs)
    return problems

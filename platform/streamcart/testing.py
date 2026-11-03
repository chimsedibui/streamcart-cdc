"""`platform/cli.py test`: runs batch datasets' SQL on small fixtures, in Trino's memory catalog, with the same
schema, primary key and expectation checks a real build runs. Same engine and dialect as production, and nothing in
the lakehouse is read or written.

A fixture file, pipelines/<layer>/tests/<name>.yml, holds cases:

    dataset: gold.fct_orders
    cases:
      - name: a total that does not add up is caught
        given:                          # input tables: rows with the columns the query needs (others are NULL)
          silver.orders: [{id: o1, total: 1, ...}]
        expect:
          failing: {total_matches_parts: 1}      # rows failing each check (unlisted checks: 0)
          rows: [{order_id: o1, ...}]            # optional: the output, ordered by primary key, listed columns only
"""

import datetime
import math
import sys

import yaml

from . import checks, silver
from .engine import Trino
from .specs import PIPELINES, Spec, load_all

USER = "platform-test"


def literal(value, sql_type: str) -> str:
    if value is None:
        return "NULL"
    if sql_type == "boolean":
        return "true" if value else "false"
    if sql_type in ("bigint", "integer", "smallint", "double"):
        return str(value)
    text = str(value).replace("'", "''")
    if sql_type == "date":
        return f"DATE '{text}'"
    if sql_type.startswith("timestamp"):
        return f"CAST(TIMESTAMP '{text}' AS {sql_type})"
    return f"'{text}'"


def columns_of(spec: Spec) -> dict[str, str]:
    return silver.trino_columns(spec) if spec.mode == "streaming" else spec.columns


def load_table(t: Trino, spec: Spec, rows: list[dict]):
    cols = columns_of(spec)
    unknown = {c for r in rows for c in r} - cols.keys()
    if unknown:
        raise ValueError(f"fixture for {spec.dataset} has columns it does not have: {sorted(unknown)}")
    t.execute(f"CREATE SCHEMA IF NOT EXISTS memory.{spec.layer}")
    t.execute(f"DROP TABLE IF EXISTS memory.{spec.dataset}")
    t.execute(f"CREATE TABLE memory.{spec.dataset} ({', '.join(f'{c} {ty}' for c, ty in cols.items())})")
    if rows:
        values = ", ".join("(" + ", ".join(literal(r.get(c), ty) for c, ty in cols.items()) + ")" for r in rows)
        t.execute(f"INSERT INTO memory.{spec.dataset} VALUES {values}")


def same(actual, expected) -> bool:
    if isinstance(actual, float) or isinstance(expected, float):
        return actual is not None and expected is not None and math.isclose(actual, expected, rel_tol=1e-9)
    if isinstance(actual, datetime.datetime) and isinstance(expected, str):
        return actual == datetime.datetime.fromisoformat(expected)
    return str(actual) == str(expected) if actual is not None and expected is not None else actual is expected


def run_case(t: Trino, spec: Spec, specs: dict[str, Spec], case: dict) -> list[str]:
    for name, rows in (case.get("given") or {}).items():
        if name not in specs:
            return [f"given table {name} has no spec"]
        load_table(t, specs[name], rows or [])
    for up in spec.upstream:
        if up not in (case.get("given") or {}):
            load_table(t, specs[up], [])
    out = f"{spec.name}__test"
    t.execute(f"CREATE SCHEMA IF NOT EXISTS memory.{spec.layer}")
    t.execute(f"DROP TABLE IF EXISTS memory.{spec.layer}.{out}")
    t.execute(f"CREATE TABLE memory.{spec.layer}.{out} AS\n{spec.sql}")
    table = f"{spec.layer}.{out}"

    problems = []
    outcomes = [checks.schema(t, spec, spec.layer, out)]
    if outcomes[0].passed:
        outcomes.append(checks.primary_key(t, spec, table))
        outcomes += [checks.expectation(t, spec, e, table) for e in spec.expectations]
    expect = case.get("expect") or {}
    want_failing = expect.get("failing") or {}
    unknown = set(want_failing) - {o.name for o in outcomes} - {e.name for e in spec.expectations}
    if unknown:
        problems.append(f"expect.failing names unknown checks {sorted(unknown)}")
    for o in outcomes:
        got, want = o.failing_rows or (0 if o.passed else 1), want_failing.get(o.name, 0)
        if got != want:
            problems.append(f"{o.name}: {got} failing rows, expected {want}{' (' + o.detail + ')' if o.detail else ''}")
    if "rows" in expect and outcomes[0].passed:
        wanted = expect["rows"]
        cols = list(dict.fromkeys(c for r in wanted for c in r))
        got = t.rows(f"SELECT {', '.join(cols)} FROM {table} ORDER BY {checks.keys(spec)}")
        if len(got) != len(wanted):
            problems.append(f"{len(got)} output rows, expected {len(wanted)}")
        for i, (g, w) in enumerate(zip(got, wanted)):
            for c, v in zip(cols, g):
                if c in w and not same(v, w[c]):
                    problems.append(f"row {i + 1} {c}: {v!r}, expected {w[c]!r}")
    return problems


def run(datasets: list[str]) -> int:
    specs = load_all()
    files = sorted(PIPELINES.glob("*/tests/*.yml"))
    t = Trino(catalog="memory", user=USER)
    failed = total = 0
    for path in files:
        fixture = yaml.safe_load(path.read_text())
        name = fixture["dataset"]
        if datasets and name not in datasets:
            continue
        spec = specs.get(name)
        if spec is None or spec.mode != "batch":
            print(f"✗ {path.name}: {name} is not a batch dataset")
            failed += 1
            continue
        for case in fixture["cases"]:
            total += 1
            try:
                problems = run_case(t, spec, specs, case)
            except Exception as e:                     # a query that does not run is a failed test, not a crash
                problems = [f"{type(e).__name__}: {e}"]
            print(f"{'✓' if not problems else '✗'} {name}: {case['name']}")
            for p in problems:
                print(f"    {p}")
            failed += bool(problems)
    untested = sorted(s.dataset for s in specs.values() if s.mode == "batch"
                      and not (PIPELINES / s.layer / "tests" / f"{s.name}.yml").exists())
    if untested and not datasets:
        print(f"no fixtures for {', '.join(untested)}")
    print(f"{total - failed}/{total} cases pass")
    sys.stdout.flush()
    return 1 if failed or not total else 0

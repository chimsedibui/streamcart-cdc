#!/usr/bin/env python3
"""The platform CLI: build, validate and test pipelines from their specs (pipelines/<layer>/<name>.yml).

    platform/cli.py new silver.<table>       scaffold a silver spec from the source table (--missing: every new one)
    platform/cli.py new gold.<name>          scaffold a gold spec and its SQL
    platform/cli.py validate [--offline]     lint specs and SQL, check generated code; with the source: the contract
    platform/cli.py generate [--check]       write flink/sql/silver.sql from the silver specs
    platform/cli.py test [dataset ...]       run gold SQL on fixtures (pipelines/gold/tests/) in Trino's memory catalog
    platform/cli.py build <dataset> [...]    write-audit-publish gold datasets now, as Dagster does on its schedule
    platform/cli.py lineage                  send the lineage graph from the specs to Marquez

Needs `pip install -r platform/requirements.txt`. The source is reached with PSQL (platform/streamcart/source.py),
Trino at TRINO_URL (default http://localhost:8090), Marquez at OPENLINEAGE_URL (default http://localhost:5000).
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from streamcart import silver, specs, validate  # noqa: E402
from streamcart.source import ROOT, captured, source_tables  # noqa: E402

GOLD_TEMPLATE = """dataset: gold.{name}
owner: analytics
description: >
  What one row is, and how each metric is defined.
mode: batch
from: [silver.orders]
sql: {name}.sql
schedule: "*/10 * * * *"
primary_key: [id]
columns:
  id: varchar
expectations:
  - name: id_not_null
    sql: id IS NOT NULL
    on_fail: block
freshness: {{warn_after: 30m, error_after: 2h}}
"""


def cmd_new(args) -> int:
    if args.missing:
        names = [t["table_name"] for t in captured(source_tables()) if not silver.spec_path(t["table_name"]).exists()]
        return new_silver(names)
    if not args.dataset:
        sys.exit("name a dataset (silver.<table> or gold.<name>) or pass --missing")
    layer, _, name = args.dataset.partition(".")
    if layer == "silver":
        return new_silver([name])
    if layer == "gold":
        spec, sql = specs.PIPELINES / "gold" / f"{name}.yml", specs.PIPELINES / "gold" / f"{name}.sql"
        if spec.exists():
            sys.exit(f"{spec.relative_to(ROOT)} exists")
        spec.write_text(GOLD_TEMPLATE.format(name=name))
        sql.write_text("SELECT id\nFROM silver.orders\n")
        print(f"wrote {spec.relative_to(ROOT)} and {sql.relative_to(ROOT)}")
        return 0
    sys.exit(f"unknown layer '{layer}'")


def new_silver(names: list[str]) -> int:
    tables = {t["table_name"]: t for t in captured(source_tables())}
    for name in names:
        if name not in tables:
            sys.exit(f"ecommerce.{name} is not a captured source table")
        path = silver.spec_path(name)
        if path.exists():
            sys.exit(f"{path.relative_to(ROOT)} exists")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(silver.scaffold(tables[name]))
        print(f"wrote {path.relative_to(ROOT)}")
    if names:
        print("now: platform/cli.py generate, then review the specs")
    return 0


def cmd_validate(args) -> int:
    problems = validate.run(with_source=not args.offline)
    for p in problems:
        print(f"  ✗ {p}")
    loaded = specs.load_all() if not problems else {}
    print(f"{len(problems)} problem(s)" if problems else
          f"ok: {len(loaded)} specs" + ("" if args.offline else ", source contract holds"))
    return 1 if problems else 0


def cmd_generate(args) -> int:
    all_specs = specs.load_all()
    sql = silver.render([s for s in all_specs.values() if s.mode == "streaming"])
    if args.check:
        stale = not silver.OUT.exists() or silver.OUT.read_text() != sql
        print(f"{silver.OUT.relative_to(ROOT)} is {'stale' if stale else 'up to date'}")
        return 1 if stale else 0
    silver.OUT.write_text(sql)
    print(f"wrote {silver.OUT.relative_to(ROOT)}: {sql.count('INSERT INTO')} tables")
    return 0


def cmd_test(args) -> int:
    from streamcart import testing
    return testing.run(args.datasets)


def cmd_build(args) -> int:
    from streamcart import wap
    all_specs = specs.load_all()
    failed = 0
    for name in args.datasets:
        if name not in all_specs or all_specs[name].mode != "batch":
            sys.exit(f"{name} is not a batch dataset")
        result = wap.build(all_specs[name], run_id="cli")
        print(result.summary())
        failed += result.outcome != "published"
    return 1 if failed else 0


def cmd_lineage(args) -> int:
    from streamcart import lineage
    n = lineage.publish_static(specs.load_all())
    print(f"sent {n} lineage events")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(prog="platform/cli.py", description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="command", required=True)
    s = sub.add_parser("new", help="scaffold a spec")
    s.add_argument("dataset", nargs="?")
    s.add_argument("--missing", action="store_true", help="a silver spec for every captured table without one")
    s.set_defaults(func=cmd_new)
    s = sub.add_parser("validate", help="lint specs and SQL, check the source contract")
    s.add_argument("--offline", action="store_true", help="skip the source contract (no database needed)")
    s.set_defaults(func=cmd_validate)
    s = sub.add_parser("generate", help="write flink/sql/silver.sql")
    s.add_argument("--check", action="store_true", help="exit 1 if it is stale")
    s.set_defaults(func=cmd_generate)
    s = sub.add_parser("test", help="run gold SQL on fixtures")
    s.add_argument("datasets", nargs="*")
    s.set_defaults(func=cmd_test)
    s = sub.add_parser("build", help="write-audit-publish gold datasets")
    s.add_argument("datasets", nargs="+")
    s.set_defaults(func=cmd_build)
    s = sub.add_parser("lineage", help="send the specs' lineage graph to Marquez")
    s.set_defaults(func=cmd_lineage)
    args = p.parse_args()
    try:
        return args.func(args)
    except specs.SpecError as e:
        print(f"  ✗ {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

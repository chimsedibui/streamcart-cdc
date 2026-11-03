"""Pipeline specs: one YAML file per dataset under pipelines/<layer>/<name>.yml, the single source of truth for what a
dataset is (owner, inputs, columns, key), how it is built (streaming upsert or batch SQL) and what it must satisfy
(expectations, freshness). Everything else is derived from them: the Flink job, the Dagster assets and checks, the
lineage graph and the docs.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .source import ROOT

PIPELINES = ROOT / "pipelines"
LAYERS = ("silver", "gold")
ON_FAIL = ("block", "quarantine", "warn")
KEYS = {
    "common": {"dataset", "owner", "description", "mode", "from", "primary_key", "columns", "expectations",
               "freshness"},
    "streaming": {"replica_identity"},
    "batch": {"sql", "schedule", "reconcile"},
}
DURATION = re.compile(r"^(\d+)([smhd])$")
SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


class SpecError(Exception):
    pass


def seconds(text: str) -> int:
    m = DURATION.match(str(text))
    if not m:
        raise SpecError(f"bad duration '{text}' (use e.g. 30s, 5m, 2h, 1d)")
    return int(m[1]) * SECONDS[m[2]]


@dataclass(frozen=True)
class Expectation:
    name: str
    sql: str            # a boolean SQL expression over one row; rows where it is not TRUE fail
    on_fail: str        # block: nothing is published; quarantine: failing rows are set aside; warn: recorded only


@dataclass
class Spec:
    path: Path
    dataset: str
    owner: str
    description: str
    mode: str                           # streaming (Flink upsert from Kafka) | batch (Trino SQL under Dagster)
    inputs: list[str]                   # kafka:<topic>, silver.<table>, gold.<table>
    primary_key: list[str]
    columns: dict[str, str]             # silver: PostgreSQL types of the source; gold: Trino types of the output
    expectations: list[Expectation]
    warn_after: int | None              # seconds since the last commit
    error_after: int | None
    replica_identity: str | None = None
    sql_file: Path | None = None
    schedule: str | None = None
    reconcile: dict | None = None
    raw: dict = field(default_factory=dict, repr=False)

    @property
    def layer(self) -> str:
        return self.dataset.split(".")[0]

    @property
    def name(self) -> str:
        return self.dataset.split(".")[1]

    @property
    def topic(self) -> str | None:
        return next((i.removeprefix("kafka:") for i in self.inputs if i.startswith("kafka:")), None)

    @property
    def upstream(self) -> list[str]:
        return [i for i in self.inputs if not i.startswith("kafka:")]

    @property
    def sql(self) -> str:
        return self.sql_file.read_text() if self.sql_file else ""


def load(path: Path) -> Spec:
    where = path.relative_to(ROOT)
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as e:
        raise SpecError(f"{where}: not valid YAML: {e}") from None
    mode = raw.get("mode")
    if mode not in ("streaming", "batch"):
        raise SpecError(f"{where}: mode must be streaming or batch")
    unknown = set(raw) - KEYS["common"] - KEYS[mode]
    if unknown:
        raise SpecError(f"{where}: unknown keys {sorted(unknown)}")
    for key in ("dataset", "owner", "from", "primary_key", "columns"):
        if not raw.get(key):
            raise SpecError(f"{where}: '{key}' is required")
    expected = f"{path.parent.name}.{path.stem}"
    if raw["dataset"] != expected:
        raise SpecError(f"{where}: dataset is '{raw['dataset']}', the file name says '{expected}'")

    expectations, seen = [], set()
    for e in raw.get("expectations") or []:
        name, sql, on_fail = e.get("name"), e.get("sql"), e.get("on_fail", "warn")
        if not name or not sql:
            raise SpecError(f"{where}: every expectation needs a name and sql")
        if name in seen:
            raise SpecError(f"{where}: expectation '{name}' is defined twice")
        if on_fail not in ON_FAIL:
            raise SpecError(f"{where}: {name}: on_fail must be one of {ON_FAIL}")
        if mode == "streaming" and on_fail == "block":
            raise SpecError(f"{where}: {name}: a stream cannot be blocked; use quarantine or warn")
        seen.add(name)
        expectations.append(Expectation(name, " ".join(str(sql).split()), on_fail))

    freshness = raw.get("freshness") or {}
    warn = seconds(freshness["warn_after"]) if "warn_after" in freshness else None
    error = seconds(freshness["error_after"]) if "error_after" in freshness else None
    if warn and error and warn > error:
        raise SpecError(f"{where}: freshness warn_after is longer than error_after")

    inputs = raw["from"] if isinstance(raw["from"], list) else [raw["from"]]
    columns = {str(k): str(v) for k, v in raw["columns"].items()}
    missing = [k for k in raw["primary_key"] if k not in columns]
    if missing:
        raise SpecError(f"{where}: primary key columns {missing} are not in columns")

    spec = Spec(path=path, dataset=raw["dataset"], owner=raw["owner"], description=raw.get("description", ""),
                mode=mode, inputs=inputs, primary_key=list(raw["primary_key"]), columns=columns,
                expectations=expectations, warn_after=warn, error_after=error, raw=raw)
    if mode == "streaming":
        if len([i for i in inputs if i.startswith("kafka:")]) != 1 or len(inputs) != 1:
            raise SpecError(f"{where}: a streaming dataset reads exactly one kafka:<topic>")
        spec.replica_identity = raw.get("replica_identity")
        if spec.replica_identity not in ("full", "default"):
            raise SpecError(f"{where}: replica_identity must be full or default")
    else:
        if not raw.get("sql"):
            raise SpecError(f"{where}: a batch dataset needs sql: <file>")
        spec.sql_file = path.parent / raw["sql"]
        if not spec.sql_file.exists():
            raise SpecError(f"{where}: {raw['sql']} does not exist")
        spec.schedule = raw.get("schedule")
        spec.reconcile = raw.get("reconcile")
        if spec.reconcile:
            rec = spec.reconcile
            if not rec.get("sql") or not rec.get("key") or not rec.get("compare"):
                raise SpecError(f"{where}: reconcile needs sql, key and compare")
            if not (path.parent / rec["sql"]).exists():
                raise SpecError(f"{where}: {rec['sql']} does not exist")
    return spec


def load_all() -> dict[str, Spec]:
    specs = {}
    for layer in LAYERS:
        for path in sorted((PIPELINES / layer).glob("*.yml")):
            spec = load(path)
            specs[spec.dataset] = spec
    return specs


def ordered(specs: dict[str, Spec]) -> list[Spec]:
    """Specs in dependency order (inputs first); unknown inputs and cycles are errors."""
    done, out, visiting = set(), [], set()

    def visit(name: str, chain: list[str]):
        if name in done:
            return
        if name in visiting:
            raise SpecError(f"dependency cycle: {' -> '.join(chain + [name])}")
        if name not in specs:
            raise SpecError(f"{chain[-1]} reads {name}, which has no spec")
        visiting.add(name)
        for up in specs[name].upstream:
            visit(up, chain + [name])
        visiting.discard(name)
        done.add(name)
        out.append(specs[name])

    for name in sorted(specs):
        visit(name, [])
    return out

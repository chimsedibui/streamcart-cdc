#!/bin/bash
# platform-db, first start only: one database per platform service (owned by the `platform` superuser of this
# instance) and, in `platform`, the ops schema Dagster writes check results and publishes to. Grafana reads ops.* as
# the read-only role ops_reader.
set -euo pipefail
psql -v ON_ERROR_STOP=1 -U platform -d platform <<SQL
CREATE DATABASE dagster;
CREATE DATABASE marquez;
CREATE DATABASE superset;

CREATE SCHEMA ops;
-- One row per evaluation of a check: expectations (one per spec entry), freshness, reconciliation, schema
CREATE TABLE ops.check_results (
    id            BIGSERIAL PRIMARY KEY,
    evaluated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    dataset       TEXT        NOT NULL,             -- silver.orders, gold.daily_gmv
    check_name    TEXT        NOT NULL,
    passed        BOOLEAN     NOT NULL,
    severity      TEXT        NOT NULL CHECK (severity IN ('ERROR', 'WARN')),
    failing_rows  BIGINT,
    metric        DOUBLE PRECISION,                 -- freshness: seconds since the last commit
    detail        TEXT,
    run_id        TEXT
);
CREATE INDEX idx_check_results_latest ON ops.check_results (dataset, check_name, evaluated_at DESC);
CREATE VIEW ops.check_latest AS
SELECT DISTINCT ON (dataset, check_name) * FROM ops.check_results ORDER BY dataset, check_name, evaluated_at DESC;

-- One row per write-audit-publish run of a batch dataset
CREATE TABLE ops.publishes (
    id            BIGSERIAL PRIMARY KEY,
    started_at    TIMESTAMPTZ NOT NULL,
    finished_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    dataset       TEXT        NOT NULL,
    branch        TEXT        NOT NULL,
    outcome       TEXT        NOT NULL CHECK (outcome IN ('published', 'blocked', 'failed')),
    rows_written  BIGINT,
    rows_quarantined BIGINT,
    snapshot_id   BIGINT,
    detail        TEXT,
    run_id        TEXT
);
CREATE INDEX idx_publishes_dataset ON ops.publishes (dataset, finished_at DESC);

CREATE ROLE ops_reader LOGIN PASSWORD '$(cat /run/secrets/ops_reader_password)';
GRANT USAGE ON SCHEMA ops TO ops_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA ops TO ops_reader;
ALTER ROLE ops_reader SET default_transaction_read_only = on;
SQL

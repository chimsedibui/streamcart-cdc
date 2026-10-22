-- Least-privilege application roles; passwords come from passwords.sh. Idempotent: runs at init on a fresh volume
-- and from observability/start.sh on an existing one.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'source_api') THEN CREATE ROLE source_api LOGIN; END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_reader') THEN CREATE ROLE app_reader LOGIN; END IF;
END
$$;
-- nobody connects to the business database unless granted (the identity service's role has no business here)
REVOKE CONNECT ON DATABASE enterprise_db FROM PUBLIC;
GRANT CONNECT ON DATABASE enterprise_db TO source_api, app_reader, cdc_user, monitor_reader;
GRANT USAGE ON SCHEMA ecommerce TO source_api, app_reader;

-- source_api: the only writer. Reads and writes rows; owns nothing and runs no DDL
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ecommerce TO source_api;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ecommerce TO source_api;
ALTER DEFAULT PRIVILEGES IN SCHEMA ecommerce GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO source_api;
ALTER DEFAULT PRIVILEGES IN SCHEMA ecommerce GRANT USAGE, SELECT ON SEQUENCES TO source_api;

-- app_reader: read-only, for warehouse-jobs reconciliation and the Control Center health check (slot lag)
GRANT SELECT ON ALL TABLES IN SCHEMA ecommerce TO app_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA ecommerce GRANT SELECT ON TABLES TO app_reader;
GRANT pg_monitor TO app_reader;
ALTER ROLE app_reader SET default_transaction_read_only = on;

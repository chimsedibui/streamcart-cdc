DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'monitor_reader') THEN
    CREATE ROLE monitor_reader LOGIN;   -- password: source/postgres/passwords.sh
  END IF;
END $$;
GRANT CONNECT ON DATABASE enterprise_db TO monitor_reader;
GRANT pg_monitor TO monitor_reader;
ALTER ROLE monitor_reader SET default_transaction_read_only = on;

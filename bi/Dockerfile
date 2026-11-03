# Superset with the drivers it needs here: Trino (the lakehouse) and psycopg2 (its own metadata in platform-db)
FROM apache/superset:4.1.2
USER root
RUN pip install --no-cache-dir trino==0.340.0 psycopg2-binary==2.9.10
COPY superset_config.py /app/pythonpath/superset_config.py
COPY bootstrap.py /app/bootstrap.py
COPY start.sh /app/start.sh
USER superset
CMD ["/app/start.sh"]

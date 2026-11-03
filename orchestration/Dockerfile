# Dagster webserver and daemon for the StreamCart platform. The code (orchestration/streamcart_dagster), the platform
# package (platform/streamcart) and the specs (pipelines/) are mounted, so a spec change only needs a code reload.
FROM python:3.11-slim
COPY --from=platform requirements.txt /tmp/platform-requirements.txt
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
ENV DAGSTER_HOME=/opt/dagster/home PYTHONPATH=/opt/streamcart/platform:/opt/streamcart/orchestration
COPY dagster.yaml workspace.yaml /opt/dagster/home/
COPY entrypoint.sh /entrypoint.sh
WORKDIR /opt/dagster/home
ENTRYPOINT ["/entrypoint.sh"]

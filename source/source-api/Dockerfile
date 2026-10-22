FROM python:3.11-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY entrypoint.sh ./entrypoint.sh
ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus
EXPOSE 8100
CMD ["sh", "./entrypoint.sh"]

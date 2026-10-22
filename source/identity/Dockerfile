FROM python:3.11-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus
# client IPs from X-Forwarded-For: only the gateway and services on the compose network can reach this port
EXPOSE 8300
CMD ["sh", "-c", "mkdir -p $PROMETHEUS_MULTIPROC_DIR && rm -f $PROMETHEUS_MULTIPROC_DIR/*.db && exec uvicorn app.main:app --host 0.0.0.0 --port 8300 --workers 4 --proxy-headers --forwarded-allow-ips=*"]

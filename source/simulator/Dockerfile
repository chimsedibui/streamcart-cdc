# No database driver on purpose: the simulator can only change data through the Source API
FROM python:3.11-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config.yaml .
COPY sim ./sim
EXPOSE 8200
CMD ["uvicorn", "sim.server:app", "--host", "0.0.0.0", "--port", "8200"]

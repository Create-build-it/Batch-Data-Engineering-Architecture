FROM python:3.11-slim

WORKDIR /app

# librdkafka wird von confluent-kafka benötigt
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Kein Root-User -> bessere Container-Sicherheit
RUN useradd -m appuser
USER appuser

CMD ["python", "producer.py"]

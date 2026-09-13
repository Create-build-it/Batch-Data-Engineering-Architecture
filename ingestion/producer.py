"""
Ingestion-Microservice
=======================
Simuliert die quartalsweise Datenaufnahme historischer Flugdaten.
Liest einen CSV-Datensatz (z. B. von Kaggle, z. B. "Airline Delay and
Cancellation Data") ein und publiziert jede Zeile als JSON-Nachricht auf
ein Kafka-Topic. Kafka entkoppelt dabei die Ingestion vom nachgelagerten
Storage-Layer (HDFS) und puffert die Datenmenge zuverlässig.

Das Skript ist bewusst schema-agnostisch gehalten: Es übernimmt alle
Spalten der CSV unverändert und ergänzt zusätzlich einen
ingestion_timestamp (Pflicht laut Aufgabenstellung), sodass es mit
unterschiedlichen Flugdaten-Datensätzen funktioniert, ohne den Code
anpassen zu müssen.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ingestion-producer")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "flight-data-raw")
DATA_FILE = os.environ.get("DATA_FILE", "/data/flights.csv")
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "10000"))


def delivery_report(err, msg):
    if err is not None:
        logger.error("Zustellung fehlgeschlagen: %s", err)


def main():
    if not os.path.exists(DATA_FILE):
        logger.error(
            "Datendatei %s nicht gefunden. Bitte den Flugdaten-Datensatz "
            "(z. B. von Kaggle) unter ingestion/data/flights.csv ablegen.",
            DATA_FILE,
        )
        return

    producer_conf = {
        "bootst

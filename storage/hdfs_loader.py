"""
Storage-Microservice: Raw-Layer-Loader
=======================================
Konsumiert die von Kafka gepufferten Rohdaten und schreibt sie in
Batches als Parquet-Dateien in den HDFS Raw-Layer (/raw/flights).
Parquet wird bewusst gewählt (statt CSV), da es spaltenorientiert,
komprimiert und damit effizient von Spark im Processing-Layer gelesen
werden kann.

Die Kommunikation mit HDFS erfolgt über die WebHDFS-REST-API des
Namenode, wodurch keine native Hadoop-Client-Installation im Container
nötig ist (schlankes Image, einfache Wartbarkeit).
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
from confluent_kafka import Consumer
from hdfs import InsecureClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("hdfs-loader")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "flight-data-raw")
HDFS_NAMENODE_URL = os.environ.get("HDFS_NAMENODE_URL", "http://namenode:9870")
HDFS_RAW_PATH = os.environ.get("HDFS_RAW_PATH", "/raw/flights")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5000"))
POLL_TIMEOUT_SECONDS = 10.0
IDLE_FLUSH_SECONDS = 30


def get_hdfs_client() -> InsecureClient:
    client = InsecureClient(HDFS_NAMENODE_URL, user="root")
    client.makedirs(HDFS_RAW_PATH)
    return client


def flush_batch(client: InsecureClient, records: list) -> None:
    if not records:
        return
    df = pd.DataFrame(records)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    filename = f"{HDFS_RAW_PATH}/batch_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}.parquet"
    with client.write(filename) as writer:
        writer.write(buffer.read())

    logger.info("Batch mit %d Datensätzen nach %s geschrieben.", len(records), filename)


def main():
    consumer_conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "hdfs-loader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    hdfs_client = get_hdfs_client()
    logger.info("Verbunden mit HDFS Namenode %s, Raw-Layer-Pfad %s", HDFS_NAMENODE_URL, HDFS_RAW_PATH)

    buffer = []
    last_flush = time.time()

    try:
        while True:
            msg = consumer.poll(POLL_TIMEOUT_SECONDS)

            if msg is None:
                if buffer and (time.time() - last_flush) > IDLE_FLUSH_SECONDS:
                    flush_batch(hdfs_client, buffer)
                    buffer = []
                    last_flush = time.time()
                continue

            if msg.error():
                logger.error("Kafka-Fehler: %s", msg.error())
                continue

            record = json.loads(msg.value())
            buffer.append(record)

            if len(buffer) >= BATCH_SIZE:
                flush_batch(hdfs_client, buffer)
                buffer = []
                last_flush = time.time()

    except KeyboardInterrupt:
        logger.info("Loader wird beendet ...")
    finally:
        if buffer:
            flush_batch(hdfs_client, buffer)
        consumer.close()


if __name__ == "__main__":
    main()

"""
Ingestion-Microservice
=======================
Simuliert eine Flotte von IoT-Sensoren (z. B. Temperatur, Luftfeuchtigkeit,
Luftdruck), die kontinuierlich Messwerte erzeugen, und publiziert jede
Messung als JSON-Nachricht auf ein Kafka-Topic.

Jede Nachricht enthält einen Zeitstempel (Pflicht laut Aufgabenstellung)
sowie eine sensor_id, die als Kafka-Message-Key genutzt wird. Dadurch
landen alle Werte eines Sensors garantiert in derselben Partition und
somit in korrekter Reihenfolge (wichtig für spätere Windowing-Operationen).
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("sensor-producer")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sensor-readings")
NUM_SENSORS = int(os.environ.get("NUM_SENSORS", "10"))
EMIT_INTERVAL_SECONDS = float(os.environ.get("EMIT_INTERVAL_SECONDS", "1"))

SENSOR_TYPES = [
    {"type": "temperature", "unit": "celsius", "base": 21.0, "spread": 3.0},
    {"type": "humidity", "unit": "percent", "base": 45.0, "spread": 10.0},
    {"type": "pressure", "unit": "hpa", "base": 1013.0, "spread": 5.0},
]


def build_sensors(num_sensors: int):
    """Weist jedem simulierten Sensor einen Typ und eine feste ID zu."""
    sensors = []
    for i in range(num_sensors):
        sensor_type = SENSOR_TYPES[i % len(SENSOR_TYPES)]
        sensors.append({"sensor_id": f"sensor-{i:03d}", **sensor_type})
    return sensors


def generate_reading(sensor: dict) -> dict:
    """Erzeugt einen plausiblen, leicht verrauschten Messwert."""
    noise = random.gauss(0, sensor["spread"] / 4)
    value = round(sensor["base"] + noise, 2)
    return {
        "sensor_id": sensor["sensor_id"],
        "sensor_type": sensor["type"],
        "value": value,
        "unit": sensor["unit"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg):
    if err is not None:
        logger.error("Zustellung fehlgeschlagen: %s", err)
    else:
        logger.debug("Zugestellt an %s [%s]", msg.topic(), msg.partition())


def main():
    producer_conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "sensor-producer",
        # Zuverlässigkeit: warten bis alle In-Sync-Replicas bestätigt haben
        "acks": "all",
        "retries": 5,
        "linger.ms": 50,
    }
    producer = Producer(producer_conf)
    sensors = build_sensors(NUM_SENSORS)

    logger.info(
        "Starte Simulation von %d Sensoren -> Topic '%s' auf %s (Intervall: %ss)",
        NUM_SENSORS, KAFKA_TOPIC, KAFKA_BROKER, EMIT_INTERVAL_SECONDS,
    )

    try:
        while True:
            for sensor in sensors:
                reading = generate_reading(sensor)
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=reading["sensor_id"],
                    value=json.dumps(reading),
                    callback=delivery_report,
                )
            producer.poll(0)
            time.sleep(EMIT_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Producer wird beendet ...")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()

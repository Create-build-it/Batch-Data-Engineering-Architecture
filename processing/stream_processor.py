"""
Processing-Microservice (Kafka Streams via Faust)
==================================================
Konsumiert rohe Sensor-Messwerte aus Kafka, aggregiert sie in einem
Tumbling Window pro Sensor (Windowing-Funktion: min / max / avg / count)
und schreibt die Aggregate anschließend

  a) zurück nach Kafka (Topic "sensor-aggregates") -> für weitere
     Konsumenten / künftige zweite Pipeline, und
  b) direkt in die TimescaleDB (Storage-Layer) -> für das
     Echtzeit-Reporting.

Windowing-Strategie: Tumbling Window (nicht überlappend, feste Breite
WINDOW_SECONDS).
"""

import asyncio
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean

import asyncpg
import faust

logger = logging.getLogger("stream-processor")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "sensor-readings")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "sensor-aggregates")
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "10"))

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.environ.get("POSTGRES_USER", "streaming_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "streaming_pw")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "sensor_data")


app = faust.App(
    "sensor-stream-processor",
    broker=f"kafka://{KAFKA_BROKER}",
    value_serializer="raw",
    topic_partitions=3,
)


class SensorReading(faust.Record, serializer="json"):
    sensor_id: str
    sensor_type: str
    value: float
    unit: str
    timestamp: str


class SensorAggregate(faust.Record, serializer="json"):
    sensor_id: str
    sensor_type: str
    unit: str
    window_start: str
    window_end: str
    avg_value: float
    min_value: float
    max_value: float
    sample_count: int


raw_topic = app.topic(INPUT_TOPIC, value_type=SensorReading)
aggregate_topic = app.topic(OUTPUT_TOPIC, value_type=SensorAggregate)

_window_buffer: dict[str, list[SensorReading]] = defaultdict(list)
_pg_pool: asyncpg.Pool | None = None


@app.agent(raw_topic)
async def consume_readings(readings):
    """Nimmt jede eintreffende Messung entgegen und puffert sie
    für das aktuell laufende Aggregations-Fenster."""
    async for reading in readings:
        _window_buffer[reading.sensor_id].append(reading)


async def get_pg_pool() -> asyncpg.Pool:
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = await asyncpg.create_pool(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB,
            min_size=1,
            max_size=5,
        )
    return _pg_pool


async def persist_aggregate(agg: SensorAggregate) -> None:
    pool = await get_pg_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO sensor_aggregates
                (window_start, window_end, sensor_id, sensor_type, unit,
                 avg_value, min_value, max_value, sample_count)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            datetime.fromisoformat(agg.window_start),
            datetime.fromisoformat(agg.window_end),
            agg.sensor_id,
            agg.sensor_type,
            agg.unit,
            agg.avg_value,
            agg.min_value,
            agg.max_value,
            agg.sample_count,
        )


@app.timer(interval=WINDOW_SECONDS)
async def flush_window():
    """Schließt alle WINDOW_SECONDS das aktuelle Tumbling Window:
    berechnet Aggregate pro Sensor und schreibt sie nach Kafka + TimescaleDB."""
    if not _window_buffer:
        return

    window_end = datetime.now(timezone.utc)
    window_start = window_end.timestamp() - WINDOW_SECONDS
    window_start = datetime.fromtimestamp(window_start, tz=timezone.utc)

    sensors_to_flush = list(_window_buffer.items())
    _window_buffer.clear()

    for sensor_id, readings in sensors_to_flush:
        if not readings:
            continue
        values = [r.value for r in readings]
        agg = SensorAggregate(
            sensor_id=sensor_id,
            sensor_type=readings[0].sensor_type,
            unit=readings[0].unit,
            window_start=window_start.isoformat(),
            window_end=window_end.isoformat(),
            avg_value=round(mean(values), 3),
            min_value=round(min(values), 3),
            max_value=round(max(values), 3),
            sample_count=len(values),
        )

        await aggregate_topic.send(key=sensor_id, value=agg)

        try:
            await persist_aggregate(agg)
        except Exception:
            logger.exception("Konnte Aggregat für %s nicht in TimescaleDB schreiben", sensor_id)

    logger.info("Fenster geschlossen: %d Sensoren aggregiert (%s - %s)",
                len(sensors_to_flush), window_start.isoformat(), window_end.isoformat())


@app.on_shutdown()
async def close_pool(app):
    global _pg_pool
    if _pg_pool is not None:
        await _pg_pool.close()


if __name__ == "__main__":
    app.main()

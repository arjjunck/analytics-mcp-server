"""
Kafka Consumer — Analytics Event Ingestion
==========================================
Consumes events from the 'analytics.events' Kafka topic and writes
them to raw.events in PostgreSQL in real time.

Run alongside the MCP server:
    python kafka_consumer.py

Mirrors the real-time ingestion pattern used at Stripe / HubSpot Dublin.
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime

import asyncpg
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("kafka-consumer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "analytics.events")
KAFKA_GROUP     = os.getenv("KAFKA_GROUP_ID", "analytics-mcp-group")
DATABASE_URL    = os.getenv("DATABASE_URL", "postgresql://localhost:5432/analytics_mcp")

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)
    return _pool


async def insert_event(pool: asyncpg.Pool, event: dict) -> None:
    """Write one event record to raw.events."""
    await pool.execute(
        """
        INSERT INTO raw.events (user_id, event_type, properties, created_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
        """,
        event.get("user_id"),
        event.get("event_type"),
        json.dumps(event.get("properties", {})),
        datetime.fromisoformat(event["created_at"]) if "created_at" in event else datetime.utcnow(),
    )


async def consume(stop_event: asyncio.Event) -> None:
    pool = await get_pool()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=1000,   # non-blocking poll
    )

    log.info("Consumer started — topic=%s  group=%s", KAFKA_TOPIC, KAFKA_GROUP)
    ingested = 0

    try:
        while not stop_event.is_set():
            for message in consumer:
                if stop_event.is_set():
                    break
                try:
                    await insert_event(pool, message.value)
                    ingested += 1
                    if ingested % 100 == 0:
                        log.info("Ingested %d events (partition=%d offset=%d)",
                                 ingested, message.partition, message.offset)
                except Exception as exc:
                    log.warning("Failed to insert event: %s — %s", message.value, exc)
            await asyncio.sleep(0.1)
    finally:
        consumer.close()
        await pool.close()
        log.info("Consumer stopped. Total ingested: %d", ingested)


def main() -> None:
    stop_event = asyncio.Event()

    def _handle_signal(*_):
        log.info("Shutdown signal received…")
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    asyncio.run(consume(stop_event))


if __name__ == "__main__":
    main()

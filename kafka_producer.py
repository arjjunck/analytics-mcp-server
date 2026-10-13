"""
Kafka Producer — Synthetic Event Generator
==========================================
Simulates a live event stream: page_view, click, purchase, feature_use.
Use this to demo / test the consumer without a real front-end.

Run:
    python kafka_producer.py            # continuous stream
    python kafka_producer.py --count 50 # send 50 events and exit
"""

import argparse
import json
import logging
import os
import random
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("kafka-producer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "analytics.events")

EVENT_TYPES = ["page_view", "click", "purchase", "feature_use", "sign_up", "logout"]
FEATURES    = ["dashboard", "reports", "export", "api_key", "billing", "integrations"]
PAGES       = ["/home", "/dashboard", "/reports", "/settings", "/pricing", "/docs"]


def make_event(user_id: int) -> dict:
    event_type = random.choice(EVENT_TYPES)
    props: dict = {"session_id": f"s_{random.randint(1000, 9999)}"}

    if event_type == "page_view":
        props["page"] = random.choice(PAGES)
    elif event_type == "click":
        props["element"] = random.choice(["button", "link", "nav", "cta"])
        props["page"] = random.choice(PAGES)
    elif event_type == "purchase":
        props["amount"] = round(random.uniform(29.0, 499.0), 2)
        props["plan"]   = random.choice(["starter", "pro", "enterprise"])
    elif event_type == "feature_use":
        props["feature"] = random.choice(FEATURES)

    return {
        "user_id":    user_id,
        "event_type": event_type,
        "properties": props,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def run(count: int | None, delay: float) -> None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
    except NoBrokersAvailable:
        log.error("No Kafka broker at %s — is Kafka running?", KAFKA_BOOTSTRAP)
        return

    log.info("Producer started — topic=%s", KAFKA_TOPIC)
    sent = 0

    try:
        while count is None or sent < count:
            user_id = random.randint(1, 100)
            event   = make_event(user_id)
            producer.send(KAFKA_TOPIC, event)
            sent += 1
            if sent % 10 == 0:
                producer.flush()
                log.info("Sent %d events", sent)
            time.sleep(delay)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        log.info("Producer stopped. Total sent: %d", sent)


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic Kafka event producer")
    parser.add_argument("--count", type=int, default=None,
                        help="Number of events to send (omit for continuous)")
    parser.add_argument("--delay", type=float, default=0.2,
                        help="Seconds between events (default 0.2)")
    args = parser.parse_args()
    run(args.count, args.delay)


if __name__ == "__main__":
    main()

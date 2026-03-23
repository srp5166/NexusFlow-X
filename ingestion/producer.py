"""Send synthetic events to Kafka topic nexusflow-events."""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

from kafka import KafkaProducer

from ingestion.event_generator import generate_events_batch

IN_DOCKER = Path("/.dockerenv").exists()
KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "kafka:9092" if IN_DOCKER else "127.0.0.1:29092",
)
KAFKA_TOPIC = "nexusflow-events"


def send_events_to_kafka(events, broker=KAFKA_BROKER, topic=KAFKA_TOPIC):
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for event in events:
        producer.send(topic, value=event)
    producer.flush()
    print(f"Sent {len(events)} events to Kafka topic '{topic}'")


if __name__ == "__main__":
    print("Starting Kafka producer...")
    while True:
        send_events_to_kafka(generate_events_batch(10))
        time.sleep(1)

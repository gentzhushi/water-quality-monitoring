import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "water-quality-readings")

PH_VALUES = [6.9, 7.2, 7.4, 8.1, 6.2, 8.9]
TEMPERATURE_VALUES = [18.5, 21.0, 23.5, 28.2, -2.0, 42.0]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def delivery_report(error, message) -> None:
    if error is not None:
        print(f"Failed to deliver reading: {error}", flush=True)


def connect() -> Producer:
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "water-quality-mock-producer",
        }
    )


def reading(sensor_id: str, sensor_type: str, value: float) -> dict:
    return {
        "sensor_id": sensor_id,
        "sensor_type": sensor_type,
        "value": value,
        "timestamp": utc_now(),
    }


def main() -> None:
    producer = connect()
    print(f"Mock producer sending readings to {TOPIC} via {BOOTSTRAP_SERVERS}", flush=True)

    while True:
        messages = [
            reading("mock_sensor_01", "pH", random.choice(PH_VALUES)),
            reading("mock_sensor_02", "temperature", random.choice(TEMPERATURE_VALUES)),
        ]

        for message in messages:
            producer.produce(
                TOPIC,
                key=message["sensor_id"],
                value=json.dumps(message),
                callback=delivery_report,
            )
            print(f"Sent reading: {message}", flush=True)

        producer.flush()
        time.sleep(random.uniform(1, 2))


if __name__ == "__main__":
    main()

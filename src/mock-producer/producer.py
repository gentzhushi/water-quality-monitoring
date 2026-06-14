import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "water-quality-readings")

SENSOR_PROFILES = [
    {
        "sensor_id": "mock_sensor_01",
        "sensor_type": "pH",
        "values": [6.9, 7.2, 7.4, 8.1, 6.2, 8.9, 5.8, 9.2],
    },
    {
        "sensor_id": "mock_sensor_02",
        "sensor_type": "temperature",
        "values": [18.5, 21.0, 23.5, 28.2, -2.0, 36.5, -7.0, 42.0],
    },
    {
        "sensor_id": "mock_sensor_03",
        "sensor_type": "turbidity",
        "values": [1.2, 2.1, 3.5, 4.0, 6.5, 9.0, 24.0],
    },
    {
        "sensor_id": "mock_sensor_04",
        "sensor_type": "conductivity",
        "values": [250.0, 600.0, 900.0, 1200.0, 35.0, 1800.0, 2600.0],
    },
    {
        "sensor_id": "mock_sensor_05",
        "sensor_type": "dissolved_oxygen",
        "values": [6.2, 7.1, 8.4, 9.3, 4.2, 2.6, 15.5, 19.0],
    },
    {
        "sensor_id": "mock_sensor_06",
        "sensor_type": "ORP",
        "values": [220.0, 310.0, 420.0, 480.0, 120.0, 40.0, 720.0],
    },
]


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
            reading(
                profile["sensor_id"],
                profile["sensor_type"],
                random.choice(profile["values"]),
            )
            for profile in SENSOR_PROFILES
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

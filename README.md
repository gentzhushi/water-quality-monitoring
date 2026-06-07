# water-quality-monitoring
Reference implementation for a Semantic Digital Twin-based system for intelligent real-time water quality monitoring, integrating IoT, stream processing, semantic technologies, and machine learning.

## Run the local Docker demo

First off, make sure you have `docker` and `docker compose`.

1. Clone the repository and `cd` into it:
```sh
git clone git@github.com:gentzhushi/water-quality-monitoring && \
cd water-quality-monitoring
```

2. Start the services from the `src` directory:
```sh
cd src
docker compose up --build
```

This starts the existing Flask/sensor services plus an independent Kafka/Spark test pipeline:

```text
mock-producer -> Kafka topic water-quality-readings -> Spark -> Kafka topic water-quality-alerts -> Kafka UI
```

## Services

- Flask server: http://localhost:8000
- Kafka UI: http://localhost:8080
- Kafka broker inside Docker: `kafka:9092`
- Kafka broker from the host, if needed: `localhost:9094`

## Kafka/Spark topics

- `water-quality-readings`: fake readings produced by `mock-producer`
- `water-quality-alerts`: abnormal readings detected by Spark

The mock producer sends both pH and temperature messages every 1-2 seconds. It intentionally includes normal and abnormal values so Spark can create alerts.

Alert thresholds:

- pH below `6.5`: `LOW_PH`
- pH above `8.5`: `HIGH_PH`
- temperature below `0`: `LOW_TEMPERATURE`
- temperature above `35`: `HIGH_TEMPERATURE`

## Verify with Kafka UI

1. Open http://localhost:8080.
2. Select the `local` Kafka cluster.
3. Open the `water-quality-readings` topic and confirm messages are arriving.
4. Open the `water-quality-alerts` topic and confirm Spark is writing alert messages when abnormal values appear.

To manually test Spark, use Kafka UI to produce this message to `water-quality-readings`:

```json
{
  "sensor_id": "manual_test_01",
  "sensor_type": "pH",
  "value": 9.2,
  "timestamp": "2026-06-07T12:00:00Z"
}
```

Expected result: Spark writes a `HIGH_PH` alert to `water-quality-alerts`.

Another manual test:

```json
{
  "sensor_id": "manual_test_02",
  "sensor_type": "temperature",
  "value": 42,
  "timestamp": "2026-06-07T12:00:00Z"
}
```

Expected result: Spark writes a `HIGH_TEMPERATURE` alert to `water-quality-alerts`.

## Useful logs

From `src`:

```sh
docker compose logs -f mock-producer
docker compose logs -f spark
docker compose logs -f kafka
```

## Later integration

The Kafka/Spark path is intentionally separate from the existing sensor-to-Flask flow. Later, the real sensor generator can replace `mock-producer`, or the Flask server can be extended to publish received measurements into Kafka before Spark processes them.

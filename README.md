# water-quality-monitoring
Reference implementation for a Semantic Digital Twin-based system for intelligent real-time water quality monitoring, integrating IoT, stream processing, semantic technologies, and machine learning.

## Run the local Docker demo

First off, make sure you have `docker` and `docker compose`.

1. Clone the repository and `cd` into it:
```sh
git clone git@github.com:gentzhushi/water-quality-monitoring && \
cd water-quality-monitoring/src
```

2. Start the services from the `src` directory:
```sh
cd src
docker compose up -d --build
```

This starts the existing Flask/sensor services, Kafka, Cassandra, and a small Spark standalone cluster:

```text
mock-producer -> Kafka topic water-quality-readings -> Spark master/worker cluster -> Cassandra
                                                         -> Kafka water-quality-alerts -> notification-service
```

The Cassandra schema and Spark streaming job start automatically. No manual `cqlsh` or `spark-submit` command is needed for the normal demo.

The notification service also starts automatically. By default it runs in dry-run mode, so it prints the email it would send instead of connecting to SMTP.

## Services

- Flask server: http://localhost:8000
- Digital Twin dashboard: http://localhost:8001/digital-twin
- Digital Twin API docs: http://localhost:8001/docs
- Kafka UI: http://localhost:8080
- Spark Master UI: http://localhost:8081
- Spark Worker UI: http://localhost:8082
- Spark Application UI: http://localhost:4040
- Notification service: consumes `water-quality-alerts` and sends Gmail SMTP email notifications and optional Telegram bot notifications
- Kafka broker inside Docker: `kafka:9092`
- Kafka broker from the host, if needed: `localhost:9094`

The Spark Application UI on port `4040` appears only while the streaming job is running.

## Spark streaming job

The Spark streaming job is submitted automatically by the `spark-master` container after Kafka and Cassandra are ready.

This keeps the demo flow simple:

```text
Kafka water-quality-readings -> Spark validation/enrichment/aggregation -> Cassandra
Kafka water-quality-readings -> Spark alert detection -> Cassandra alerts_by_sensor_day
Kafka water-quality-readings -> Spark alert detection -> Kafka water-quality-alerts -> notification-service
Spark alert/AI/performance outputs -> Cassandra -> Digital Twin dashboard
```

The Spark consumer keeps the current reading message schema unchanged. It reads JSON from Kafka, parses the fields, filters out broken messages, enriches valid readings with Cassandra sensor metadata, writes dashboard-ready readings and aggregates to Cassandra, prints useful live output to the Spark logs, writes alert history to Cassandra, and writes enriched alerts back to Kafka.

Spark also writes read-only Digital Twin outputs to Cassandra:

- alert history by sensor/day and location/day
- explainable statistical anomaly scores
- latest AI/anomaly state by location
- per-minute pipeline performance metrics

For troubleshooting only, the automatic submit command is:

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.ui.port=4040 `
  --conf spark.jars.ivy=/tmp/spark-ivy `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 `
  --conf spark.cassandra.connection.host=cassandra `
  --conf spark.cassandra.connection.port=9042 `
  /opt/spark-apps/streaming_job.py
```

## Kafka/Spark topics

- `water-quality-readings`: fake readings produced by `mock-producer`
- `water-quality-alerts`: enriched abnormal-reading alerts detected by Spark and consumed by `notification-service`

Current reading message format:

```json
{
  "sensor_id": "mock_sensor_01",
  "sensor_type": "pH",
  "value": 8.9,
  "timestamp": "2026-06-09T21:37:59Z"
}
```

Spark treats `sensor_id`, `sensor_type`, `value`, and `timestamp` as required fields.

The mock producer sends both pH and temperature messages every 1-2 seconds. It intentionally includes normal and abnormal values so Spark can create alerts.

Alert thresholds:

- pH below `6.5`: `LOW_PH`
- pH above `8.5`: `HIGH_PH`
- temperature below `0`: `LOW_TEMPERATURE`
- temperature above `35`: `HIGH_TEMPERATURE`

Alert messages include the sensor id, location id, location name, parameter, value, unit, alert type, severity, expected threshold range, event time, and processing time. The notification service uses this Kafka message directly and does not query Cassandra when sending an email.

## Email notifications with Gmail SMTP

The notification service uses Python's built-in SMTP support. For a personal Gmail account, use `smtp.gmail.com` with STARTTLS on port `587`.

Use a Google App Password for `SMTP_PASSWORD`, not your normal Gmail password. Google App Passwords require 2-Step Verification on the Gmail account.

Dry-run mode is enabled by default:

```text
NOTIFICATION_DRY_RUN=true
```

To send real email, create `src/.env` and set:

```sh
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your_email@gmail.com
SMTP_PASSWORD=your_google_app_password
SMTP_SENDER_EMAIL=your_email@gmail.com
SMTP_SENDER_NAME=Water Quality Monitoring
EMAIL_ENABLED=true
NOTIFICATION_RECIPIENTS=person1@example.com,person2@example.com
NOTIFICATION_DRY_RUN=false
ALERT_COOLDOWN_SECONDS=300
```

Set `EMAIL_ENABLED=false` to disable email notifications entirely. The `src/.env` file is ignored by Git, so the Gmail App Password stays local. If dry-run mode is enabled, the service logs the email content instead of sending it.

Sent email notifications include a styled HTML email and a plain-text fallback for clients that do not render HTML.

## Telegram notifications with a bot

Telegram notifications are optional and disabled by default. The notification service uses Telegram long polling, so the local Docker demo does not need a public webhook URL.

Create a bot with BotFather, then add the token to `src/.env`:

```sh
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_SUBSCRIBERS_FILE=/app/data/telegram_subscribers.json
TELEGRAM_POLL_SECONDS=5
```

Start the stack and send `/start` to the bot from Telegram. The notification service stores the Telegram chat id in the `notification_data` Docker volume. Send `/stop` to unsubscribe and `/help` to see the supported commands. Set `TELEGRAM_ENABLED=false` to disable both Telegram bot polling and Telegram alert sending.

If `NOTIFICATION_DRY_RUN=true`, alert messages are logged instead of being sent to Telegram subscribers. The bot can still receive `/start` and `/stop` commands when Telegram is enabled.

## Verify with Kafka UI

1. Open http://localhost:8080.
2. Select the `local` Kafka cluster.
3. Open the `water-quality-readings` topic and confirm messages are arriving.
4. Open the `water-quality-alerts` topic and confirm Spark is writing alert messages when abnormal values appear.
5. Check `docker compose logs -f notification-service` and confirm the notification service logs dry-run emails, dry-run Telegram alerts, or send results.

To manually test Spark, use Kafka UI to produce this message to `water-quality-readings`:

```json
{
  "sensor_id": "mock_sensor_01",
  "sensor_type": "pH",
  "value": 9.2,
  "timestamp": "2026-06-07T12:00:00Z"
}
```

Expected result: Spark writes a `HIGH_PH` alert to `water-quality-alerts`.

Another manual test:

```json
{
  "sensor_id": "mock_sensor_02",
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
docker compose logs -f cassandra
docker compose logs -f spark-master
docker compose logs -f spark-worker
docker compose logs -f kafka
docker compose logs -f notification-service
docker compose logs -f dashboard-backend
```

## Digital Twin dashboard

Open http://localhost:8001/digital-twin after the stack has been running for a minute or two.

The dashboard is read-only and is separate from the sensor control dashboard. It reads Cassandra through the `dashboard-backend` service and shows:

- Overview: latest sensor state, active alerts, system status, recent metrics
- Readings: processed Cassandra readings by sensor/day
- Alarms: grouped alarm view derived from alert history
- AI Insights: rolling average, rolling standard deviation, z-score, rate of change, anomaly score, and explanation
- Performance: processed readings, alert counts, anomaly scores, and estimated event latency by minute

AI scores are explainable statistical scores rather than a trained ML model. Spark combines:

- threshold distance: how far the value is outside the allowed pH/temperature range
- statistical difference: how unusual the value is compared with the recent rolling window
- rate of change: how quickly the value moved compared with the previous reading

The final anomaly level is `NORMAL`, `WATCH`, `WARNING`, or `CRITICAL`. A rule breach is always raised to at least `WARNING`, and a critical pH/temperature breach is raised to `CRITICAL`, so the dashboard label matches the explanation shown to the user.

## Later integration

The Kafka/Spark path is intentionally separate from the existing sensor-to-Flask flow. Later, the real sensor generator can replace `mock-producer`, or the Flask server can be extended to publish received measurements into Kafka before Spark processes them.

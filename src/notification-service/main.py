import json

from confluent_kafka import Consumer

from config import load_config
from email_channel import EmailChannel
from router import NotificationRouter


def create_consumer(config):
    consumer = Consumer(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "group.id": config.kafka_group_id,
            "client.id": "water-quality-notification-service",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([config.alerts_topic])
    return consumer


def parse_alert(message):
    value = message.value().decode("utf-8")
    return json.loads(value)


def main():
    config = load_config()
    router = NotificationRouter(config.alert_cooldown_seconds)
    email_channel = EmailChannel(config)
    consumer = create_consumer(config)

    print(f"Notification service started: {config.describe()}", flush=True)

    try:
        while True:
            message = consumer.poll(config.poll_timeout_seconds)
            if message is None:
                continue

            if message.error():
                print(f"Kafka consumer error: {message.error()}", flush=True)
                continue

            try:
                alert = parse_alert(message)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                print(f"Skipping invalid alert message: {error}", flush=True)
                continue

            should_send, reason = router.should_send(alert)
            if not should_send:
                print(f"Skipping notification: {reason}", flush=True)
                continue

            sent = email_channel.send(alert)
            if sent:
                router.mark_sent(alert)

    except KeyboardInterrupt:
        print("Notification service stopping", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

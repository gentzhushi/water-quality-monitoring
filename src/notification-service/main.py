import json
import threading

from confluent_kafka import Consumer

from config import load_config
from email_channel import EmailChannel
from router import NotificationRouter
from telegram_bot import TelegramBot
from telegram_channel import TelegramChannel
from telegram_subscribers import TelegramSubscribers


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


def create_notification_channels(config):
    channels = []
    telegram_bot = None

    if config.email_enabled:
        channels.append(EmailChannel(config))
    else:
        print("Email notifications are disabled", flush=True)

    if not config.telegram_enabled:
        print("Telegram notifications are disabled", flush=True)
    elif not config.telegram_bot_token:
        print(
            "Telegram notifications are enabled but TELEGRAM_BOT_TOKEN is missing",
            flush=True,
        )
    else:
        subscribers = TelegramSubscribers(config.telegram_subscribers_file)
        channels.append(TelegramChannel(config, subscribers))
        telegram_bot = TelegramBot(config, subscribers)

    if not channels:
        print(
            "No notification channels are enabled; alerts will be consumed but not sent",
            flush=True,
        )

    return channels, telegram_bot


def start_telegram_bot(telegram_bot):
    if telegram_bot is None:
        return

    thread = threading.Thread(target=telegram_bot.run, daemon=True)
    thread.start()


def send_to_channels(channels, alert):
    sent = False

    for channel in channels:
        if channel.send(alert):
            sent = True

    return sent


def main():
    config = load_config()
    router = NotificationRouter(config.alert_cooldown_seconds)
    channels, telegram_bot = create_notification_channels(config)
    consumer = create_consumer(config)

    print(f"Notification service started: {config.describe()}", flush=True)
    start_telegram_bot(telegram_bot)

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

            sent = send_to_channels(channels, alert)
            if sent:
                router.mark_sent(alert)
            else:
                print("No notification channel sent this alert", flush=True)

    except KeyboardInterrupt:
        print("Notification service stopping", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

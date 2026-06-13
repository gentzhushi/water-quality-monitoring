import os


def read_bool(name, default):
    value = os.getenv(name)
    if value is None:
        return default

    return value.strip().lower() in ("1", "true", "yes", "on")


def read_int(name, default):
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default

    return int(value)


def read_recipients():
    value = os.getenv("NOTIFICATION_RECIPIENTS", "")
    recipients = []

    for item in value.split(","):
        email = item.strip()
        if email:
            recipients.append(email)

    return recipients


class Config:
    def __init__(self):
        self.kafka_bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "kafka:9092",
        )
        self.alerts_topic = os.getenv("ALERTS_TOPIC", "water-quality-alerts")
        self.kafka_group_id = os.getenv(
            "KAFKA_GROUP_ID",
            "water-quality-notification-service",
        )

        self.smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = read_int("SMTP_PORT", 587)
        self.smtp_username = os.getenv("SMTP_USERNAME", "")
        self.smtp_password = os.getenv("SMTP_PASSWORD", "")
        self.smtp_sender_email = (
            os.getenv("SMTP_SENDER_EMAIL", "") or self.smtp_username
        )
        self.smtp_sender_name = os.getenv(
            "SMTP_SENDER_NAME",
            "Water Quality Monitoring",
        )

        self.recipients = read_recipients()
        self.dry_run = read_bool("NOTIFICATION_DRY_RUN", True)
        self.alert_cooldown_seconds = read_int("ALERT_COOLDOWN_SECONDS", 300)
        self.poll_timeout_seconds = float(
            os.getenv("KAFKA_POLL_TIMEOUT_SECONDS", "1.0")
        )

    def describe(self):
        return {
            "kafka_bootstrap_servers": self.kafka_bootstrap_servers,
            "alerts_topic": self.alerts_topic,
            "kafka_group_id": self.kafka_group_id,
            "dry_run": self.dry_run,
            "recipient_count": len(self.recipients),
            "alert_cooldown_seconds": self.alert_cooldown_seconds,
            "smtp_host": self.smtp_host,
            "smtp_port": self.smtp_port,
            "smtp_username_configured": bool(self.smtp_username),
            "smtp_password_configured": bool(self.smtp_password),
            "smtp_sender_email_configured": bool(self.smtp_sender_email),
        }


def load_config():
    return Config()

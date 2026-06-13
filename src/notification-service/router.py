import time


class NotificationRouter:
    def __init__(self, cooldown_seconds):
        self.cooldown_seconds = cooldown_seconds
        self.last_sent_at = {}

    def alert_key(self, alert):
        return (
            alert.get("sensor_id", "unknown-sensor"),
            alert.get("alert_type", "unknown-alert"),
        )

    def should_send(self, alert):
        key = self.alert_key(alert)
        now = time.time()
        last_sent = self.last_sent_at.get(key)

        if last_sent is None:
            return True, ""

        seconds_since_last_send = now - last_sent
        if seconds_since_last_send >= self.cooldown_seconds:
            return True, ""

        seconds_left = int(self.cooldown_seconds - seconds_since_last_send)
        reason = f"cooldown active for {key[0]} / {key[1]} ({seconds_left}s left)"
        return False, reason

    def mark_sent(self, alert):
        key = self.alert_key(alert)
        self.last_sent_at[key] = time.time()

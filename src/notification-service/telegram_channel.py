import requests

from telegram_template import build_telegram_message


class TelegramChannel:
    def __init__(self, config, subscribers):
        self.config = config
        self.subscribers = subscribers
        self.base_url = f"https://api.telegram.org/bot{config.telegram_bot_token}"

    def send(self, alert):
        if not self.config.telegram_bot_token:
            print("Cannot send Telegram alert: TELEGRAM_BOT_TOKEN is missing", flush=True)
            return False

        chat_ids = self.subscribers.list_chat_ids()
        if not chat_ids:
            print("No Telegram subscribers configured; skipping Telegram alert", flush=True)
            return False

        message = build_telegram_message(alert)

        if self.config.dry_run:
            print("Notification dry run: Telegram alerts were not sent", flush=True)
            print(f"Telegram subscriber count: {len(chat_ids)}", flush=True)
            print(message, flush=True)
            return True

        sent_count = 0
        for chat_id in chat_ids:
            if self.send_to_chat(chat_id, message):
                sent_count += 1

        if sent_count:
            print(f"Telegram alert sent to {sent_count} subscriber(s)", flush=True)
            return True

        return False

    def send_to_chat(self, chat_id, text):
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        }

        try:
            response = requests.post(url, json=payload, timeout=10)
        except requests.RequestException as error:
            print(f"Telegram send failed for chat {chat_id}: {error}", flush=True)
            return False

        if response.ok:
            return True

        error_text = telegram_error_text(response)
        print(f"Telegram send failed for chat {chat_id}: {error_text}", flush=True)

        if response.status_code in (400, 403):
            removed = self.subscribers.remove_chat_id(chat_id)
            if removed:
                print(f"Removed invalid Telegram subscriber {chat_id}", flush=True)

        return False


def telegram_error_text(response):
    try:
        data = response.json()
    except ValueError:
        return f"HTTP {response.status_code}: {response.text}"

    description = data.get("description", response.text)
    return f"HTTP {response.status_code}: {description}"

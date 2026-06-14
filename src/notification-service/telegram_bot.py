import time

import requests


class TelegramBot:
    def __init__(self, config, subscribers):
        self.config = config
        self.subscribers = subscribers
        self.base_url = f"https://api.telegram.org/bot{config.telegram_bot_token}"
        self.offset = 0

    def run(self):
        print("Telegram bot polling started", flush=True)

        while True:
            try:
                updates = self.get_updates()
            except (requests.RequestException, ValueError) as error:
                print(f"Telegram polling failed: {error}", flush=True)
                time.sleep(self.poll_seconds())
                continue

            for update in updates:
                self.offset = max(self.offset, update.get("update_id", 0) + 1)
                self.handle_update(update)

    def poll_seconds(self):
        return max(1, self.config.telegram_poll_seconds)

    def get_updates(self):
        url = f"{self.base_url}/getUpdates"
        params = {
            "offset": self.offset,
            "timeout": self.poll_seconds(),
        }

        response = requests.get(
            url,
            params=params,
            timeout=self.poll_seconds() + 10,
        )
        response.raise_for_status()

        data = response.json()
        if not data.get("ok"):
            print(f"Telegram getUpdates returned an error: {data}", flush=True)
            return []

        return data.get("result", [])

    def handle_update(self, update):
        message = update.get("message")
        if not message:
            return

        chat = message.get("chat", {})
        chat_id = chat.get("id")
        text = message.get("text", "").strip()

        if chat_id is None or not text:
            return

        command = text.split()[0].split("@")[0].lower()

        if command == "/start":
            added = self.subscribers.add_chat_id(chat_id)
            if added:
                print(f"Added Telegram subscriber {chat_id}", flush=True)
            self.reply(chat_id, "You are subscribed to water quality alerts.")
            return

        if command == "/stop":
            removed = self.subscribers.remove_chat_id(chat_id)
            if removed:
                print(f"Removed Telegram subscriber {chat_id}", flush=True)
            self.reply(chat_id, "You are unsubscribed from water quality alerts.")
            return

        if command == "/help":
            self.reply(
                chat_id,
                "Commands:\n/start - subscribe to alerts\n/stop - unsubscribe",
            )

    def reply(self, chat_id, text):
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
        }

        try:
            response = requests.post(url, json=payload, timeout=10)
        except requests.RequestException as error:
            print(f"Telegram reply failed for chat {chat_id}: {error}", flush=True)
            return

        if not response.ok:
            print(
                f"Telegram reply failed for chat {chat_id}: HTTP {response.status_code}",
                flush=True,
            )

import json
import os
import threading


class TelegramSubscribers:
    def __init__(self, file_path):
        self.file_path = file_path
        self.chat_ids = set()
        self.lock = threading.Lock()
        self.load()

    def load(self):
        if not os.path.exists(self.file_path):
            return

        try:
            with open(self.file_path, "r", encoding="utf-8") as file:
                data = json.load(file)
        except (OSError, json.JSONDecodeError) as error:
            print(f"Could not read Telegram subscribers file: {error}", flush=True)
            return

        for chat_id in data.get("chat_ids", []):
            self.chat_ids.add(str(chat_id))

    def save(self):
        folder = os.path.dirname(self.file_path)
        if folder:
            os.makedirs(folder, exist_ok=True)

        data = {"chat_ids": sorted(self.chat_ids)}
        temp_path = f"{self.file_path}.tmp"

        with open(temp_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=2)

        os.replace(temp_path, self.file_path)

    def add_chat_id(self, chat_id):
        chat_id = str(chat_id)

        with self.lock:
            if chat_id in self.chat_ids:
                return False

            self.chat_ids.add(chat_id)
            self.save()
            return True

    def remove_chat_id(self, chat_id):
        chat_id = str(chat_id)

        with self.lock:
            if chat_id not in self.chat_ids:
                return False

            self.chat_ids.remove(chat_id)
            self.save()
            return True

    def list_chat_ids(self):
        with self.lock:
            return sorted(self.chat_ids)

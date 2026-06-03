from dataclasses import dataclass
from enum import Enum
from random import randrange
import requests
from typing import List, Self
import yaml


@dataclass(frozen=True)
class Sensor:
    name: str
    type: str
    rate: int
    min:  int
    max:  int
    endpoint: str

    def measure(self, value: int | None = None) -> requests.Response:
        if value is None:
            value = randrange(self.min, self.max)

        return requests.post(
            self.endpoint,
            json={
                "sensor_name": self.name,
                "sensor_type": self.type,
                "value": value
                }
            )


@dataclass(frozen=True)
class SensorConfig:
    sensors: List[Sensor]

    @classmethod
    def load_cfg(cls, file_path: str) -> Self:
        with open(file_path) as f:
            cfg = yaml.safe_load(f)

        sensors = [Sensor(**s) for s in cfg["sensors"]]

        return cls(sensors)

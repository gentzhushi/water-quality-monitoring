from dataclasses import dataclass
from enum import Enum
from typing import List, Self
import yaml


class SensorType(Enum):
    HVAC = 1
    # OTHER_TYPE = 2 ...
    # rest of sensor types


@dataclass(frozen=True)
class Sensor:
    name: str
    type: SensorType
    rate: int


@dataclass(frozen=True)
class SensorConfig:
    sensors: List[Sensor]

    @classmethod
    def load_cfg(cls, file_path: str) -> Self:
        with open(file_path) as f:
            cfg = yaml.safe_load(f)

        sensors = [Sensor(**s) for s in cfg["sensors"]]

        return cls(sensors)

from   argparse import ArgumentParser, Namespace
import asyncio
import httpx
import random
from   typing   import Any


SENSOR_PROFILES = {
    "pH": {
        "normal": [(6.7, 8.7)],
        "warning": [(6.1, 6.4), (9.05, 9.35)],
        "critical": [(5.4, 5.95), (9.6, 10.2)],
    },
    "temperature": {
        "normal": [(8.0, 24.0)],
        "warning": [(31.0, 34.0)],
        "critical": [(36.0, 45.0)],
    },
    "turbidity": {
        "normal": [(0.2, 7.0)],
        "warning": [(12.0, 35.0)],
        "critical": [(60.0, 150.0)],
    },
    "conductivity": {
        "normal": [(180.0, 460.0)],
        "warning": [(80.0, 140.0), (550.0, 1200.0)],
        "critical": [(5.0, 45.0), (1600.0, 7000.0)],
    },
    "dissolved_oxygen": {
        "normal": [(6.0, 10.0)],
        "warning": [(3.2, 5.2)],
        "critical": [(0.5, 2.8)],
    },
    "ORP": {
        "normal": [(320.0, 480.0)],
        "warning": [(150.0, 280.0), (520.0, 650.0)],
        "critical": [(-150.0, 90.0), (720.0, 900.0)],
    },
}


class Sensor:
    def __init__(
            self,
            id:                 str,
            min:                float | None = None,
            max:                float | None = None,
            type:               str   | None = None,
            unit:               str   | None = None,
            measure_interval_s: int   | None = None,
            config_interval_s:  int          = 10
            ):
        self.id                 = id
        self.min                = min
        self.max                = max
        self.type               = type
        self.unit               = unit
        self.measure_interval_s = measure_interval_s
        self.config_interval_s  = config_interval_s
        self.reading_count      = 0

    @classmethod
    def from_row(cls, row: dict[str, Any]):
        return cls(
            id                  = row["sensor_id"],
            min                 = row["min"],
            max                 = row["max"],
            type                = row["type"],
            unit                = row["unit"],
            measure_interval_s  = row["measure_interval_s"],
            config_interval_s   = row["config_interval_s"]
        )

    def _get_value(self) -> float | None:
        if self.min is None or self.max is None:
            return None

        self.reading_count += 1
        profile = SENSOR_PROFILES.get(self.type)

        if profile is None:
            value = random.uniform(self.min, self.max)
            return self._clamp(value)

        if self.reading_count % 20 == 0:
            ranges = profile["critical"]
        elif self.reading_count % 8 == 0:
            ranges = profile["warning"]
        else:
            ranges = profile["normal"]

        low, high = random.choice(ranges)
        return self._clamp(random.uniform(low, high))

    def _clamp(self, value: float) -> float:
        return max(self.min, min(self.max, value))


class SensorCluster:
    def __init__(
            self,
            base_url:          str,
            cluster_id:        str,
            config_interval_s: int
            ):
        self.sensors: dict[str, Sensor] = {}
        self.base_url                          = base_url.rstrip("/")
        self.cluster_id                        = self._normalize_cluster_id(cluster_id)
        self.config_interval_s                 = config_interval_s

    async def run(self):
        await asyncio.gather(
            self.poll_sensors_config_loop(),
            self.start_sensors_loop()
        )

    async def poll_sensors_config_loop(self) -> None:
        async with httpx.AsyncClient(timeout=5) as client:
            while True:
                try:
                    response = await client.get(
                        f"{self.base_url}/sensors-by-cluster/{self.cluster_id}"
                    )
                    response.raise_for_status()

                    rows = response.json()
                    if not isinstance(rows, list):
                        raise ValueError("sensor config response must be a list")

                    sensors = {}
                    for row in rows:
                        sensor_id = row["sensor_id"]
                        sensor = Sensor.from_row(row)
                        existing = self.sensors.get(sensor_id)
                        if existing is not None:
                            sensor.reading_count = existing.reading_count
                        sensors[sensor_id] = sensor
                    self.sensors = sensors

                except Exception as e:
                    print(f"Exception in `poll_sensors_config_loop()`: {e}")
                    await asyncio.sleep(
                        self._jitter(self.config_interval_s)
                    )
                    continue

                await asyncio.sleep(
                    self._jitter(self.config_interval_s)
                )

    async def start_sensors_loop(self) -> None:
        async with httpx.AsyncClient(timeout=5) as client:
            while True:
                tasks = []

                for _, sensor in self.sensors.items():
                    if sensor is None:
                        continue

                    tasks.append(self._send_measurement(client, sensor))

                if tasks:
                    await asyncio.gather(*tasks)

                await asyncio.sleep(self._jitter(1))

    async def _send_measurement(self, client: httpx.AsyncClient, sensor: Sensor) -> None:
            value = sensor._get_value()

            try:
                await client.post(
                    f"{self.base_url}/sensor-measurement",
                    json={
                        "sensor_id": f"{self.cluster_id}{self._normalize_sensor_id(sensor.id)}",
                        "sensor_type": sensor.type,
                        "value": value,
                    },
                )
            except Exception as e:
                print(f"Exception in `_send_measurement()`: {e}")
                pass

    def _jitter(self, interval_s: int):
        return max(1.0, interval_s + random.uniform(-0.1, 0.1) * interval_s)

    def _normalize_cluster_id(self, cluster_id: str) -> str:
        cluster_id = str(cluster_id).strip().upper()
        return cluster_id if cluster_id.startswith("C") else f"C{cluster_id}"

    def _normalize_sensor_id(self, sensor_id: str) -> str:
        sensor_id = str(sensor_id).strip().upper()
        return sensor_id if sensor_id.startswith("S") else f"S{sensor_id}"


def __parse_args__() -> Namespace:
    p = ArgumentParser(
            prog="Simulated cluster of sensors",
            # suggest_on_error=True
            # color=True
            )

    p.add_argument(
            "--cluster-id",
            required=True
            )

    p.add_argument(
            "--config-interval-s",
            type=int,
            required=False,
            default=10
            )

    p.add_argument(
            "--base-url",
            required=True
            )

    return p.parse_args()

async def main():
    args = __parse_args__()

    sensor_cluster = SensorCluster(base_url=args.base_url,
                                   cluster_id=args.cluster_id,
                                   config_interval_s=args.config_interval_s
                                   )

    await sensor_cluster.run()

if __name__ == "__main__":
    asyncio.run(main())

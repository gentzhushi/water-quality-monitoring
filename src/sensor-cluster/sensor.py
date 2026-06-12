from   argparse import ArgumentParser, Namespace
import asyncio
import httpx
import random
from   typing   import Any


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

        return ((self.max - self.min) * random.random()) + self.min


class SensorCluster:
    def __init__(
            self,
            base_url:          str,
            cluster_id:        str,
            config_interval_s: int
            ):
        self.sensors: dict[int, Sensor | None] = {}
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

                    rows = response.json()

                    for row in rows:
                        sensor_id = row["sensor_id"]
                        self.sensors[sensor_id] = Sensor.from_row(row)

                except Exception as e:
                    print(f"Exception in `poll_sensors_config_loop()`: {e}")
                    return;

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

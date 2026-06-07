from    argparse import ArgumentParser, Namespace
import  asyncio
import  random
import  httpx


class Sensor:

    def __init__(
            self,
            id:                 str,
            base_url:           str,
            min:                float = None,
            max:                float = None,
            type:               str   = None,
            measure_interval_s: int   = None,
            config_interval_s:  int   = 10
            ):
        self.id                 = id
        self.base_url           = base_url.rstrip("/")
        self.min                = min
        self.max                = max
        self.type               = type
        self.measure_interval_s = measure_interval_s
        self.config_interval_s  = config_interval_s

    def _get_value(self) -> float:
        if self.min == None or self.max == None:
            return None
        return ((self.max - self.min) * random.random()) + self.min

    async def _sleep_with_jitter(self, interval_s: int) -> None:
        if interval_s == None:
            await asyncio.sleep(5)
        else:
            jitter_s = random.uniform(-0.1, 0.1) * interval_s
            await asyncio.sleep(max(1, interval_s + jitter_s))

    async def _get_config_loop(self, client: httpx.AsyncClient) -> None:
        while True:
            await client.get(
                f"{self.base_url}/sensor-config/{self.id}"
            )

            print(f"Got a config!")

            # TODO: Handle configuration

            await self._sleep_with_jitter(self.config_interval_s)

    async def _post_measure_loop(self, client: httpx.AsyncClient) -> None:
        while True:
            if self.min == None or self.max == None or self.measure_interval_s == None:
                self._sleep_with_jitter(self.measure_interval_s)
            else:
                await client.post(
                    f"{self.base_url}/measure",
                    json={
                        "sensor_id": self.id,
                        "sensor_type": self.type,
                        "value": self._get_value()
                        }
                    )
                print(f"Posted a measurement!")

            # TODO: handle http response

            await self._sleep_with_jitter(self.measure_interval_s)

    async def start(self) -> None:
        print(f"[{self.id}]: Sensor started.")
        async with httpx.AsyncClient(timeout = 5) as client:
            await asyncio.gather(
                self._post_measure_loop(client),
                self._get_config_loop(client)
            )

def __parse_args__() -> Namespace:
    p = ArgumentParser(
            prog="Simulated sensor instance",
            description="Simulate an IoT sensor",
            # suggest_on_error=True
            # color=True
            )

    p.add_argument(
            "--id",
            required=True
            )

    p.add_argument(
            "--type",
            required=False,
            choices=["pH", "temperature"]
            )

    p.add_argument(
            "--min",
            type=float,
            required=False
            )

    p.add_argument(
            "--max",
            type=float,
            required=False
            )

    p.add_argument(
            "--measure-interval-s",
            type=int,
            required=False,
            default=None
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


if __name__ == "__main__":

    args = __parse_args__()

    sensor = Sensor(
            id=args.id,
            base_url=args.base_url
            # min=args.min,
            # max=args.max,
            # type=args.type,
            # measure_interval_s=args.measure_interval_s,
            # config_interval_s=args.config_interval_s,
            )

    asyncio.run(sensor.start())

from    argparse import ArgumentParser, Namespace
from    random   import random
import  requests
from    time     import sleep


class Sensor:

    def __init__(
            self,
            id:            str,
            type:          str,
            min:           float,
            max:           float,
            interval_ms:   int,
            endpoint:      str
            ):
        self.id          = id
        self.type        = type
        self.min         = min
        self.max         = max
        self.interval_ms = interval_ms
        self.endpoint    = endpoint

    def _get_value(self) -> float:
        return ((self.max - self.min) * random()) + self.min

    def _tick(self) -> requests.Response:
        return requests.post(
            self.endpoint,
            json={
                "sensor_id": self.id,
                "sensor_type": self.type,
                "value": self._get_value()
                }
            )

    def start(self) -> None:
        print(f"[{self.id}]: Sensor started.")
        while True:
            self._tick()
            sleep(self.interval_ms / 1000)

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
            required=True,
            choices=["pH", "temperature"]
            )

    p.add_argument(
            "--min",
            type=float,
            required=True
            )

    p.add_argument(
            "--max",
            type=float,
            required=True
            )

    p.add_argument(
            "--interval-ms",
            type=int,
            required=True,
            default=1000
            )

    p.add_argument(
            "--endpoint",
            required=True
            )

    return p.parse_args()
 

if __name__ == "__main__":
   
    args = __parse_args__()

    sensor = Sensor(
            id=args.id,
            type=args.type,
            min=args.min,
            max=args.max,
            interval_ms=args.interval_ms,
            endpoint=args.endpoint
            )

    sensor.start()

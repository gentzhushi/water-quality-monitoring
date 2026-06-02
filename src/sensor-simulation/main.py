from sensor import Sensor, SensorConfig
import time


def main() -> int:
    
    cfg = SensorConfig.load_cfg("sensors.yaml")
    print(f"Config loaded!\n{cfg}")

    while True:
        now = int(time.time())
        for s in cfg.sensors:
            if now % s.rate == 0:
                s.measure(now)
            else:
                continue
        time.sleep(1)

    return 0


if __name__ == "__main__":
    main()

from sensor import Sensor, SensorType, SensorConfig


def main() -> int:
    
    cfg = SensorConfig.load_cfg("sensors.yaml")
    print(cfg)

    return 0


if __name__ == "__main__":
    main()

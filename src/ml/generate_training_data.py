import argparse
import csv
import random
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / "data" / "water_quality_training.csv"
SEED = 42
WINDOW_MINUTES = 5
FORECAST_HORIZON_MINUTES = 10
RUNS_PER_SCENARIO = 80
MINUTES_PER_RUN = 80

PARAMETER_RULES = {
    "pH": {
        "normal_low": 6.5,
        "normal_high": 9.0,
        "critical_low": 6.0,
        "critical_high": 9.5,
        "threshold_scale": 1.0,
    },
    "temperature": {
        "normal_low": 0.0,
        "normal_high": 30.0,
        "critical_low": -1.0,
        "critical_high": 35.0,
        "threshold_scale": 10.0,
    },
    "turbidity": {
        "normal_low": 0.0,
        "normal_high": 10.0,
        "critical_low": -1.0,
        "critical_high": 50.0,
        "threshold_scale": 15.0,
    },
    "conductivity": {
        "normal_low": 150.0,
        "normal_high": 500.0,
        "critical_low": 50.0,
        "critical_high": 1500.0,
        "threshold_scale": 500.0,
    },
    "dissolved_oxygen": {
        "normal_low": 5.5,
        "normal_high": 14.0,
        "critical_low": 3.0,
        "critical_high": 18.0,
        "threshold_scale": 3.0,
    },
    "ORP": {
        "normal_low": 300.0,
        "normal_high": 500.0,
        "critical_low": 100.0,
        "critical_high": 700.0,
        "threshold_scale": 200.0,
    },
}

BASELINE_VALUES = {
    "pH": 7.2,
    "temperature": 21.0,
    "turbidity": 2.0,
    "conductivity": 320.0,
    "dissolved_oxygen": 8.2,
    "ORP": 380.0,
}

NOISE = {
    "pH": 0.04,
    "temperature": 0.35,
    "turbidity": 0.20,
    "conductivity": 25.0,
    "dissolved_oxygen": 0.18,
    "ORP": 10.0,
}

SCENARIOS = [
    "normal",
    "storm_runoff",
    "oxygen_depletion",
    "chemical_shift",
    "sensor_fault",
    "gradual_degradation",
]


def feature_prefix(parameter):
    return parameter.lower()


def clamp(value, low, high):
    return max(low, min(high, value))


def threshold_distance(parameter, value):
    rule = PARAMETER_RULES[parameter]
    distance = 0.0

    if value < rule["normal_low"]:
        distance = rule["normal_low"] - value
    elif value > rule["normal_high"]:
        distance = value - rule["normal_high"]

    return min(1.0, distance / rule["threshold_scale"])


def state_for_value(parameter, value):
    rule = PARAMETER_RULES[parameter]

    if value <= rule["critical_low"] or value >= rule["critical_high"]:
        return "critical"
    if value < rule["normal_low"] or value > rule["normal_high"]:
        return "warning"
    return "normal"


def mean(values):
    return sum(values) / len(values)


def stddev(values):
    if len(values) < 2:
        return 0.0

    average = mean(values)
    variance = sum((value - average) ** 2 for value in values) / (len(values) - 1)
    return variance ** 0.5


def normal_values(rng):
    return {
        parameter: BASELINE_VALUES[parameter] + rng.gauss(0.0, NOISE[parameter])
        for parameter in BASELINE_VALUES
    }


def add_storm_runoff(values, progress):
    values["turbidity"] += 90.0 * progress
    values["conductivity"] += 1900.0 * progress
    values["dissolved_oxygen"] -= 3.2 * progress
    values["ORP"] -= 180.0 * progress


def add_oxygen_depletion(values, progress):
    values["temperature"] += 15.0 * progress
    values["dissolved_oxygen"] -= 5.8 * progress
    values["ORP"] -= 260.0 * progress


def add_chemical_shift(values, progress, direction):
    values["pH"] += direction * 2.0 * progress
    values["conductivity"] += 700.0 * progress
    values["ORP"] += direction * 280.0 * progress


def add_sensor_fault(values, progress, parameter):
    if progress < 0.35:
        return

    if parameter == "pH":
        values["pH"] += 2.5
    elif parameter == "temperature":
        values["temperature"] += 25.0
    elif parameter == "turbidity":
        values["turbidity"] += 80.0
    elif parameter == "conductivity":
        values["conductivity"] += 2600.0
    elif parameter == "dissolved_oxygen":
        values["dissolved_oxygen"] -= 7.0
    else:
        values["ORP"] += 450.0


def add_gradual_degradation(values, progress):
    values["turbidity"] += 10.0 * progress
    values["conductivity"] += 900.0 * progress
    values["dissolved_oxygen"] -= 3.8 * progress
    values["ORP"] -= 160.0 * progress


def scenario_values(scenario, minute, event_start, rng, run_options):
    values = normal_values(rng)

    if scenario == "normal":
        return values

    progress = clamp((minute - event_start) / 25.0, 0.0, 1.0)

    if scenario == "storm_runoff":
        add_storm_runoff(values, progress)
    elif scenario == "oxygen_depletion":
        add_oxygen_depletion(values, progress)
    elif scenario == "chemical_shift":
        add_chemical_shift(values, progress, run_options["chemical_direction"])
    elif scenario == "sensor_fault":
        add_sensor_fault(values, progress, run_options["fault_parameter"])
    elif scenario == "gradual_degradation":
        add_gradual_degradation(values, progress)

    return values


def target_class_for_minute(scenario, minute, event_start):
    if scenario == "normal":
        return "normal"

    if minute >= event_start - FORECAST_HORIZON_MINUTES:
        return scenario

    return "normal"


def make_feature_row(run_id, scenario, minute, readings, event_start):
    row = {
        "run_id": run_id,
        "scenario": scenario,
        "minute": minute,
    }
    warning_count = 0
    critical_count = 0
    distances = []

    for parameter in PARAMETER_RULES:
        values = [reading[parameter] for reading in readings]
        latest = values[-1]
        prefix = feature_prefix(parameter)
        distance = threshold_distance(parameter, latest)
        state = state_for_value(parameter, latest)

        row[f"{prefix}_latest"] = round(latest, 4)
        row[f"{prefix}_mean_5m"] = mean(values)
        row[f"{prefix}_std_5m"] = stddev(values)
        row[f"{prefix}_slope_5m"] = (values[-1] - values[0]) / max(1, len(values) - 1)
        row[f"{prefix}_threshold_distance"] = distance
        distances.append(distance)

        if state == "critical":
            critical_count += 1
        elif state == "warning":
            warning_count += 1

    row["warning_count_5m"] = warning_count
    row["critical_count_5m"] = critical_count
    row["max_threshold_distance"] = max(distances)
    row["avg_threshold_distance"] = mean(distances)
    row["target_class"] = target_class_for_minute(scenario, minute, event_start)
    return row


def feature_columns():
    columns = ["run_id", "scenario", "minute"]

    for parameter in PARAMETER_RULES:
        prefix = feature_prefix(parameter)
        columns.extend(
            [
                f"{prefix}_latest",
                f"{prefix}_mean_5m",
                f"{prefix}_std_5m",
                f"{prefix}_slope_5m",
                f"{prefix}_threshold_distance",
            ]
        )

    columns.extend(
        [
            "warning_count_5m",
            "critical_count_5m",
            "max_threshold_distance",
            "avg_threshold_distance",
            "target_class",
        ]
    )
    return columns


def generate_rows(runs_per_scenario):
    rows = []

    for scenario in SCENARIOS:
        for run_index in range(runs_per_scenario):
            rng = random.Random(SEED + len(rows) + run_index)
            run_id = f"{scenario}_{run_index:03d}"
            event_start = rng.randint(28, 42)
            run_options = {
                "chemical_direction": rng.choice([-1, 1]),
                "fault_parameter": rng.choice(list(PARAMETER_RULES.keys())),
            }
            readings = []

            for minute in range(MINUTES_PER_RUN):
                values = scenario_values(scenario, minute, event_start, rng, run_options)
                readings.append(values)

                if minute < WINDOW_MINUTES:
                    continue

                window = readings[-WINDOW_MINUTES:]
                rows.append(make_feature_row(run_id, scenario, minute, window, event_start))

    return rows


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic water-quality ML training data.")
    parser.add_argument("--runs-per-scenario", type=int, default=RUNS_PER_SCENARIO)
    parser.add_argument("--output", type=Path, default=DATA_PATH)
    args = parser.parse_args()

    rows = generate_rows(args.runs_per_scenario)
    args.output.parent.mkdir(parents=True, exist_ok=True)

    with args.output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=feature_columns())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} training rows to {args.output}")


if __name__ == "__main__":
    main()

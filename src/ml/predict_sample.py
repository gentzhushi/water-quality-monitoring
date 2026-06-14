import json
from pathlib import Path
from typing import Dict, List

import joblib
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
MODEL_PATH = BASE_DIR / "artifacts" / "early_warning_random_forest.joblib"
SCHEMA_PATH = BASE_DIR / "artifacts" / "feature_schema.json"

BASELINE_VALUES = {
    "pH": 7.2,
    "temperature": 21.0,
    "turbidity": 2.0,
    "conductivity": 700.0,
    "dissolved_oxygen": 8.2,
    "ORP": 320.0,
}


def risk_level(risk_score: float, levels: List[dict]) -> str:
    for item in levels:
        if item["min"] <= risk_score < item["max"]:
            return item["level"]
    return "CRITICAL"


def threshold_distance(parameter: str, value: float, rules: dict) -> float:
    rule = rules[parameter]
    distance = 0.0

    if value < rule["normal_low"]:
        distance = rule["normal_low"] - value
    elif value > rule["normal_high"]:
        distance = value - rule["normal_high"]

    return min(1.0, distance / rule["threshold_scale"])


def make_sample(values: Dict[str, float], slopes: Dict[str, float], bundle: dict) -> Dict[str, float]:
    row = {
        "warning_count_5m": 0,
        "critical_count_5m": 0,
        "max_threshold_distance": 0.0,
        "avg_threshold_distance": 0.0,
    }
    distances = []

    for parameter, rule in bundle["parameter_rules"].items():
        key = parameter.lower()
        latest = values.get(parameter, BASELINE_VALUES[parameter])
        slope = slopes.get(parameter, 0.0)
        distance = threshold_distance(parameter, latest, bundle["parameter_rules"])
        distances.append(distance)

        row[f"{key}_latest"] = latest
        row[f"{key}_mean_5m"] = latest - (slope * 2.0)
        row[f"{key}_std_5m"] = abs(slope) * 0.8
        row[f"{key}_slope_5m"] = slope
        row[f"{key}_threshold_distance"] = distance

        if latest <= rule["critical_low"] or latest >= rule["critical_high"]:
            row["critical_count_5m"] += 1
        elif latest < rule["normal_low"] or latest > rule["normal_high"]:
            row["warning_count_5m"] += 1

    row["max_threshold_distance"] = max(distances)
    row["avg_threshold_distance"] = sum(distances) / len(distances)
    return row


def predict_one(name: str, row: Dict[str, float], bundle: dict, schema: dict) -> dict:
    model = bundle["model"]
    feature_columns = schema["feature_columns"]
    classes = list(bundle["class_labels"])
    probabilities = model.predict_proba(pd.DataFrame([row])[feature_columns])[0]
    normal_index = classes.index("normal")
    risk_score = 1.0 - float(probabilities[normal_index])
    level = risk_level(risk_score, bundle["risk_levels"])

    non_normal_indexes = [
        index
        for index, label in enumerate(classes)
        if label != "normal"
    ]
    best_event_index = max(non_normal_indexes, key=lambda index: probabilities[index])
    predicted_event_type = "none" if level == "LOW" else classes[best_event_index]

    return {
        "sample": name,
        "risk_score": round(risk_score, 4),
        "risk_level": level,
        "predicted_event_type": predicted_event_type,
        "class_probabilities": {
            label: round(float(probabilities[index]), 4)
            for index, label in enumerate(classes)
        },
    }


def main() -> None:
    bundle = joblib.load(MODEL_PATH)
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))

    samples = {
        "normal_stable_water": make_sample({}, {}, bundle),
        "storm_runoff_warning": make_sample(
            {
                "turbidity": 18.0,
                "conductivity": 2300.0,
                "dissolved_oxygen": 5.4,
                "ORP": 230.0,
            },
            {
                "turbidity": 2.0,
                "conductivity": 180.0,
                "dissolved_oxygen": -0.25,
                "ORP": -12.0,
            },
            bundle,
        ),
        "oxygen_depletion_warning": make_sample(
            {
                "temperature": 31.0,
                "dissolved_oxygen": 2.8,
                "ORP": 95.0,
            },
            {
                "temperature": 0.8,
                "dissolved_oxygen": -0.55,
                "ORP": -25.0,
            },
            bundle,
        ),
    }

    for name, row in samples.items():
        prediction = predict_one(name, row, bundle, schema)
        print(
            f"{prediction['sample']}: risk={prediction['risk_score']}, "
            f"level={prediction['risk_level']}, event={prediction['predicted_event_type']}"
        )
        print(f"  probabilities={prediction['class_probabilities']}")


if __name__ == "__main__":
    main()

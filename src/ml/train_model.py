import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, f1_score

from generate_training_data import PARAMETER_RULES, SCENARIOS, WINDOW_MINUTES, FORECAST_HORIZON_MINUTES


BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / "data" / "water_quality_training.csv"
ARTIFACT_DIR = BASE_DIR / "artifacts"
MODEL_PATH = ARTIFACT_DIR / "early_warning_random_forest.joblib"
SCHEMA_PATH = ARTIFACT_DIR / "feature_schema.json"
REPORT_JSON_PATH = ARTIFACT_DIR / "training_report.json"
REPORT_MD_PATH = ARTIFACT_DIR / "training_report.md"
RANDOM_STATE = 42
MODEL_VERSION = "synthetic-rf-v1"

RISK_LEVELS = [
    {"level": "LOW", "min": 0.0, "max": 0.35},
    {"level": "MEDIUM", "min": 0.35, "max": 0.60},
    {"level": "HIGH", "min": 0.60, "max": 0.85},
    {"level": "CRITICAL", "min": 0.85, "max": 1.01},
]

EXCLUDED_COLUMNS = {"run_id", "scenario", "minute", "target_class"}


def feature_prefix(parameter):
    return parameter.lower()


def feature_columns_from_data(data):
    return [
        column
        for column in data.columns
        if column not in EXCLUDED_COLUMNS
    ]


def split_by_run(data):
    run_ids = sorted(data["run_id"].unique())
    test_run_ids = set(run_ids[::5])

    train_data = data[~data["run_id"].isin(test_run_ids)].copy()
    test_data = data[data["run_id"].isin(test_run_ids)].copy()
    return train_data, test_data


def risk_level_for_score(risk_score):
    for item in RISK_LEVELS:
        if item["min"] <= risk_score < item["max"]:
            return item["level"]
    return "CRITICAL"


def prediction_summary(model, feature_columns, row):
    probabilities = model.predict_proba(pd.DataFrame([row])[feature_columns])[0]
    classes = list(model.classes_)
    normal_index = classes.index("normal")
    risk_score = 1.0 - float(probabilities[normal_index])

    non_normal_indexes = [
        index
        for index, label in enumerate(classes)
        if label != "normal"
    ]
    best_index = max(non_normal_indexes, key=lambda index: probabilities[index])

    risk_level = risk_level_for_score(risk_score)
    predicted_event_type = "none" if risk_level == "LOW" else classes[best_index]

    return {
        "risk_score": round(risk_score, 4),
        "risk_level": risk_level,
        "predicted_event_type": predicted_event_type,
        "class_probabilities": {
            label: round(float(probabilities[index]), 4)
            for index, label in enumerate(classes)
        },
    }


def example_predictions(model, test_data, feature_columns):
    examples = []

    for target_class in SCENARIOS:
        matching_rows = test_data[test_data["target_class"] == target_class]
        if matching_rows.empty:
            continue

        sample = matching_rows.iloc[0]
        features = sample[feature_columns].to_dict()
        prediction = prediction_summary(model, feature_columns, features)
        prediction.update(
            {
                "expected_class": target_class,
                "run_id": sample["run_id"],
                "minute": int(sample["minute"]),
            }
        )
        examples.append(prediction)

    return examples


def top_feature_importances(model, feature_columns, limit=20):
    pairs = sorted(
        zip(feature_columns, model.feature_importances_),
        key=lambda item: item[1],
        reverse=True,
    )

    return [
        {"feature": feature, "importance": round(float(importance), 6)}
        for feature, importance in pairs[:limit]
    ]


def build_feature_schema(feature_columns):
    return {
        "model_name": "early_warning_random_forest",
        "model_version": MODEL_VERSION,
        "model_type": "RandomForestClassifier",
        "target": "target_class",
        "window_minutes": WINDOW_MINUTES,
        "forecast_horizon_minutes": FORECAST_HORIZON_MINUTES,
        "parameters": list(PARAMETER_RULES.keys()),
        "feature_columns": feature_columns,
        "risk_levels": RISK_LEVELS,
        "scenario_classes": SCENARIOS,
        "notes": "This model is trained offline on synthetic scenario data. It predicts early warning risk, not exact future sensor values.",
    }


def write_markdown_report(report):
    lines = [
        "# Early-Warning ML Training Report",
        "",
        f"- Trained at: `{report['trained_at']}`",
        f"- Model: `{report['model_type']}`",
        f"- Version: `{report['model_version']}`",
        f"- Training rows: `{report['training_rows']}`",
        f"- Test rows: `{report['test_rows']}`",
        f"- Accuracy: `{report['accuracy']}`",
        f"- Macro F1: `{report['macro_f1']}`",
        "",
        "## Per-Class Metrics",
        "",
        "| Class | Precision | Recall | F1 | Support |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]

    for class_name, metrics in report["per_class_metrics"].items():
        lines.append(
            f"| {class_name} | {metrics['precision']:.4f} | "
            f"{metrics['recall']:.4f} | {metrics['f1-score']:.4f} | "
            f"{int(metrics['support'])} |"
        )

    lines.extend(
        [
            "",
            "## Top Feature Importances",
            "",
            "| Feature | Importance |",
            "| --- | ---: |",
        ]
    )

    for item in report["top_feature_importances"]:
        lines.append(f"| {item['feature']} | {item['importance']:.6f} |")

    lines.extend(["", "## Example Predictions", ""])

    for prediction in report["example_predictions"]:
        lines.append(
            f"- `{prediction['expected_class']}` sample from `{prediction['run_id']}` "
            f"minute `{prediction['minute']}`: risk `{prediction['risk_score']}`, "
            f"level `{prediction['risk_level']}`, event `{prediction['predicted_event_type']}`"
        )

    REPORT_MD_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Training CSV not found at {DATA_PATH}. Run generate_training_data.py first."
        )

    data = pd.read_csv(DATA_PATH)
    feature_columns = feature_columns_from_data(data)
    train_data, test_data = split_by_run(data)

    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=10,
        min_samples_leaf=10,
        class_weight="balanced_subsample",
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )

    model.fit(train_data[feature_columns], train_data["target_class"])
    predictions = model.predict(test_data[feature_columns])
    trained_at = datetime.now(timezone.utc).isoformat()

    per_class_metrics = classification_report(
        test_data["target_class"],
        predictions,
        output_dict=True,
        zero_division=0,
    )
    labels = list(model.classes_)

    report = {
        "trained_at": trained_at,
        "model_version": MODEL_VERSION,
        "model_type": "RandomForestClassifier",
        "model_path": str(MODEL_PATH),
        "feature_schema_path": str(SCHEMA_PATH),
        "training_rows": int(len(train_data)),
        "test_rows": int(len(test_data)),
        "accuracy": round(float(accuracy_score(test_data["target_class"], predictions)), 4),
        "macro_f1": round(float(f1_score(test_data["target_class"], predictions, average="macro")), 4),
        "per_class_metrics": {
            label: per_class_metrics[label]
            for label in labels
        },
        "confusion_matrix": {
            "labels": labels,
            "matrix": confusion_matrix(test_data["target_class"], predictions, labels=labels).tolist(),
        },
        "top_feature_importances": top_feature_importances(model, feature_columns),
        "example_predictions": example_predictions(model, test_data, feature_columns),
    }

    schema = build_feature_schema(feature_columns)
    bundle = {
        "model": model,
        "feature_columns": feature_columns,
        "class_labels": labels,
        "risk_levels": RISK_LEVELS,
        "parameter_rules": PARAMETER_RULES,
        "window_minutes": WINDOW_MINUTES,
        "forecast_horizon_minutes": FORECAST_HORIZON_MINUTES,
        "model_name": schema["model_name"],
        "model_version": schema["model_version"],
        "trained_at": trained_at,
    }

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, MODEL_PATH)
    SCHEMA_PATH.write_text(json.dumps(schema, indent=2), encoding="utf-8")
    REPORT_JSON_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    write_markdown_report(report)

    print(f"Accuracy: {report['accuracy']}")
    print(f"Macro F1: {report['macro_f1']}")
    print(f"Saved model to {MODEL_PATH}")


if __name__ == "__main__":
    main()

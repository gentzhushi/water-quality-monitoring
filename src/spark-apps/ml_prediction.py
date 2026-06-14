import json
import math
import os
from datetime import datetime, timedelta

import joblib
import pandas as pd
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


MODEL_PATH = os.getenv("ML_MODEL_PATH", "/opt/ml-artifacts/early_warning_random_forest.joblib")
SCHEMA_PATH = os.getenv("ML_SCHEMA_PATH", "/opt/ml-artifacts/feature_schema.json")
MODEL_VERSION = os.getenv("ML_MODEL_VERSION", "synthetic-rf-v1")
MIN_READINGS_PER_PARAMETER = int(os.getenv("ML_MIN_READINGS_PER_PARAMETER", "2"))
LOW_RISK_EVENT_TYPE = "none"

MODEL_BUNDLE = None
FEATURE_SCHEMA = None
RECENT_READINGS = {}

PREDICTION_HISTORY_SCHEMA = StructType(
    [
        StructField("location_id", StringType(), nullable=False),
        StructField("prediction_date", DateType(), nullable=False),
        StructField("prediction_time", TimestampType(), nullable=False),
        StructField("forecast_horizon_minutes", IntegerType(), nullable=False),
        StructField("risk_score", DoubleType(), nullable=False),
        StructField("risk_level", StringType(), nullable=False),
        StructField("predicted_event_type", StringType(), nullable=False),
        StructField("class_probabilities", StringType(), nullable=False),
        StructField("explanation", StringType(), nullable=False),
        StructField("model_name", StringType(), nullable=False),
        StructField("model_version", StringType(), nullable=False),
        StructField("computed_at", TimestampType(), nullable=False),
    ]
)

LATEST_PREDICTION_SCHEMA = StructType(
    [
        StructField("location_id", StringType(), nullable=False),
        StructField("prediction_time", TimestampType(), nullable=False),
        StructField("forecast_horizon_minutes", IntegerType(), nullable=False),
        StructField("risk_score", DoubleType(), nullable=False),
        StructField("risk_level", StringType(), nullable=False),
        StructField("predicted_event_type", StringType(), nullable=False),
        StructField("class_probabilities", StringType(), nullable=False),
        StructField("explanation", StringType(), nullable=False),
        StructField("model_name", StringType(), nullable=False),
        StructField("model_version", StringType(), nullable=False),
        StructField("computed_at", TimestampType(), nullable=False),
    ]
)


def load_model_bundle():
    global MODEL_BUNDLE, FEATURE_SCHEMA

    if MODEL_BUNDLE is None:
        with open(SCHEMA_PATH, "r", encoding="utf-8") as schema_file:
            FEATURE_SCHEMA = json.load(schema_file)

        MODEL_BUNDLE = joblib.load(MODEL_PATH)
        print(
            f"Loaded ML model {FEATURE_SCHEMA['model_name']} from {MODEL_PATH}",
            flush=True,
        )

    return MODEL_BUNDLE, FEATURE_SCHEMA


def safe_float(value, default=None):
    if value is None:
        return default
    return float(value)


def feature_prefix(parameter):
    return parameter.lower()


def mean(values):
    return sum(values) / len(values)


def sample_std(values):
    if len(values) < 2:
        return 0.0

    average = mean(values)
    variance = sum((value - average) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def slope_per_minute(readings):
    if len(readings) < 2:
        return 0.0

    first = readings[0]
    last = readings[-1]
    seconds = (last["event_time"] - first["event_time"]).total_seconds()

    if seconds <= 0:
        return 0.0

    minutes = seconds / 60.0
    return (last["value"] - first["value"]) / minutes


def rule_from_reading(reading, parameter, bundle):
    fallback_rules = bundle.get("parameter_rules", {})
    fallback = fallback_rules.get(parameter, {})

    return {
        "normal_low": reading.get("normal_low", fallback.get("normal_low")),
        "normal_high": reading.get("normal_high", fallback.get("normal_high")),
        "critical_low": reading.get("critical_low", fallback.get("critical_low")),
        "critical_high": reading.get("critical_high", fallback.get("critical_high")),
        "threshold_scale": reading.get("threshold_scale", fallback.get("threshold_scale", 1.0)),
    }


def threshold_distance(value, rule):
    distance = 0.0
    normal_low = rule.get("normal_low")
    normal_high = rule.get("normal_high")
    threshold_scale = rule.get("threshold_scale") or 1.0

    if normal_low is not None and value < normal_low:
        distance = normal_low - value
    elif normal_high is not None and value > normal_high:
        distance = value - normal_high

    return min(1.0, distance / threshold_scale)


def reading_state(value, rule):
    critical_low = rule.get("critical_low")
    critical_high = rule.get("critical_high")
    normal_low = rule.get("normal_low")
    normal_high = rule.get("normal_high")

    if critical_low is not None and value <= critical_low:
        return "critical"
    if critical_high is not None and value >= critical_high:
        return "critical"
    if normal_low is not None and value < normal_low:
        return "warning"
    if normal_high is not None and value > normal_high:
        return "warning"
    return "normal"


def prune_recent_readings(cutoff_time):
    for key in list(RECENT_READINGS.keys()):
        recent = [
            reading
            for reading in RECENT_READINGS[key]
            if reading["event_time"] >= cutoff_time
        ]

        if recent:
            RECENT_READINGS[key] = recent
        else:
            del RECENT_READINGS[key]


def update_recent_readings(rows, window_minutes):
    event_times = [
        row.event_time
        for row in rows
        if row.event_time is not None
    ]

    if not event_times:
        return None

    newest_time = max(event_times)
    cutoff_time = newest_time - timedelta(minutes=window_minutes)

    for row in rows:
        if row.location_id is None or row.parameter is None:
            continue
        if row.event_time is None or row.value is None:
            continue

        reading = {
            "event_time": row.event_time,
            "value": float(row.value),
            "unit": row.unit,
            "location_name": row.location_name,
            "normal_low": safe_float(row.normal_low),
            "normal_high": safe_float(row.normal_high),
            "critical_low": safe_float(row.critical_low),
            "critical_high": safe_float(row.critical_high),
            "threshold_scale": safe_float(row.threshold_scale, 1.0),
        }

        key = (row.location_id, row.parameter)
        RECENT_READINGS.setdefault(key, []).append(reading)
        RECENT_READINGS[key].sort(key=lambda item: item["event_time"])

    prune_recent_readings(cutoff_time)
    return newest_time


def locations_with_recent_data():
    return sorted({location_id for location_id, _parameter in RECENT_READINGS.keys()})


def build_feature_rows(prediction_time, bundle, schema):
    feature_rows = []
    metadata_rows = []
    parameters = schema["parameters"]
    window_minutes = int(schema["window_minutes"])
    cutoff_time = prediction_time - timedelta(minutes=window_minutes)

    for location_id in locations_with_recent_data():
        row = {}
        distances = []
        warning_count = 0
        critical_count = 0
        enough_data = True

        for parameter in parameters:
            readings = [
                reading
                for reading in RECENT_READINGS.get((location_id, parameter), [])
                if cutoff_time <= reading["event_time"] <= prediction_time
            ]

            if len(readings) < MIN_READINGS_PER_PARAMETER:
                enough_data = False
                break

            latest = readings[-1]
            values = [reading["value"] for reading in readings]
            prefix = feature_prefix(parameter)
            rule = rule_from_reading(latest, parameter, bundle)
            distance = threshold_distance(latest["value"], rule)
            state = reading_state(latest["value"], rule)

            row[f"{prefix}_latest"] = latest["value"]
            row[f"{prefix}_mean_5m"] = mean(values)
            row[f"{prefix}_std_5m"] = sample_std(values)
            row[f"{prefix}_slope_5m"] = slope_per_minute(readings)
            row[f"{prefix}_threshold_distance"] = distance
            distances.append(distance)

            if state == "critical":
                critical_count += 1
            elif state == "warning":
                warning_count += 1

        if not enough_data:
            continue

        row["warning_count_5m"] = warning_count
        row["critical_count_5m"] = critical_count
        row["max_threshold_distance"] = max(distances)
        row["avg_threshold_distance"] = mean(distances)

        for feature in schema["feature_columns"]:
            row.setdefault(feature, 0.0)

        feature_rows.append(row)
        metadata_rows.append({"location_id": location_id})

    return feature_rows, metadata_rows


def risk_level_for_score(risk_score, levels):
    for item in levels:
        if item["min"] <= risk_score < item["max"]:
            return item["level"]
    return "CRITICAL"


def best_non_normal_event(probabilities, classes):
    non_normal_indexes = [
        index
        for index, label in enumerate(classes)
        if label != "normal"
    ]

    if not non_normal_indexes:
        return LOW_RISK_EVENT_TYPE

    best_index = max(non_normal_indexes, key=lambda index: probabilities[index])
    return classes[best_index]


def make_explanation(risk_level, predicted_event_type, forecast_horizon_minutes):
    if risk_level == "LOW":
        return "Model sees low near-term degradation risk from the recent readings."

    readable_event = predicted_event_type.replace("_", " ")
    return (
        f"Model predicts {risk_level.lower()} risk of {readable_event} "
        f"within the next {forecast_horizon_minutes} minutes."
    )


def predict_rows(feature_rows, metadata_rows, prediction_time, bundle, schema):
    model = bundle["model"]
    feature_columns = schema["feature_columns"]
    classes = list(bundle.get("class_labels", model.classes_))
    normal_index = classes.index("normal")
    forecast_horizon_minutes = int(schema["forecast_horizon_minutes"])
    model_name = schema.get("model_name", "early_warning_random_forest")
    model_version = str(
        bundle.get("model_version")
        or schema.get("model_version")
        or MODEL_VERSION
    )

    frame = pd.DataFrame(feature_rows)
    probabilities_by_row = model.predict_proba(frame[feature_columns])
    computed_at = datetime.utcnow()

    predictions = []
    for index, probabilities in enumerate(probabilities_by_row):
        class_probabilities = {
            label: round(float(probabilities[class_index]), 4)
            for class_index, label in enumerate(classes)
        }
        risk_score = round(1.0 - float(probabilities[normal_index]), 4)
        risk_level = risk_level_for_score(risk_score, bundle["risk_levels"])

        if risk_level == "LOW":
            predicted_event_type = LOW_RISK_EVENT_TYPE
        else:
            predicted_event_type = best_non_normal_event(probabilities, classes)

        prediction = {
            "location_id": metadata_rows[index]["location_id"],
            "prediction_date": prediction_time.date(),
            "prediction_time": prediction_time,
            "forecast_horizon_minutes": forecast_horizon_minutes,
            "risk_score": risk_score,
            "risk_level": risk_level,
            "predicted_event_type": predicted_event_type,
            "class_probabilities": json.dumps(class_probabilities, sort_keys=True),
            "explanation": make_explanation(
                risk_level,
                predicted_event_type,
                forecast_horizon_minutes,
            ),
            "model_name": model_name,
            "model_version": model_version,
            "computed_at": computed_at,
        }
        predictions.append(prediction)

    return predictions


def write_prediction_rows(spark, predictions, cassandra_keyspace):
    history_df = spark.createDataFrame(predictions, PREDICTION_HISTORY_SCHEMA)
    history_df.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        keyspace=cassandra_keyspace,
        table="ml_predictions_by_location_minute",
    ).save()

    latest_rows = [
        {
            key: value
            for key, value in prediction.items()
            if key != "prediction_date"
        }
        for prediction in predictions
    ]
    latest_df = spark.createDataFrame(latest_rows, LATEST_PREDICTION_SCHEMA)
    latest_df.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        keyspace=cassandra_keyspace,
        table="latest_ml_predictions_by_location",
    ).save()


def write_ml_predictions(batch_df, batch_id, cassandra_keyspace):
    if len(batch_df.take(1)) == 0:
        return

    bundle, schema = load_model_bundle()

    rows = (
        batch_df.select(
            "location_id",
            "location_name",
            "parameter",
            "event_time",
            "value",
            "unit",
            "normal_low",
            "normal_high",
            "critical_low",
            "critical_high",
            "threshold_scale",
        )
        .where("rule_enabled = true")
        .collect()
    )

    prediction_time = update_recent_readings(rows, int(schema["window_minutes"]))
    if prediction_time is None:
        return

    feature_rows, metadata_rows = build_feature_rows(prediction_time, bundle, schema)
    if not feature_rows:
        print(
            f"ML batch {batch_id}: waiting for enough recent data to predict",
            flush=True,
        )
        return

    predictions = predict_rows(feature_rows, metadata_rows, prediction_time, bundle, schema)
    write_prediction_rows(batch_df.sparkSession, predictions, cassandra_keyspace)

    print(
        f"ML batch {batch_id}: wrote {len(predictions)} prediction row(s)",
        flush=True,
    )

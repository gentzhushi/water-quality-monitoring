import json
import os
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

import uvicorn
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.util import Date as CassandraDate
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates


CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "water_quality")
DEFAULT_CLUSTER_ID = os.getenv("DEFAULT_CLUSTER_ID", "")
DEFAULT_LOCATION_ID = os.getenv("DEFAULT_LOCATION_ID", "")

app = FastAPI(title="Water Quality Digital Twin Dashboard")
templates = Jinja2Templates(directory="templates")

cluster: Cluster | None = None
session = None


def connect_to_cassandra():
    global cluster, session

    for attempt in range(1, 31):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect(CASSANDRA_KEYSPACE)
            session.row_factory = dict_factory
            print("Connected to Cassandra dashboard keyspace.", flush=True)
            return
        except Exception as exc:
            print(f"Cassandra is not ready for dashboard backend ({attempt}/30): {exc}", flush=True)
            time.sleep(3)

    raise RuntimeError("Dashboard backend could not connect to Cassandra.")


@app.on_event("startup")
def startup() -> None:
    connect_to_cassandra()


@app.on_event("shutdown")
def shutdown() -> None:
    if cluster is not None:
        cluster.shutdown()


def db():
    if session is None:
        raise RuntimeError("Cassandra session is not initialized.")
    return session


def utc_today() -> date:
    return datetime.now(timezone.utc).date()


def parse_day(value: str | None) -> date:
    if not value:
        return utc_today()
    return date.fromisoformat(value)


def clamp_limit(value: int, maximum: int = 500) -> int:
    return max(1, min(value, maximum))


def serialize(value: Any) -> Any:
    if isinstance(value, CassandraDate):
        return value.date().isoformat()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    return value


def serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: serialize(value) for key, value in row.items()}


def serialize_rows(rows) -> list[dict[str, Any]]:
    return [serialize_row(row) for row in rows]


def query_rows(statement: str, params: tuple = ()) -> list[dict[str, Any]]:
    return serialize_rows(db().execute(statement, params))


def full_sensor_id(cluster_id: str, local_sensor_id: str) -> str:
    return f"{cluster_id}{local_sensor_id}"


def normalize_cluster_id(cluster_id: str | None) -> str | None:
    if cluster_id is None:
        return None
    cluster_id = cluster_id.strip()
    return cluster_id or None


def get_cluster_ids() -> list[str]:
    rows = query_rows("SELECT cluster_id FROM sensor_clusters")
    cluster_ids = {row["cluster_id"] for row in rows}
    fallback_rows = query_rows("SELECT cluster_id FROM sensor_configs_by_cluster")
    cluster_ids.update(row["cluster_id"] for row in fallback_rows)
    return sorted(cluster_ids)


def get_sensors(cluster_id: str | None = None) -> list[dict[str, Any]]:
    cluster_id = normalize_cluster_id(cluster_id)
    if cluster_id is not None:
        rows = query_rows(
            """
            SELECT cluster_id, sensor_id, sensor_type, unit, min_value, max_value,
                   measure_interval_s, config_interval_s, updated_at
            FROM sensor_configs_by_cluster
            WHERE cluster_id = %s
            """,
            (cluster_id,),
        )
    else:
        rows = []
        for cluster in get_cluster_ids():
            rows.extend(
                query_rows(
                    """
                    SELECT cluster_id, sensor_id, sensor_type, unit, min_value, max_value,
                           measure_interval_s, config_interval_s, updated_at
                    FROM sensor_configs_by_cluster
                    WHERE cluster_id = %s
                    """,
                    (cluster,),
                )
            )

    sensors = [
        {
            **row,
            "local_sensor_id": row["sensor_id"],
            "sensor_id": full_sensor_id(row["cluster_id"], row["sensor_id"]),
            "parameter": row["sensor_type"],
            "status": "active",
            "last_seen_at": row.get("updated_at"),
        }
        for row in rows
    ]
    sensors.sort(key=lambda item: (item["cluster_id"], item["local_sensor_id"]))
    return sensors


def get_first_sensor_id(cluster_id: str | None = None) -> str | None:
    sensors = get_sensors(cluster_id)
    if not sensors:
        return None
    return sensors[0]["sensor_id"]


def get_latest_rows_for_clusters(table_name: str, columns: str, cluster_id: str | None = None) -> list[dict[str, Any]]:
    cluster_id = normalize_cluster_id(cluster_id)
    if cluster_id is not None:
        return query_rows(
            f"""
            SELECT {columns}
            FROM {table_name}
            WHERE cluster_id = %s
            """,
            (cluster_id,),
        )

    rows: list[dict[str, Any]] = []
    for cluster in get_cluster_ids():
        rows.extend(
            query_rows(
                f"""
                SELECT {columns}
                FROM {table_name}
                WHERE cluster_id = %s
                """,
                (cluster,),
            )
        )
    rows.sort(key=lambda item: item.get("event_time") or item.get("minute_start") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return rows


def get_day_rows_for_clusters(table_name: str, columns: str, bucket_date: date, cluster_id: str | None = None) -> list[dict[str, Any]]:
    cluster_id = normalize_cluster_id(cluster_id)
    if cluster_id is not None:
        return query_rows(
            f"""
            SELECT {columns}
            FROM {table_name}
            WHERE cluster_id = %s AND bucket_date = %s
            """,
            (cluster_id, bucket_date),
        )

    rows: list[dict[str, Any]] = []
    for cluster in get_cluster_ids():
        rows.extend(
            query_rows(
                f"""
                SELECT {columns}
                FROM {table_name}
                WHERE cluster_id = %s AND bucket_date = %s
                """,
                (cluster, bucket_date),
            )
        )
    rows.sort(key=lambda item: item["event_time"], reverse=True)
    return rows


def get_metric_rows_for_clusters(metric_date: date, cluster_id: str | None = None) -> list[dict[str, Any]]:
    cluster_id = normalize_cluster_id(cluster_id)
    if cluster_id is not None:
        return query_rows(
            """
            SELECT cluster_id, metric_date, minute_start, processed_reading_count, alert_count,
                   avg_anomaly_score, max_anomaly_score, avg_event_latency_ms,
                   max_event_latency_ms, updated_at
            FROM pipeline_metrics_by_cluster_minute
            WHERE cluster_id = %s AND metric_date = %s
            """,
            (cluster_id, metric_date),
        )

    rows: list[dict[str, Any]] = []
    for cluster in get_cluster_ids():
        rows.extend(
            query_rows(
                """
                SELECT cluster_id, metric_date, minute_start, processed_reading_count, alert_count,
                       avg_anomaly_score, max_anomaly_score, avg_event_latency_ms,
                       max_event_latency_ms, updated_at
                FROM pipeline_metrics_by_cluster_minute
                WHERE cluster_id = %s AND metric_date = %s
                """,
                (cluster, metric_date),
            )
        )
    rows.sort(key=lambda item: item["minute_start"], reverse=True)
    return rows


def aggregate_metrics(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None

    latest_minute = max(row["minute_start"] for row in rows)
    minute_rows = [row for row in rows if row["minute_start"] == latest_minute]
    processed = sum(row.get("processed_reading_count", 0) or 0 for row in minute_rows)
    alerts = sum(row.get("alert_count", 0) or 0 for row in minute_rows)
    latency_values = [row.get("avg_event_latency_ms") for row in minute_rows if row.get("avg_event_latency_ms") is not None]
    score_values = [row.get("avg_anomaly_score") for row in minute_rows if row.get("avg_anomaly_score") is not None]
    max_score_values = [row.get("max_anomaly_score") for row in minute_rows if row.get("max_anomaly_score") is not None]
    max_latency_values = [row.get("max_event_latency_ms") for row in minute_rows if row.get("max_event_latency_ms") is not None]

    updated_values = [row.get("updated_at") for row in minute_rows if row.get("updated_at") is not None]

    return {
        "metric_date": minute_rows[0]["metric_date"],
        "minute_start": latest_minute,
        "processed_reading_count": processed,
        "alert_count": alerts,
        "avg_anomaly_score": round(sum(score_values) / len(score_values), 4) if score_values else None,
        "max_anomaly_score": max(max_score_values) if max_score_values else None,
        "avg_event_latency_ms": round(sum(latency_values) / len(latency_values), 3) if latency_values else None,
        "max_event_latency_ms": max(max_latency_values) if max_latency_values else None,
        "updated_at": max(updated_values) if updated_values else None,
    }


def get_parameter_rules() -> dict[str, dict[str, Any]]:
    rows = query_rows(
        """
        SELECT parameter, display_name, unit
        FROM parameter_rules_by_parameter
        """
    )
    return {row["parameter"]: row for row in rows}


def parse_class_probabilities(value: Any) -> dict[str, float]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    return parsed


def normalize_ml_prediction(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None

    prediction = dict(row)
    prediction["class_probabilities"] = parse_class_probabilities(
        prediction.get("class_probabilities")
    )
    return prediction


def get_latest_ml_prediction(location_id: str = DEFAULT_LOCATION_ID) -> dict[str, Any] | None:
    if location_id:
        rows = query_rows(
            """
            SELECT location_id, prediction_time, forecast_horizon_minutes, risk_score,
                   risk_level, predicted_event_type, class_probabilities, explanation,
                   model_name, model_version, computed_at
            FROM latest_ml_predictions_by_location
            WHERE location_id = %s
            """,
            (location_id,),
        )
    else:
        rows = query_rows(
            """
            SELECT location_id, prediction_time, forecast_horizon_minutes, risk_score,
                   risk_level, predicted_event_type, class_probabilities, explanation,
                   model_name, model_version, computed_at
            FROM latest_ml_predictions_by_location
            """
        )
        rows.sort(
            key=lambda row: row.get("computed_at") or row.get("prediction_time"),
            reverse=True,
        )
    if not rows:
        return None
    return normalize_ml_prediction(rows[0])


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/digital-twin")


@app.get("/digital-twin", response_class=HTMLResponse)
def digital_twin(request: Request):
    return templates.TemplateResponse(
        request,
        "digital_twin.html",
        {
            "default_cluster_id": DEFAULT_CLUSTER_ID,
            "default_cluster_label": "All sensors",
            "default_location_id": DEFAULT_LOCATION_ID,
        },
    )


@app.get("/healthcheck")
def healthcheck():
    db().execute("SELECT now() FROM system.local")
    return {"status": "ok"}


@app.get("/api/sensors")
def api_sensors(cluster_id: str | None = None):
    return {"items": get_sensors(cluster_id)}


@app.get("/api/clusters")
def api_clusters():
    return {
        "default_cluster_id": DEFAULT_CLUSTER_ID,
        "items": [{"cluster_id": cluster_id} for cluster_id in get_cluster_ids()],
    }


@app.get("/api/latest-readings")
def api_latest_readings(cluster_id: str | None = None):
    rows = get_latest_rows_for_clusters(
        "latest_readings_by_cluster",
        "cluster_id, parameter, sensor_id, local_sensor_id, event_time, value, unit, quality_status, updated_at",
        cluster_id,
    )
    return {"items": rows}


@app.get("/api/overview")
def api_overview():
    sensors = get_sensors()
    latest = api_latest_readings()["items"]
    parameter_rules = get_parameter_rules()
    latest_ml_prediction = get_latest_ml_prediction()
    latest_ai = get_latest_rows_for_clusters(
        "latest_ai_scores_by_cluster",
        "cluster_id, parameter, sensor_id, local_sensor_id, event_time, value, unit, anomaly_score, anomaly_level, rolling_average, z_score, rate_of_change, explanation, updated_at",
    )
    today = utc_today()
    alerts = get_day_rows_for_clusters(
        "alerts_by_cluster_day",
        "cluster_id, bucket_date, event_time, sensor_id, local_sensor_id, parameter, value, unit, alert_type, severity, alarm_state, message, explanation, processed_at",
        today,
    )
    metrics = aggregate_metrics(get_metric_rows_for_clusters(today))

    recent_cutoff = datetime.now(timezone.utc) - timedelta(minutes=5)
    recent_alerts = [
        alert
        for alert in alerts
        if datetime.fromisoformat(alert["event_time"].replace("Z", "+00:00")) >= recent_cutoff
    ]

    critical = any(alert["severity"] == "CRITICAL" for alert in recent_alerts) or any(
        item["anomaly_level"] == "CRITICAL" for item in latest_ai
    )
    warning = bool(recent_alerts) or any(
        item["anomaly_level"] in {"WATCH", "WARNING"} for item in latest_ai
    )
    system_status = "CRITICAL" if critical else ("WARNING" if warning else "NORMAL")

    latest_by_parameter: dict[str, dict[str, Any]] = {}
    for item in latest:
        parameter = item["parameter"]
        rule = parameter_rules.get(parameter, {})
        group = latest_by_parameter.setdefault(
            parameter,
            {
                "parameter": parameter,
                "display_name": rule.get("display_name") or parameter,
                "unit": item.get("unit") or rule.get("unit"),
                "values": [],
            },
        )
        group["values"].append(item["value"])

    def average(parameter: str) -> float | None:
        group = latest_by_parameter.get(parameter)
        values = group["values"] if group else []
        if not values:
            return None
        return round(sum(values) / len(values), 3)

    parameter_averages = []
    for group in latest_by_parameter.values():
        values = group["values"]
        parameter_averages.append(
            {
                "parameter": group["parameter"],
                "display_name": group["display_name"],
                "unit": group["unit"],
                "average_value": round(sum(values) / len(values), 3),
            }
        )
    parameter_averages.sort(key=lambda item: item["display_name"])

    return {
        "cluster_id": None,
        "system_status": system_status,
        "active_sensor_count": len(sensors),
        "active_alert_count": len(recent_alerts),
        "average_ph": average("pH"),
        "average_temperature": average("temperature"),
        "parameter_averages": parameter_averages,
        "latest_metric": metrics,
        "latest_readings": latest,
        "latest_ai": latest_ai,
        "latest_ml_prediction": latest_ml_prediction,
        "recent_alerts": recent_alerts[:20],
    }


@app.get("/api/readings")
def api_readings(
    sensor_id: str | None = None,
    bucket_date: str | None = None,
    limit: int = Query(default=200, ge=1, le=500),
    cluster_id: str | None = None,
):
    sensor_id = sensor_id or get_first_sensor_id(cluster_id)
    if sensor_id is None:
        return {"sensor_id": None, "items": []}

    day = parse_day(bucket_date)
    limit = clamp_limit(limit)
    rows = query_rows(
        f"""
        SELECT sensor_id, bucket_date, event_time, cluster_id, local_sensor_id, parameter,
               value, unit, quality_status, ingestion_time
        FROM readings_by_sensor_day
        WHERE sensor_id = %s AND bucket_date = %s
        LIMIT {limit}
        """,
        (sensor_id, day),
    )
    rows.reverse()
    return {"sensor_id": sensor_id, "bucket_date": day.isoformat(), "items": rows}


@app.get("/api/alerts")
def api_alerts(
    cluster_id: str | None = None,
    bucket_date: str | None = None,
    severity: str | None = None,
    sensor_id: str | None = None,
    limit: int = Query(default=200, ge=1, le=500),
):
    day = parse_day(bucket_date)
    limit = clamp_limit(limit)
    cluster_id = normalize_cluster_id(cluster_id)
    alert_columns = "cluster_id, bucket_date, event_time, sensor_id, local_sensor_id, parameter, value, unit, alert_type, severity, alarm_state, message, explanation, processed_at"

    if sensor_id:
        rows = query_rows(
            f"""
            SELECT {alert_columns}
            FROM alerts_by_sensor_day
            WHERE sensor_id = %s AND bucket_date = %s
            """,
            (sensor_id, day),
        )
        if cluster_id:
            rows = [row for row in rows if row["cluster_id"] == cluster_id]
    else:
        rows = get_day_rows_for_clusters(
            "alerts_by_cluster_day",
            alert_columns,
            day,
            cluster_id,
        )

    if severity:
        rows = [row for row in rows if row["severity"] == severity]
    return {"cluster_id": cluster_id, "bucket_date": day.isoformat(), "items": rows[:limit]}


@app.get("/api/alarms")
def api_alarms(
    cluster_id: str | None = None,
    bucket_date: str | None = None,
    sensor_id: str | None = None,
    severity: str | None = None,
):
    alerts = api_alerts(
        cluster_id=cluster_id,
        bucket_date=bucket_date,
        sensor_id=sensor_id,
        severity=severity,
        limit=500,
    )["items"]
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    now = datetime.now(timezone.utc)

    for alert in alerts:
        key = (alert["sensor_id"], alert["alert_type"])
        event_time = datetime.fromisoformat(alert["event_time"].replace("Z", "+00:00"))
        existing = grouped.get(key)
        if existing is None:
            grouped[key] = {
                "sensor_id": alert["sensor_id"],
                "parameter": alert["parameter"],
                "alert_type": alert["alert_type"],
                "severity": alert["severity"],
                "state": "ACTIVE" if now - event_time <= timedelta(minutes=5) else "RESOLVED",
                "first_seen": alert["event_time"],
                "last_seen": alert["event_time"],
                "alert_count": 1,
                "latest_value": alert["value"],
                "message": alert["message"],
                "explanation": alert["explanation"],
            }
            continue

        existing["alert_count"] += 1
        existing["first_seen"] = min(existing["first_seen"], alert["event_time"])
        existing["last_seen"] = max(existing["last_seen"], alert["event_time"])
        if alert["severity"] == "CRITICAL":
            existing["severity"] = "CRITICAL"

    return {"items": list(grouped.values())}


@app.get("/api/ai-insights")
def api_ai_insights(
    sensor_id: str | None = None,
    bucket_date: str | None = None,
    anomaly_level: str | None = None,
    limit: int = Query(default=120, ge=1, le=500),
    cluster_id: str | None = None,
):
    sensor_id = sensor_id or get_first_sensor_id(cluster_id)
    if sensor_id is None:
        return {"sensor_id": None, "items": []}

    day = parse_day(bucket_date)
    limit = clamp_limit(limit)
    rows = query_rows(
        f"""
        SELECT sensor_id, bucket_date, event_time, cluster_id, local_sensor_id, parameter,
               value, unit, rolling_average, rolling_stddev, z_score, rate_of_change,
               threshold_component, statistical_component, rate_component,
               anomaly_score, anomaly_level, explanation, computed_at
        FROM ai_scores_by_sensor_day
        WHERE sensor_id = %s AND bucket_date = %s
        LIMIT {limit}
        """,
        (sensor_id, day),
    )
    rows.reverse()
    if anomaly_level:
        anomaly_level = anomaly_level.strip().upper()
        rows = [row for row in rows if row["anomaly_level"] == anomaly_level]
    return {"sensor_id": sensor_id, "bucket_date": day.isoformat(), "items": rows}


@app.get("/api/ml-predictions")
def api_ml_predictions(
    location_id: str = DEFAULT_LOCATION_ID,
    prediction_date: str | None = None,
    limit: int = Query(default=160, ge=1, le=500),
):
    day = parse_day(prediction_date)
    limit = clamp_limit(limit)
    location_id = location_id.strip() if location_id else ""

    if location_id:
        rows = query_rows(
            f"""
            SELECT location_id, prediction_date, prediction_time, forecast_horizon_minutes,
                   risk_score, risk_level, predicted_event_type, class_probabilities,
                   explanation, model_name, model_version, computed_at
            FROM ml_predictions_by_location_minute
            WHERE location_id = %s AND prediction_date = %s
            LIMIT {limit}
            """,
            (location_id, day),
        )
    else:
        locations = [
            row["location_id"]
            for row in query_rows("SELECT location_id FROM latest_ml_predictions_by_location")
        ]
        rows = []
        for prediction_location in locations:
            rows.extend(
                query_rows(
                    f"""
                    SELECT location_id, prediction_date, prediction_time, forecast_horizon_minutes,
                           risk_score, risk_level, predicted_event_type, class_probabilities,
                           explanation, model_name, model_version, computed_at
                    FROM ml_predictions_by_location_minute
                    WHERE location_id = %s AND prediction_date = %s
                    LIMIT {limit}
                    """,
                    (prediction_location, day),
                )
            )
        rows.sort(key=lambda row: row["prediction_time"], reverse=True)
        rows = rows[:limit]

    rows.sort(key=lambda row: row["prediction_time"])
    return {
        "location_id": location_id or None,
        "prediction_date": day.isoformat(),
        "latest": get_latest_ml_prediction(location_id),
        "items": [normalize_ml_prediction(row) for row in rows],
    }


@app.get("/api/performance")
def api_performance(
    metric_date: str | None = None,
    limit: int = Query(default=120, ge=1, le=500),
    cluster_id: str | None = None,
):
    day = parse_day(metric_date)
    limit = clamp_limit(limit)
    rows = get_metric_rows_for_clusters(day, cluster_id)
    if cluster_id is None:
        aggregated: dict[datetime, dict[str, Any]] = {}
        for row in rows:
            minute = row["minute_start"]
            bucket = aggregated.setdefault(
                minute,
                {
                    "metric_date": row["metric_date"],
                    "minute_start": minute,
                    "processed_reading_count": 0,
                    "alert_count": 0,
                    "avg_anomaly_score_values": [],
                    "max_anomaly_score_values": [],
                    "avg_event_latency_ms_values": [],
                    "max_event_latency_ms_values": [],
                    "updated_at_values": [],
                },
            )
            bucket["processed_reading_count"] += row.get("processed_reading_count", 0) or 0
            bucket["alert_count"] += row.get("alert_count", 0) or 0
            if row.get("avg_anomaly_score") is not None:
                bucket["avg_anomaly_score_values"].append(row["avg_anomaly_score"])
            if row.get("max_anomaly_score") is not None:
                bucket["max_anomaly_score_values"].append(row["max_anomaly_score"])
            if row.get("avg_event_latency_ms") is not None:
                bucket["avg_event_latency_ms_values"].append(row["avg_event_latency_ms"])
            if row.get("max_event_latency_ms") is not None:
                bucket["max_event_latency_ms_values"].append(row["max_event_latency_ms"])
            if row.get("updated_at") is not None:
                bucket["updated_at_values"].append(row["updated_at"])

        rows = []
        for minute, bucket in aggregated.items():
            rows.append(
                {
                    "metric_date": bucket["metric_date"],
                    "minute_start": minute,
                    "processed_reading_count": bucket["processed_reading_count"],
                    "alert_count": bucket["alert_count"],
                    "avg_anomaly_score": round(sum(bucket["avg_anomaly_score_values"]) / len(bucket["avg_anomaly_score_values"]), 4) if bucket["avg_anomaly_score_values"] else None,
                    "max_anomaly_score": max(bucket["max_anomaly_score_values"]) if bucket["max_anomaly_score_values"] else None,
                    "avg_event_latency_ms": round(sum(bucket["avg_event_latency_ms_values"]) / len(bucket["avg_event_latency_ms_values"]), 3) if bucket["avg_event_latency_ms_values"] else None,
                    "max_event_latency_ms": max(bucket["max_event_latency_ms_values"]) if bucket["max_event_latency_ms_values"] else None,
                    "updated_at": max(bucket["updated_at_values"]) if bucket["updated_at_values"] else None,
                }
            )
        rows.sort(key=lambda item: item["minute_start"], reverse=True)
    else:
        rows.sort(key=lambda item: item["minute_start"], reverse=True)
    rows = rows[:limit]
    rows.reverse()
    return {"cluster_id": normalize_cluster_id(cluster_id), "metric_date": day.isoformat(), "items": rows}


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=80)

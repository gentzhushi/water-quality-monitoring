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
DEFAULT_LOCATION_ID = os.getenv("DEFAULT_LOCATION_ID", "demo_location_01")
DEFAULT_LOCATION_NAME = os.getenv("DEFAULT_LOCATION_NAME", "Demo Lake 01")

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


def get_sensors(location_id: str = DEFAULT_LOCATION_ID) -> list[dict[str, Any]]:
    metadata_rows = query_rows(
        """
        SELECT location_id, location_name, sensor_id, sensor_type, parameter, unit, status, last_seen_at
        FROM sensors_by_location
        WHERE location_id = %s
        """,
        (location_id,),
    )
    sensors_by_id = {row["sensor_id"]: row for row in metadata_rows}

    latest_rows = query_rows(
        """
        SELECT location_id, parameter, sensor_id, event_time, value, unit, quality_status, updated_at
        FROM latest_readings_by_location
        WHERE location_id = %s
        """,
        (location_id,),
    )

    for row in latest_rows:
        existing = sensors_by_id.get(row["sensor_id"])
        if existing is None:
            sensors_by_id[row["sensor_id"]] = {
                "location_id": row["location_id"],
                "location_name": DEFAULT_LOCATION_NAME,
                "sensor_id": row["sensor_id"],
                "sensor_type": row["parameter"],
                "parameter": row["parameter"],
                "unit": row["unit"],
                "status": "active",
                "last_seen_at": row["event_time"],
            }
        elif row.get("event_time"):
            existing["last_seen_at"] = row["event_time"]

    return sorted(sensors_by_id.values(), key=lambda row: row["sensor_id"])


def get_first_sensor_id(location_id: str = DEFAULT_LOCATION_ID) -> str | None:
    sensors = get_sensors(location_id)
    if not sensors:
        return None
    return sensors[0]["sensor_id"]


def get_parameter_rules() -> dict[str, dict[str, Any]]:
    rows = query_rows(
        """
        SELECT parameter, display_name, unit
        FROM parameter_rules_by_parameter
        """
    )
    return {row["parameter"]: row for row in rows}


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/digital-twin")


@app.get("/digital-twin", response_class=HTMLResponse)
def digital_twin(request: Request):
    return templates.TemplateResponse(
        request,
        "digital_twin.html",
        {"default_location_id": DEFAULT_LOCATION_ID},
    )


@app.get("/healthcheck")
def healthcheck():
    db().execute("SELECT now() FROM system.local")
    return {"status": "ok"}


@app.get("/api/sensors")
def api_sensors(location_id: str = DEFAULT_LOCATION_ID):
    return {"items": get_sensors(location_id)}


@app.get("/api/latest-readings")
def api_latest_readings(location_id: str = DEFAULT_LOCATION_ID):
    rows = query_rows(
        """
        SELECT location_id, parameter, sensor_id, event_time, value, unit, quality_status, updated_at
        FROM latest_readings_by_location
        WHERE location_id = %s
        """,
        (location_id,),
    )
    return {"items": rows}


@app.get("/api/overview")
def api_overview(location_id: str = DEFAULT_LOCATION_ID):
    sensors = get_sensors(location_id)
    latest = api_latest_readings(location_id)["items"]
    parameter_rules = get_parameter_rules()
    latest_ai = query_rows(
        """
        SELECT location_id, parameter, sensor_id, event_time, value, unit, anomaly_score,
               anomaly_level, rolling_average, z_score, rate_of_change, explanation, updated_at
        FROM latest_ai_scores_by_location
        WHERE location_id = %s
        """,
        (location_id,),
    )
    today = utc_today()
    alerts = query_rows(
        """
        SELECT location_id, bucket_date, event_time, sensor_id, parameter, value, unit,
               alert_type, severity, alarm_state, message, explanation, processed_at
        FROM alerts_by_location_day
        WHERE location_id = %s AND bucket_date = %s
        LIMIT 200
        """,
        (location_id, today),
    )
    metrics = query_rows(
        """
        SELECT metric_date, minute_start, processed_reading_count, alert_count, avg_anomaly_score,
               max_anomaly_score, avg_event_latency_ms, max_event_latency_ms, updated_at
        FROM pipeline_metrics_by_minute
        WHERE metric_date = %s
        LIMIT 1
        """,
        (today,),
    )

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
        "location_id": location_id,
        "system_status": system_status,
        "active_sensor_count": len(sensors),
        "active_alert_count": len(recent_alerts),
        "average_ph": average("pH"),
        "average_temperature": average("temperature"),
        "parameter_averages": parameter_averages,
        "latest_metric": metrics[0] if metrics else None,
        "latest_readings": latest,
        "latest_ai": latest_ai,
        "recent_alerts": recent_alerts[:20],
    }


@app.get("/api/readings")
def api_readings(
    sensor_id: str | None = None,
    bucket_date: str | None = None,
    limit: int = Query(default=200, ge=1, le=500),
    location_id: str = DEFAULT_LOCATION_ID,
):
    sensor_id = sensor_id or get_first_sensor_id(location_id)
    if sensor_id is None:
        return {"sensor_id": None, "items": []}

    day = parse_day(bucket_date)
    limit = clamp_limit(limit)
    rows = query_rows(
        f"""
        SELECT sensor_id, bucket_date, event_time, location_id, parameter, value, unit,
               quality_status, ingestion_time
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
    location_id: str = DEFAULT_LOCATION_ID,
    bucket_date: str | None = None,
    severity: str | None = None,
    sensor_id: str | None = None,
    limit: int = Query(default=200, ge=1, le=500),
):
    day = parse_day(bucket_date)
    limit = clamp_limit(limit)
    rows = query_rows(
        f"""
        SELECT location_id, bucket_date, event_time, sensor_id, parameter, value, unit,
               alert_type, severity, alarm_state, message, explanation, processed_at
        FROM alerts_by_location_day
        WHERE location_id = %s AND bucket_date = %s
        LIMIT {limit}
        """,
        (location_id, day),
    )
    if severity:
        rows = [row for row in rows if row["severity"] == severity]
    if sensor_id:
        rows = [row for row in rows if row["sensor_id"] == sensor_id]
    return {"location_id": location_id, "bucket_date": day.isoformat(), "items": rows}


@app.get("/api/alarms")
def api_alarms(location_id: str = DEFAULT_LOCATION_ID, bucket_date: str | None = None):
    alerts = api_alerts(location_id=location_id, bucket_date=bucket_date, limit=500)["items"]
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
    limit: int = Query(default=120, ge=1, le=500),
    location_id: str = DEFAULT_LOCATION_ID,
):
    sensor_id = sensor_id or get_first_sensor_id(location_id)
    if sensor_id is None:
        return {"sensor_id": None, "items": []}

    day = parse_day(bucket_date)
    limit = clamp_limit(limit)
    rows = query_rows(
        f"""
        SELECT sensor_id, bucket_date, event_time, location_id, parameter, value, unit,
               rolling_average, rolling_stddev, z_score, rate_of_change,
               threshold_component, statistical_component, rate_component,
               anomaly_score, anomaly_level, explanation, computed_at
        FROM ai_scores_by_sensor_day
        WHERE sensor_id = %s AND bucket_date = %s
        LIMIT {limit}
        """,
        (sensor_id, day),
    )
    rows.reverse()
    return {"sensor_id": sensor_id, "bucket_date": day.isoformat(), "items": rows}


@app.get("/api/performance")
def api_performance(
    metric_date: str | None = None,
    limit: int = Query(default=120, ge=1, le=500),
):
    day = parse_day(metric_date)
    limit = clamp_limit(limit)
    rows = query_rows(
        f"""
        SELECT metric_date, minute_start, processed_reading_count, alert_count,
               avg_anomaly_score, max_anomaly_score, avg_event_latency_ms,
               max_event_latency_ms, updated_at
        FROM pipeline_metrics_by_minute
        WHERE metric_date = %s
        LIMIT {limit}
        """,
        (day,),
    )
    rows.reverse()
    return {"metric_date": day.isoformat(), "items": rows}


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=80)

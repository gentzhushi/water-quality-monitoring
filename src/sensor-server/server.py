from  cassandra.cluster  import Cluster, Session
from  datetime           import UTC, datetime
from  fastapi            import FastAPI, HTTPException, Request
from  fastapi.responses  import HTMLResponse, RedirectResponse
from  fastapi.templating import Jinja2Templates
from  functools          import lru_cache
from  pydantic           import BaseModel
from  typing             import Any, Dict, List
import os
import re
import time
import uvicorn


CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "water_quality")
CASSANDRA_CONNECT_RETRIES = int(os.getenv("CASSANDRA_CONNECT_RETRIES", "12"))
CASSANDRA_CONNECT_DELAY_SEC = float(os.getenv("CASSANDRA_CONNECT_DELAY_SEC", "5"))
import re
from typing import Any




app = FastAPI()
templates = Jinja2Templates(directory="templates")


class SensorConfigPayload(BaseModel):
    min:                float | None = None
    max:                float | None = None
    type:               str   | None = None
    unit:               str   | None = None
    measure_interval_s: int   | None = None
    config_interval_s:  int   | None = 10


class MeasurementPayload(BaseModel):
    sensor_id:   str
    sensor_type: str
    value: float | None = None


DATA: list[dict[str, Any]] = []


@lru_cache(maxsize=1)
def get_cassandra_session() -> Session:
    last_error: Exception | None = None

    for _ in range(CASSANDRA_CONNECT_RETRIES):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            return cluster.connect(CASSANDRA_KEYSPACE)
        except Exception as exc:
            last_error = exc
            time.sleep(CASSANDRA_CONNECT_DELAY_SEC)

    raise RuntimeError("Could not connect to Cassandra") from last_error


def row_to_config(row) -> SensorConfigPayload:
    return SensorConfigPayload(
        min                = row.min_value,
        max                = row.max_value,
        type               = row.sensor_type,
        unit               = row.unit,
        measure_interval_s = row.measure_interval_s,
        config_interval_s  = row.config_interval_s,
    )


def ensure_cluster_exists(cluster_id: str) -> None:
    get_cassandra_session().execute(
        "INSERT INTO sensor_clusters (cluster_id) VALUES (%s)",
        (cluster_id,),
    )


def cluster_exists(cluster_id: str) -> bool:
    row = get_cassandra_session().execute(
        "SELECT cluster_id FROM sensor_clusters WHERE cluster_id = %s",
        (cluster_id,),
    ).one()
    return row is not None


def get_cluster_configs(cluster_id: str) -> dict[str, SensorConfigPayload]:
    rows = get_cassandra_session().execute(
        """
        SELECT sensor_id, min_value, max_value, sensor_type, unit,
               measure_interval_s, config_interval_s
        FROM sensor_configs_by_cluster
        WHERE cluster_id = %s
        """,
        (cluster_id,),
    )
    return {row.sensor_id: row_to_config(row) for row in rows}


def get_sensor_config(cluster_id: str, sensor_id: str) -> SensorConfigPayload | None:
    row = get_cassandra_session().execute(
        """
        SELECT min_value, max_value, sensor_type, unit,
               measure_interval_s, config_interval_s
        FROM sensor_configs_by_cluster
        WHERE cluster_id = %s AND sensor_id = %s
        """,
        (cluster_id, sensor_id),
    ).one()
    return row_to_config(row) if row else None


def get_all_sensor_configs() -> dict[str, dict[str, SensorConfigPayload]]:
    session = get_cassandra_session()
    cluster_rows = session.execute("SELECT cluster_id FROM sensor_clusters")
    configs: dict[str, dict[str, SensorConfigPayload]] = {
        row.cluster_id: {} for row in cluster_rows
    }

    config_rows = session.execute(
        """
        SELECT cluster_id, sensor_id, min_value, max_value, sensor_type, unit,
               measure_interval_s, config_interval_s
        FROM sensor_configs_by_cluster
        """
    )
    for row in config_rows:
        configs.setdefault(row.cluster_id, {})
        configs[row.cluster_id][row.sensor_id] = row_to_config(row)

    return dict(sorted(configs.items()))


def save_sensor_config(cluster_id: str, sensor_id: str, config: SensorConfigPayload) -> None:
    ensure_cluster_exists(cluster_id)
    get_cassandra_session().execute(
        """
        INSERT INTO sensor_configs_by_cluster (
            cluster_id, sensor_id, min_value, max_value, sensor_type, unit,
            measure_interval_s, config_interval_s, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            cluster_id,
            sensor_id,
            config.min,
            config.max,
            config.type,
            config.unit,
            config.measure_interval_s,
            config.config_interval_s,
            datetime.now(UTC),
        ),
    )


@app.post("/sensor-measurement")
async def post_measurement(payload: MeasurementPayload):
    DATA.append(payload.model_dump())
    return {"status": "ok"}


@app.get("/sensors-by-cluster/{cluster_id}")
def get_sensors(cluster_id: str):
    if not cluster_exists(cluster_id):
        raise HTTPException(status_code=404, detail="Cluster doesn't exist.")

    return [
        {
            "sensor_id": sensor_id,
            **config.model_dump(),
        }
        for sensor_id, config in get_cluster_configs(cluster_id).items()
    ]


def split_sensor_ref(sensor_ref: str) -> tuple[str, str]:
    if not re.match(r"^C\dS\d{3}$", sensor_ref):
        raise HTTPException(status_code=400, detail="Sensor ID must match `CxSyyy`")

    sensor_index = sensor_ref.find("S")
    return sensor_ref[:sensor_index], sensor_ref[sensor_index:]


@app.put("/sensors-by-id/{sid}")
def put_config(sid: str, payload: SensorConfigPayload):
    cluster_id, sensor_id = split_sensor_ref(sid)
    existing_config = get_sensor_config(cluster_id, sensor_id)

    if existing_config is None:
        save_sensor_config(cluster_id, sensor_id, payload)
        return {"status": "ok"}

    update_data = payload.model_dump(exclude_unset=True)
    updated_config = existing_config.model_copy(update=update_data)
    save_sensor_config(cluster_id, sensor_id, updated_config)
    return {"status": "ok"}


@app.get("/sensors-by-id/{sid}")
def get_config(sid: str):
    cluster_id, sensor_id = split_sensor_ref(sid)
    config = get_sensor_config(cluster_id, sensor_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sensor doesn't exist.")

    return {"config": config}

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/control-panel")

@app.get("/dashboard", response_class=HTMLResponse)
@app.get("/control-panel", response_class=HTMLResponse)
def get_dashboard(request: Request):
    context_data = {
        "request": request,
        "sensors": get_all_sensor_configs(),
    }
    return templates.TemplateResponse(request, "index.html", context_data)


@app.get("/dump/data")
def dump_data():
    return {"data": DATA}


@app.get("/dump/sensor-configs")
def dump_configs():
    return {"configs": get_all_sensor_configs()}


@app.get("/healthcheck")
def get_healthcheck():
    get_cassandra_session().execute("SELECT now() FROM system.local")
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=80)

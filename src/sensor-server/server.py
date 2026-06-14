from   fastapi            import FastAPI, HTTPException, Request
from   fastapi.responses  import HTMLResponse
from   fastapi.templating import Jinja2Templates
import os
from   pydantic           import BaseModel
import re
from   typing             import Any, Dict, List
import uvicorn


CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "water_quality")


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
    value:       float


DATA: List[Dict[str, Any]] = []


SENSOR_CONFIGS: dict[str, dict[str, SensorConfigPayload]] = {
    "C1": {
        "S001": SensorConfigPayload(
        min                = 1,
        max                = 100,
        type               = "temperature",
        unit               = "°C",
        measure_interval_s = 4
        ),
        "S002": SensorConfigPayload(
        min                = 1,
        max                = 14,
        type               = "pH",
        unit               = "pH units",
        measure_interval_s = 2
        )
    },
    "C2": {}
}


def row_to_config(row) -> SensorConfigPayload:
    return SensorConfigPayload(
        min=row.min,
        max=row.max,
        type=row.sensor_type,
        unit=row.unit,
        measure_interval_s=row.measure_interval_s,
        config_interval_s=row.config_interval_s,
    )


def get_cluster_configs(cluster_id: str) -> dict[str, SensorConfigPayload]:
    rows = session.execute(
        """
        SELECT sensor_id, min, max, sensor_type, unit, measure_interval_s, config_interval_s
        FROM sensor_configs_by_cluster
        WHERE cluster_id = %s
        """,
        (cluster_id,),
    )
    return {row.sensor_id: row_to_config(row) for row in rows}


def get_sensor_config(cluster_id: str, sensor_id: str) -> SensorConfigPayload | None:
    row = session.execute(
        """
        SELECT min, max, sensor_type, unit, measure_interval_s, config_interval_s
        FROM sensor_configs_by_cluster
        WHERE cluster_id = %s AND sensor_id = %s
        """,
        (cluster_id, sensor_id),
    ).one()
    return row_to_config(row) if row else None


def save_sensor_config(cluster_id: str, sensor_id: str, cfg: SensorConfigPayload) -> None:
    session.execute(
        """
        INSERT INTO sensor_configs_by_cluster (
            cluster_id, sensor_id, min, max, sensor_type, unit,
            measure_interval_s, config_interval_s, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            cluster_id,
            sensor_id,
            cfg.min,
            cfg.max,
            cfg.type,
            cfg.unit,
            cfg.measure_interval_s,
            cfg.config_interval_s,
            datetime.now(UTC),
        ),
    )


@app.post("/sensor-measurement")
async def post_measurement(payload: MeasurementPayload):
    DATA.append(payload.model_dump())
    return {"status": "ok"}


@app.get("/sensors-by-cluster/{cluster_id}")
def get_sensors(cluster_id: str):
    if cluster_id not in SENSOR_CONFIGS:
        raise HTTPException(status_code=404, detail="Cluster doesn't exist.")

    return [
        {
            "sensor_id": sid,
            **cfg.model_dump()
        }
        for sid, cfg in SENSOR_CONFIGS[cluster_id].items()
    ]


def split_sensor_ref(sensor_ref: str) -> tuple[str, str]:
    if not re.match(r'^C\dS\d{3}$', sensor_ref):
        raise HTTPException(status_code=400, detail="Sensor ID must match `CxSyyy`")

    s_index = sensor_ref.find("S")

    return sensor_ref[:s_index], sensor_ref[s_index:]


@app.put("/sensors-by-id/{sid}")
def put_config(sid: str, payload: SensorConfigPayload):
    cid, sensor_id = split_sensor_ref(sid)
    SENSOR_CONFIGS.setdefault(cid, {})

    if sensor_id not in SENSOR_CONFIGS[cid]:
        SENSOR_CONFIGS[cid][sensor_id] = payload
        return {"status": "ok"}

    update_data = payload.model_dump(exclude_unset=True)
    SENSOR_CONFIGS[cid][sensor_id] = SENSOR_CONFIGS[cid][sensor_id].model_copy(update=update_data)
    return {"status": "ok"}


@app.get("/sensors-by-id/{sid}")
def get_config(sid: str):
    cid, sensor_id = split_sensor_ref(sid)
    if cid not in SENSOR_CONFIGS or sensor_id not in SENSOR_CONFIGS[cid]:
        raise HTTPException(status_code=404, detail="Sensor doesn't exist.")

    return {"config": SENSOR_CONFIGS[cid][sensor_id]}


@app.get("/control-panel", response_class=HTMLResponse)
def get_dashboard(request: Request):
    context_data = {
        "request": request,
        "sensors": SENSOR_CONFIGS
    }

    return templates.TemplateResponse(request, "index.html", context_data)


@app.get("/dump/data")
def dump_data():
    return {"data": DATA}


@app.get("/dump/sensor-configs")
def dump_cfgs():
    return {"configs": SENSOR_CONFIGS}


@app.get("/healthcheck")
def get_healthcheck():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=80)

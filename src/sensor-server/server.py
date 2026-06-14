import re
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel


app = FastAPI()
templates = Jinja2Templates(directory="templates")


class SensorConfigPayload(BaseModel):
    min: float | None = None
    max: float | None = None
    type: str | None = None
    unit: str | None = None
    measure_interval_s: int | None = None
    config_interval_s: int | None = 10


class MeasurementPayload(BaseModel):
    sensor_id: str
    sensor_type: str
    value: float | None = None


DATA: list[dict[str, Any]] = []


SENSOR_CONFIGS: dict[str, dict[str, SensorConfigPayload]] = {
    "C1": {
        "S001": SensorConfigPayload(
            min=6.5,
            max=8.5,
            type="pH",
            unit="pH",
            measure_interval_s=2,
        ),
        "S002": SensorConfigPayload(
            min=0,
            max=35,
            type="temperature",
            unit="C",
            measure_interval_s=4,
        ),
        "S003": SensorConfigPayload(
            min=0,
            max=5,
            type="turbidity",
            unit="NTU",
            measure_interval_s=3,
        ),
        "S004": SensorConfigPayload(
            min=50,
            max=1500,
            type="conductivity",
            unit="uS/cm",
            measure_interval_s=5,
        ),
        "S005": SensorConfigPayload(
            min=5,
            max=14,
            type="dissolved_oxygen",
            unit="mg/L",
            measure_interval_s=4,
        ),
        "S006": SensorConfigPayload(
            min=150,
            max=500,
            type="ORP",
            unit="mV",
            measure_interval_s=5,
        ),
    },
    "C2": {},
}


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
            "sensor_id": sensor_id,
            **config.model_dump(),
        }
        for sensor_id, config in SENSOR_CONFIGS[cluster_id].items()
    ]


def split_sensor_ref(sensor_ref: str) -> tuple[str, str]:
    if not re.match(r"^C\dS\d{3}$", sensor_ref):
        raise HTTPException(status_code=400, detail="Sensor ID must match `CxSyyy`")

    sensor_index = sensor_ref.find("S")
    return sensor_ref[:sensor_index], sensor_ref[sensor_index:]


@app.put("/sensors-by-id/{sid}")
def put_config(sid: str, payload: SensorConfigPayload):
    cluster_id, sensor_id = split_sensor_ref(sid)
    SENSOR_CONFIGS.setdefault(cluster_id, {})

    if sensor_id not in SENSOR_CONFIGS[cluster_id]:
        SENSOR_CONFIGS[cluster_id][sensor_id] = payload
        return {"status": "ok"}

    update_data = payload.model_dump(exclude_unset=True)
    SENSOR_CONFIGS[cluster_id][sensor_id] = SENSOR_CONFIGS[cluster_id][sensor_id].model_copy(
        update=update_data
    )
    return {"status": "ok"}


@app.get("/sensors-by-id/{sid}")
def get_config(sid: str):
    cluster_id, sensor_id = split_sensor_ref(sid)
    if cluster_id not in SENSOR_CONFIGS or sensor_id not in SENSOR_CONFIGS[cluster_id]:
        raise HTTPException(status_code=404, detail="Sensor doesn't exist.")

    return {"config": SENSOR_CONFIGS[cluster_id][sensor_id]}


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/control-panel")


@app.get("/dashboard", response_class=HTMLResponse)
@app.get("/control-panel", response_class=HTMLResponse)
def get_dashboard(request: Request):
    context_data = {
        "request": request,
        "sensors": SENSOR_CONFIGS,
    }
    return templates.TemplateResponse(request, "index.html", context_data)


@app.get("/dump/data")
def dump_data():
    return {"data": DATA}


@app.get("/dump/sensor-configs")
def dump_configs():
    return {"configs": SENSOR_CONFIGS}


@app.get("/healthcheck")
def get_healthcheck():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=80)

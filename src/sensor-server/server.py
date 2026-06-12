from   fastapi            import FastAPI, HTTPException, Request
from   fastapi.responses  import HTMLResponse
from   fastapi.templating import Jinja2Templates
from   pydantic           import BaseModel
from   typing             import Any, Dict, List
import uvicorn


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
    }
}


@app.post("/sensor-measurement")
async def post_measurement(payload: MeasurementPayload):
    DATA.append(payload.model_dump())
    return {"status": "ok"}


@app.get("/sensors-by-cluster/{cluster_id}")
def get_sensors(cluster_id: str):
    return [
        {
            "sensor_id": sid,
            **cfg.model_dump()
        }
        for sid, cfg in SENSOR_CONFIGS[cluster_id].items()
    ]


# Qikjo osht teknikisht patch prap, veq duhet me rishiku
@app.put("/sensors-by-id/{sid}")
def put_config(sid: str, payload: SensorConfigPayload):

    print(f"requested_id:{sid}, available_clusters:{SENSOR_CONFIGS.keys()}")

    s = sid
    cid = f"C{s[1 : s.index("S")]}"
    sid = f"S{s[s.index("S")+1 : ]}"

    print(f"CID: {cid}, SID: {sid}")

    if cid not in SENSOR_CONFIGS:
        return {"status": 400, "description": "Cluster doesn't exist."}

    if sid not in SENSOR_CONFIGS[cid]:
        SENSOR_CONFIGS[cid][sid] = payload
        return {"status": "ok"}

    update_data = payload.model_dump(exclude_unset=True)
    SENSOR_CONFIGS[cid][sid] = SENSOR_CONFIGS[cid][sid].model_copy(update=update_data)
    return {"status": "ok"}


@app.get("/sensors-by-id/{sid}")
def get_config(sid: str):
    return {"config": SENSOR_CONFIGS[sid]}


@app.get("/dashboard", response_class=HTMLResponse)
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

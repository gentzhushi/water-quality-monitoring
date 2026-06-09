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
    measure_interval_s: int   | None = None
    config_interval_s:  int   | None = 10


DATA: List[Dict[str, str]] = []


SENSOR_CFGS: Dict[str, SensorConfigPayload]= {
    "temp_sensor_0": SensorConfigPayload(
        min                = 1,
        max                = 100,
        type               = "temperature",
        measure_interval_s = 4
    ),

    "ph_sensor_0": SensorConfigPayload(
        min                = 1,
        max                = 14,
        type               = "pH",
        measure_interval_s = 2
    )
}


@app.post("/measure")
async def post_measurement(payload: dict[str, Any]):
    DATA.append(payload)
    return {"status": "ok"}


@app.patch("/sensor-config/{sid}")
def post_config(sid: str, payload: SensorConfigPayload):

    print(f"requested_id:{sid}, available_ids{SENSOR_CFGS.keys()}")
    if sid not in SENSOR_CFGS:
        # NOTE: Qitu me implementu qe me shtu qat sensor config,
        #       cdo sensor kur tdhezet  qon qitu request, e poston prezencen e vet;
        #       edhe masanej ngon per state update
        raise HTTPException(status_code=404, detail=f"Sensor with ID=\"{sid}\" does not exist.")

    update_data = payload.model_dump(exclude_unset=True)

    SENSOR_CFGS[sid] = SENSOR_CFGS[sid].model_copy(update=update_data)

    return {"status": "ok"}

@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard(request: Request):
    context_data = {
        "request": request,
        "sensors": SENSOR_CFGS
    }

    return templates.TemplateResponse(request, "index.html", context_data)

@app.get("/sensor-config/{sid}")
def get_config(sid: str):
    return {"config": SENSOR_CFGS[sid]}


@app.get("/dump/data")
def dump_data():
    return {"data": DATA}


@app.get("/dump/configs")
def dump_cfgs():
    return {"configs": SENSOR_CFGS}


@app.get("/healthcheck")
def get_healthcheck():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=80)

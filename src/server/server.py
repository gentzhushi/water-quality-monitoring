from flask import Flask, request

app = Flask("flask")

DATA = []

SENSOR_CFGS = {
    "temp_sensor_0": {
        "min"                : 1,
        "max"                : 100,
        "type"               : "temperature",
        "measure_interval_s" : 4
    },
    "ph_sensor_0": {
        "min"                : 1,
        "max"                : 14,
        "type"               : "pH",
        "measure_interval_s" : 2
    }
}

@app.post("/measurement")
def measurement():
    payload = request.json
    DATA.append(payload)
    return {"status": "ok"}

@app.get("/sensor-config/<sid>")
def config(sid: str):
    return {"config": SENSOR_CFGS[sid]}

@app.get("/dump/data")
def dump_data():
    return {"data": DATA}

@app.get("/dump/configs")
def dump_cfgs():
    return {"configs": SENSOR_CFGS}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)

from flask import Flask, request

app = Flask("flask")

data = []

@app.post("/measurement")
def measurement():
    payload = request.json
    data.append(payload)
    return {"status": "ok"}

@app.get("/dump")
def dump():
    return {"data": data}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

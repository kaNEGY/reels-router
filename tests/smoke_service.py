"""Дымовой HTTP-тест: запускать только с изолированной тестовой БД и Kafka topic."""

import json
import subprocess
import time
from urllib.request import Request, urlopen
from uuid import uuid4


def request(path, data=None, key=None):
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    body = json.dumps(data).encode() if data is not None else None
    with urlopen(Request("http://127.0.0.1:8000" + path, data=body, headers=headers), timeout=5) as response:
        return response.status, json.load(response)


process = subprocess.Popen(["uvicorn", "reels_router.main:app", "--host", "127.0.0.1", "--port", "8000"])
try:
    for _ in range(30):
        try:
            status, _ = request("/health/ready")
            if status == 200:
                break
        except OSError:
            time.sleep(1)
    else:
        raise RuntimeError("Сервис не вышел в ready")
    payload = {"urls": ["https://www.instagram.com/reel/De4IKljoBfq/"]}
    key = str(uuid4())
    code, job = request("/v1/jobs", payload, key)
    assert code == 202 and job["status"] == "queued"
    code, repeat = request("/v1/jobs", payload, key)
    assert code == 200 and repeat == job
    code, result = request("/v1/jobs/" + job["job_id"])
    assert code == 200 and len(result["videos"]) == 1
    print("HTTP smoke passed: startup, readiness, create, idempotency, get")
finally:
    process.terminate()
    process.wait(timeout=15)

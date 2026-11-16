"""Проверка пользовательского сценария по локальному HTTP без реальных API-вызовов."""

import json
import time
from urllib.request import Request, urlopen


def request(path, data=None):
    body = json.dumps(data).encode() if data is not None else None
    with urlopen(Request("http://127.0.0.1:8000" + path, data=body,
                         headers={"Content-Type": "application/json"}), timeout=10) as response:
        return json.load(response)


job = request("/v1/jobs", {"urls": ["https://www.instagram.com/reel/De4IKljoBfq/"]})
job_id = job["job_id"]
for _ in range(300):
    result = request("/v1/jobs/" + job_id)
    if result["status"] == "needs_input":
        break
    if result["status"] in {"failed", "partial"}:
        raise RuntimeError(f"Пайплайн завершился с {result['status']}: {result['stages']}")
    time.sleep(1)
else:
    raise RuntimeError(f"Ожидание результата истекло: {result['stages']}")
place = result["places"][0]
assert place["name"] == "Kyoto Station" and place["candidates"] and place["evidence"]
result = request(f"/v1/jobs/{job_id}/selections", {"selections": [{
    "mention_id": place["id"], "candidate_id": place["candidates"][0]["id"]}]})
assert result["status"] == "completed"
assert {stage["kind"] for stage in result["stages"]} == {"media", "frames", "asr", "vlm", "llm", "geo"}
print("E2E passed: all stages, user selection, real CPU Gemma; download and external APIs mocked")

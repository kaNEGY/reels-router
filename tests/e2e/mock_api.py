"""Локальный HTTP mock только для изолированной сквозной проверки."""

import json

from fastapi import FastAPI, Request

app = FastAPI()


@app.post("/v1/audio/transcriptions")
async def transcription(request: Request):
    await request.body()
    return {"text": "We are at Kyoto Station."}


@app.post("/v1/chat/completions")
async def chat(request: Request):
    data = await request.json()
    if data["model"] == "mock-vlm":
        content = "The frame shows a sign reading Kyoto Station."
    else:
        prompt = data["messages"][0]["content"]
        sources = json.loads(prompt.split("Sources:\n", 1)[1])
        source = next(item for item in sources if "Kyoto Station" in item["text"])
        content = json.dumps({"places": [{"name": "Kyoto Station", "original_name": "Kyoto Station",
                                            "evidence": [{"source_id": source["id"], "quote": "Kyoto Station"}]}]})
    return {"choices": [{"message": {"content": content}}], "usage": {"total_tokens": 1}}


@app.get("/v1/geocode/search")
async def geocode():
    return {"results": [{"formatted": "Kyoto Station, Kyoto, Japan", "lat": 34.9858, "lon": 135.7588,
                         "place_id": "mock-kyoto", "rank": {"confidence": 0.9}}]}

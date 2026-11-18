from uuid import uuid4
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from reels_router.main import app
from reels_router.repository import IdempotencyConflict


class FakeRepository:
    def __init__(self):
        self.jobs = {}
        self.keys = {}

    async def create(self, body, key):
        if key and key in self.keys:
            old_body, job = self.keys[key]
            if old_body != body:
                raise IdempotencyConflict
            return job, False
        job = {"id": uuid4(), "status": "queued"}
        self.jobs[job["id"]] = job
        if key:
            self.keys[key] = (body, job)
        return job, True

    async def get(self, job_id):
        return self.jobs.get(job_id)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_job_submission_idempotency_and_status():
    app.state.repository = FakeRepository()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        body = {"urls": ["https://www.instagram.com/reel/De4IKljoBfq/?tracking=1"]}
        first = await client.post("/v1/jobs", json=body, headers={"Idempotency-Key": "abc"})
        assert first.status_code == 202
        repeat = await client.post("/v1/jobs", json=body, headers={"Idempotency-Key": "abc"})
        assert repeat.status_code == 200
        assert repeat.json() == first.json()
        other = await client.post("/v1/jobs", json={"urls": ["https://instagram.com/reel/different/"]}, headers={"Idempotency-Key": "abc"})
        assert other.status_code == 409
        assert (await client.get(f"/v1/jobs/{first.json()['job_id']}" )).status_code == 200
        assert (await client.get(f"/v1/jobs/{uuid4()}")).status_code == 404


@pytest.mark.anyio
@pytest.mark.parametrize("urls", [[], ["http://instagram.com/reel/abc/"],
    ["https://instagram.com.evil.test/reel/abc/"],
    ["https://instagram.com/reel/abc/", "https://www.instagram.com/reel/abc/?x=1"]])
async def test_bad_urls_are_rejected(urls):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.post("/v1/jobs", json={"urls": urls})).status_code == 422


@pytest.mark.anyio
async def test_internal_stage_requires_token_and_queues():
    app.state.settings = SimpleNamespace(internal_api_token="internal-test", kafka_topic_media="media",
        kafka_topic_frames="frames", kafka_topic_asr="asr", kafka_topic_vlm="vlm",
        kafka_topic_llm="llm", kafka_topic_geo="geo")
    repo = FakeRepository()
    repo.enqueue_stage = AsyncMock(return_value=uuid4())
    app.state.repository = repo
    body = {"job_id": str(uuid4()), "video_id": str(uuid4()), "artifact_ids": []}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.post("/internal/stages/media", json=body)).status_code == 401
        result = await client.post("/internal/stages/media", json=body, headers={"X-Internal-Token": "internal-test"})
        assert result.status_code == 202 and result.json()["stage_id"]
        repo.enqueue_stage.assert_awaited_once()

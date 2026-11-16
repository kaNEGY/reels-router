"""Проверка извлечения и подтверждения кандидата с подставными API."""

import asyncio
import json
from uuid import UUID

import asyncpg

from reels_router.geo_worker import handle_geo
from reels_router.llm_worker import handle_llm
from reels_router.providers import ModelResult
from reels_router.repository import Repository
from reels_router.schemas import Selection
from reels_router.settings import Settings
from reels_router.storage import Storage


class FakeLLM:
    async def generate(self, prompt):
        sources = json.loads(prompt.split("Sources:\n", 1)[1])
        source = next(item for item in sources if item["kind"] == "vlm_description")
        response = {"places": [{"name": "Example place", "original_name": "Visible sign",
                                 "evidence": [{"source_id": source["id"], "quote": "Visible sign"}]}]}
        return ModelResult(json.dumps(response), "mock-model", "mock-provider")


class FakeGeo:
    async def search(self, name):
        assert name == "Example place"
        return [{"place_id": "example", "label": "Example address", "lat": 35.0, "lon": 139.0,
                 "confidence": 0.8, "details": {"source": "test"}}]


async def main():
    settings = Settings()
    pool = await asyncpg.create_pool(host=settings.pg_host, port=settings.pg_port, user=settings.pg_user,
                                     password=settings.pg_password, database=settings.pg_database, ssl=settings.pg_sslmode)
    repo = Repository(pool, settings.kafka_topic_media, settings.kafka_topic_events)
    storage = Storage(settings)
    try:
        async with pool.acquire() as conn:
            raw = await conn.fetchval("""SELECT payload FROM reels_router.outbox
                WHERE payload->>'type'='vlm.completed' ORDER BY created_at DESC LIMIT 1""")
        event = json.loads(raw)
        assert await repo.coordinate(event, settings.kafka_topic_frames, settings.kafka_topic_asr,
                                     settings.kafka_topic_vlm, settings.kafka_topic_llm, settings.kafka_topic_geo)
        async with pool.acquire() as conn:
            raw = await conn.fetchval("""SELECT payload FROM reels_router.outbox
                WHERE payload->>'video_id'=$1 AND payload->>'type'='llm.requested'""", event["video_id"])
        await handle_llm(json.loads(raw), repo, storage, settings, FakeLLM())
        async with pool.acquire() as conn:
            raw = await conn.fetchval("""SELECT payload FROM reels_router.outbox
                WHERE payload->>'video_id'=$1 AND payload->>'type'='llm.completed'""", event["video_id"])
        assert await repo.coordinate(json.loads(raw), settings.kafka_topic_frames, settings.kafka_topic_asr,
                                     settings.kafka_topic_vlm, settings.kafka_topic_llm, settings.kafka_topic_geo)
        async with pool.acquire() as conn:
            raw = await conn.fetchval("""SELECT payload FROM reels_router.outbox
                WHERE payload->>'video_id'=$1 AND payload->>'type'='geo.requested'""", event["video_id"])
        await handle_geo(json.loads(raw), repo, storage, settings, FakeGeo())
        job_id = UUID(event["job_id"])
        job = await repo.get(job_id)
        assert job["status"] == "needs_input" and len(job["places"]) == 1
        place = job["places"][0]
        assert place["evidence"][0]["kind"] == "vlm_description" and place["evidence"][0]["time_s"] is not None
        choice = Selection(mention_id=place["id"], candidate_id=place["candidates"][0]["id"])
        assert await repo.select_places(job_id, [choice])
        assert (await repo.get(job_id))["status"] == "completed"
        assert await repo.select_places(job_id, [Selection(mention_id=place["id"])])
        assert (await repo.get(job_id))["status"] == "partial"
        print("Places integration passed: evidence, mock geocoder, confirmation and rejection")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

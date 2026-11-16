"""Проверка VLM-очереди и evidence через тестовые PostgreSQL и MinIO."""

import asyncio
import json

import asyncpg

from reels_router.providers import ModelResult
from reels_router.repository import Repository
from reels_router.settings import Settings
from reels_router.storage import Storage
from reels_router.vlm_worker import handle_vlm


class FakeVLM:
    async def describe(self, prompt, image):
        assert image.stat().st_size > 0
        return ModelResult("Visible sign", "mock-model", "mock-provider")


async def main():
    settings = Settings()
    pool = await asyncpg.create_pool(host=settings.pg_host, port=settings.pg_port, user=settings.pg_user,
                                     password=settings.pg_password, database=settings.pg_database, ssl=settings.pg_sslmode)
    repo = Repository(pool, settings.kafka_topic_media, settings.kafka_topic_events)
    storage = Storage(settings)
    try:
        async with pool.acquire() as conn:
            raw = await conn.fetchval("""SELECT payload FROM reels_router.outbox
                WHERE payload->>'type'='frames.completed' ORDER BY created_at DESC LIMIT 1""")
        frames_event = json.loads(raw)
        assert await repo.coordinate(frames_event, settings.kafka_topic_frames, settings.kafka_topic_asr,
                                     settings.kafka_topic_vlm)
        async with pool.acquire() as conn:
            raw = await conn.fetchval("""SELECT payload FROM reels_router.outbox
                WHERE payload->>'video_id'=$1 AND payload->>'type'='vlm.requested'""", frames_event["video_id"])
        event = json.loads(raw)
        await handle_vlm(event, repo, storage, settings, FakeVLM())
        async with pool.acquire() as conn:
            rows = await conn.fetch("""SELECT s.status,a.object_key FROM reels_router.stages s
                JOIN reels_router.artifacts a ON a.stage_id=s.id WHERE s.id=$1""", event["stage_id"])
        assert len(rows) == 1 and rows[0]["status"] == "completed"
        obj = await asyncio.to_thread(storage.client.get_object, Bucket=settings.s3_bucket, Key=rows[0]["object_key"])
        descriptions = json.loads(obj["Body"].read())
        assert descriptions and descriptions[0]["frame_artifact_id"] and descriptions[0]["time_s"] is not None
        print("VLM integration passed: PostgreSQL, MinIO and frame evidence with mock provider")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

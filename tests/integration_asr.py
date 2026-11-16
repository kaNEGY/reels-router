"""Интеграция очереди ASR и S3 с подставным провайдером."""

import asyncio
import json
import os
from pathlib import Path
from uuid import uuid4

import asyncpg

from reels_router.asr_worker import handle_asr
from reels_router.providers import ModelResult
from reels_router.repository import Repository
from reels_router.schemas import CreateJob
from reels_router.settings import Settings
from reels_router.storage import Storage


class FakeASR:
    async def transcribe(self, audio):
        assert audio.is_file() and audio.stat().st_size > 0
        return ModelResult("Tokyo", "mock-model", "mock-provider")


async def main():
    settings = Settings()
    pool = await asyncpg.create_pool(host=settings.pg_host, port=settings.pg_port, user=settings.pg_user,
                                     password=settings.pg_password, database=settings.pg_database, ssl=settings.pg_sslmode)
    repo = Repository(pool, settings.kafka_topic_media, settings.kafka_topic_events)
    storage = Storage(settings)
    try:
        job, _ = await repo.create(CreateJob(urls=["https://instagram.com/reel/De4IKljoBfq/"]), str(uuid4()))
        async with pool.acquire() as conn:
            raw = await conn.fetchval("SELECT payload FROM reels_router.outbox WHERE payload->>'job_id'=$1", str(job["id"]))
        media_event = json.loads(raw)
        artifacts = await storage.upload(media_event["job_id"], media_event["video_id"], media_event["stage_id"],
                                         {"audio": Path(os.environ["TEST_AUDIO_PATH"])})
        assert await repo.finish_media(media_event, artifacts)
        async with pool.acquire() as conn:
            raw = await conn.fetchval("""SELECT payload FROM reels_router.outbox
                WHERE payload->>'job_id'=$1 AND payload->>'type'='media.completed'""", str(job["id"]))
        assert await repo.coordinate(json.loads(raw), settings.kafka_topic_frames, settings.kafka_topic_asr)
        async with pool.acquire() as conn:
            raw = await conn.fetchval("""SELECT payload FROM reels_router.outbox
                WHERE payload->>'job_id'=$1 AND payload->>'type'='asr.requested'""", str(job["id"]))
        event = json.loads(raw)
        await handle_asr(event, repo, storage, settings, FakeASR())
        await handle_asr(event, repo, storage, settings, FakeASR())
        async with pool.acquire() as conn:
            rows = await conn.fetch("""SELECT s.status,a.object_key FROM reels_router.stages s
                JOIN reels_router.artifacts a ON a.stage_id=s.id WHERE s.id=$1""", event["stage_id"])
        assert len(rows) == 1 and rows[0]["status"] == "completed"
        obj = await asyncio.to_thread(storage.client.get_object, Bucket=settings.s3_bucket, Key=rows[0]["object_key"])
        result = json.loads(obj["Body"].read())
        assert result["text"] == "Tokyo" and result["segments"] is None
        print("ASR integration passed: PostgreSQL, MinIO and mock provider")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

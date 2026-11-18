"""Интеграция Gemma, PostgreSQL и S3 на коротком локальном видео."""

import asyncio
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
from gemma_frames import FrameRanker

from reels_router.frames_worker import handle_frames
from reels_router.repository import Repository
from reels_router.schemas import CreateJob
from reels_router.settings import Settings
from reels_router.storage import Storage


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
        files = {"video": Path(os.environ["TEST_VIDEO_PATH"])}
        artifacts = await storage.upload(media_event["job_id"], media_event["video_id"], media_event["stage_id"], files)
        assert await repo.finish_media(media_event, artifacts)
        async with pool.acquire() as conn:
            raw = await conn.fetchval("""SELECT payload FROM reels_router.outbox
                WHERE payload->>'job_id'=$1 AND payload->>'type'='media.completed'""", str(job["id"]))
        completed_event = json.loads(raw)
        assert await repo.coordinate(completed_event, settings.kafka_topic_frames)
        async with pool.acquire() as conn:
            raw = await conn.fetchval("""SELECT payload FROM reels_router.outbox
                WHERE payload->>'job_id'=$1 AND payload->>'type'='frames.requested'""", str(job["id"]))
        frames_event = json.loads(raw)
        with TemporaryDirectory(prefix="reels-model-test-") as directory:
            ranker = await asyncio.to_thread(FrameRanker, directory, settings.gemma_model, settings.gemma_cpu_threads)
            worker_settings = SimpleNamespace(scratch_dir="/tmp", ranker=ranker, frame_top_k=3)
            await handle_frames(frames_event, repo, storage, worker_settings)
            await handle_frames(frames_event, repo, storage, worker_settings)
        async with pool.acquire() as conn:
            rows = await conn.fetch("""SELECT s.status,a.kind,a.object_key,a.size_bytes FROM reels_router.stages s
                LEFT JOIN reels_router.artifacts a ON a.stage_id=s.id WHERE s.id=$1""", frames_event["stage_id"])
        assert rows and all(row["status"] == "completed" for row in rows)
        assert any(row["kind"] == "frames_manifest" for row in rows)
        assert any(row["kind"].startswith("frame_") for row in rows)
        for row in rows:
            head = await asyncio.to_thread(storage.client.head_object, Bucket=settings.s3_bucket, Key=row["object_key"])
            assert head["ContentLength"] == row["size_bytes"] > 0
        print(f"Frames integration passed: Gemma CPU, PostgreSQL, MinIO, {len(rows) - 1} frames")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

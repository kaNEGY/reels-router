"""Проверка защищённого механизма постановки этапа на тестовой PostgreSQL."""

import asyncio
import json
from uuid import uuid4

import asyncpg

from reels_router.repository import InvalidStage, Repository
from reels_router.schemas import CreateJob, StageRequest
from reels_router.settings import Settings


async def main():
    settings = Settings()
    pool = await asyncpg.create_pool(host=settings.pg_host, port=settings.pg_port, user=settings.pg_user,
                                     password=settings.pg_password, database=settings.pg_database, ssl=settings.pg_sslmode)
    repo = Repository(pool, "isolated.manual.media")
    try:
        job, _ = await repo.create(CreateJob(urls=["https://instagram.com/reel/De4IKljoBfq/"]), str(uuid4()))
        async with pool.acquire() as conn:
            raw = await conn.fetchval("SELECT payload FROM reels_router.outbox WHERE payload->>'job_id'=$1", str(job["id"]))
        event = json.loads(raw)
        artifact_id = uuid4()
        await repo.finish_media(event, [{"id": artifact_id, "kind": "video", "bucket": "test",
                                         "object_key": str(uuid4()), "size_bytes": 1, "sha256": "test"}])
        body = StageRequest(job_id=event["job_id"], video_id=event["video_id"], artifact_ids=[artifact_id])
        wrong = StageRequest(job_id=event["job_id"], video_id=event["video_id"], artifact_ids=[uuid4()])
        try:
            await repo.enqueue_stage(wrong, "frames", "isolated.manual.frames")
            raise AssertionError("unrelated artifact must be rejected")
        except InvalidStage:
            pass
        assert await repo.enqueue_stage(body, "frames", "isolated.manual.frames")
        try:
            await repo.enqueue_stage(body, "frames", "isolated.manual.frames")
            raise AssertionError("duplicate must be rejected")
        except InvalidStage:
            pass
        print("Internal stage integration passed: valid artifact, foreign artifact and duplicate rejection")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

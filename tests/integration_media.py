"""Тест выделенных БД/Kafka/S3; скачивание подменено ранее полученным видео."""

import asyncio
import json
import os
import shutil
from pathlib import Path
from uuid import uuid4

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from reels_router import media
from reels_router.media_worker import consume
from reels_router.repository import Repository
from reels_router.schemas import CreateJob
from reels_router.settings import Settings
from reels_router.storage import Storage


async def main():
    settings = Settings()
    settings.kafka_group_media = "reels-integration-" + str(uuid4())
    pool = await asyncpg.create_pool(host=settings.pg_host, port=settings.pg_port, user=settings.pg_user,
                                     password=settings.pg_password, database=settings.pg_database, ssl=settings.pg_sslmode)
    repo = Repository(pool, settings.kafka_topic_media, settings.kafka_topic_events)
    storage = Storage(settings)
    consumer = AIOKafkaConsumer(settings.kafka_topic_media, bootstrap_servers=settings.kafka_bootstrap_servers,
                                group_id=settings.kafka_group_media, enable_auto_commit=False,
                                auto_offset_reset="earliest")
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    original = media.run_command

    async def supplied_video(*args, timeout):
        if args[0] == "yt-dlp":
            output = Path(args[args.index("--output") + 1].replace("%(ext)s", "mp4"))
            shutil.copyfile(os.environ["TEST_VIDEO_PATH"], output)
            output.with_suffix(".info.json").write_text(json.dumps({"id": "local-test"}), encoding="utf-8")
            return b""
        return await original(*args, timeout=timeout)

    media.run_command = supplied_video
    worker = None
    try:
        await consumer.start()
        await producer.start()
        worker = asyncio.create_task(consume(consumer, repo, storage, settings))
        job, _ = await repo.create(CreateJob(urls=["https://www.instagram.com/reel/De4IKljoBfq/"]), str(uuid4()))
        while await repo.publish_one(producer):
            pass
        for _ in range(120):
            result = await repo.get(job["id"])
            if result["stages"]:
                break
            if worker.done():
                await worker
            await asyncio.sleep(1)
        else:
            raise AssertionError("media did not finish")
        assert result["stages"][0]["status"] == "completed", result["stages"]
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT kind,object_key,size_bytes FROM reels_router.artifacts WHERE stage_id=$1",
                                    result["stages"][0]["id"])
        assert {r["kind"] for r in rows} == {"video", "audio", "metadata"}
        for row in rows:
            head = await asyncio.to_thread(storage.client.head_object, Bucket=settings.s3_bucket, Key=row["object_key"])
            assert head["ContentLength"] == row["size_bytes"] > 0
        assert await repo.publish_one(producer)
        print("Media integration passed: Kafka, PostgreSQL, FFmpeg and MinIO (local video fixture)")
    finally:
        if worker:
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
        await consumer.stop()
        await producer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

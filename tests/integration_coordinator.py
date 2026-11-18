"""Проверка постановки frames-команды в выделенной тестовой PostgreSQL."""

import asyncio
import json

import asyncpg

from reels_router.repository import Repository
from reels_router.settings import Settings


async def main():
    settings = Settings()
    pool = await asyncpg.create_pool(host=settings.pg_host, port=settings.pg_port, user=settings.pg_user,
                                     password=settings.pg_password, database=settings.pg_database, ssl=settings.pg_sslmode)
    repo = Repository(pool, settings.kafka_topic_media, settings.kafka_topic_events)
    try:
        async with pool.acquire() as conn:
            raw = await conn.fetchval("""SELECT payload FROM reels_router.outbox
                WHERE payload->>'type'='media.completed' ORDER BY created_at DESC LIMIT 1""")
        event = json.loads(raw)
        assert await repo.coordinate(event, settings.kafka_topic_frames)
        assert not await repo.coordinate(event, settings.kafka_topic_frames)
        async with pool.acquire() as conn:
            rows = await conn.fetch("""SELECT payload FROM reels_router.outbox WHERE topic=$1
                AND payload->>'video_id'=$2""", settings.kafka_topic_frames, event["video_id"])
        assert len(rows) == 1
        command = json.loads(rows[0]["payload"])
        assert command["type"] == "frames.requested" and command["artifact_ids"] == event["artifact_ids"]
        print("Coordinator integration passed: frames command and duplicate event")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

"""Проверка backoff на выделенной тестовой базе, с подставным Kafka producer."""

import asyncio
import json
from datetime import date
from uuid import uuid4

import asyncpg

from reels_router.repository import Repository
from reels_router.settings import Settings


class Producer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, value, key):
        if topic == "test.failure":
            raise RuntimeError("synthetic")
        self.sent.append(topic)


async def main():
    settings = Settings()
    pool = await asyncpg.create_pool(host=settings.pg_host, port=settings.pg_port, user=settings.pg_user,
                                     password=settings.pg_password, database=settings.pg_database, ssl=settings.pg_sslmode)
    failed_id, success_id = uuid4(), uuid4()
    try:
        async with pool.acquire() as conn:
            for identifier, topic, created in ((failed_id, "test.failure", date(1900, 1, 1)),
                                               (success_id, "test.success", date(1900, 1, 2))):
                await conn.execute("""INSERT INTO reels_router.outbox (id,topic,message_key,payload,created_at)
                    VALUES ($1,$2,'test',$3::jsonb,$4::date)""", identifier, topic, json.dumps({"test": True}), created)
        producer = Producer()
        repo = Repository(pool, "unused")
        assert await repo.publish_one(producer)
        assert await repo.publish_one(producer)
        assert producer.sent == ["test.success"]
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT next_attempt_at IS NOT NULL FROM reels_router.outbox WHERE id=$1", failed_id)
            assert await conn.fetchval("SELECT published_at IS NOT NULL FROM reels_router.outbox WHERE id=$1", success_id)
        print("Outbox integration passed: failure backoff does not block another topic")
    finally:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE reels_router.outbox SET published_at=now() WHERE id=ANY($1::uuid[])", [failed_id, success_id])
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

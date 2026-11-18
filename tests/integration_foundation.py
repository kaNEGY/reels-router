"""Ручной интеграционный тест только для выделенной тестовой PostgreSQL."""

import asyncio
import json
import os
from uuid import uuid4

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from reels_router.repository import IdempotencyConflict, Repository
from reels_router.schemas import CreateJob


class Producer:
    def __init__(self, fail=False):
        self.fail = fail
        self.sent = []

    async def send_and_wait(self, topic, value, key):
        if self.fail:
            raise RuntimeError("synthetic failure")
        self.sent.append((topic, json.loads(value), key.decode()))


async def main():
    pool = await asyncpg.create_pool(host=os.environ["TEST_PG_HOST"], user=os.environ["TEST_PG_USER"],
                                     password=os.environ["TEST_PG_PASSWORD"], database=os.environ["TEST_PG_DATABASE"])
    topic = os.environ["TEST_KAFKA_TOPIC"]
    repo = Repository(pool, topic)
    body = CreateJob(urls=["https://instagram.com/reel/abc/"])
    key = f"integration-{uuid4()}"
    producer = AIOKafkaProducer(bootstrap_servers=os.environ["TEST_KAFKA_BOOTSTRAP"])
    consumer = AIOKafkaConsumer(topic, bootstrap_servers=os.environ["TEST_KAFKA_BOOTSTRAP"],
                                group_id=f"reels-test-{uuid4()}", auto_offset_reset="earliest")
    try:
        results = await asyncio.gather(*(repo.create(body, key) for _ in range(5)))
        assert sum(created for _, created in results) == 1
        job_id = results[0][0]["id"]
        assert all(row["id"] == job_id for row, _ in results)
        try:
            await repo.create(CreateJob(urls=["https://instagram.com/reel/other/"]), key)
            raise AssertionError("conflict expected")
        except IdempotencyConflict:
            pass
        assert len((await repo.get(job_id))["videos"]) == 1
        assert await repo.publish_one(Producer(fail=True))
        async with pool.acquire() as conn:
            postponed = await conn.fetchval("""SELECT next_attempt_at IS NOT NULL FROM reels_router.outbox
                WHERE payload->>'job_id'=$1""", str(job_id))
            assert postponed
            await conn.execute("UPDATE reels_router.outbox SET next_attempt_at=NULL WHERE payload->>'job_id'=$1", str(job_id))
        await producer.start()
        await consumer.start()
        assert await repo.publish_one(producer)
        while True:
            message = await asyncio.wait_for(consumer.getone(), timeout=20)
            event = json.loads(message.value)
            if event["job_id"] == str(job_id):
                break
        assert message.key.decode() == event["video_id"] and event["type"] == "media.requested"
        print("PostgreSQL + Kafka: concurrent idempotency, rollback, outbox and message received")
    finally:
        await consumer.stop()
        await producer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

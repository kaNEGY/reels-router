"""Проверка HTTP-выбора на выделенной тестовой PostgreSQL."""

import asyncio
from uuid import uuid4

import asyncpg
import httpx

from reels_router.main import app
from reels_router.repository import Repository
from reels_router.settings import Settings


async def main():
    settings = Settings()
    pool = await asyncpg.create_pool(host=settings.pg_host, port=settings.pg_port, user=settings.pg_user,
                                     password=settings.pg_password, database=settings.pg_database, ssl=settings.pg_sslmode)
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""SELECT v.job_id,m.id AS mention_id,c.id AS candidate_id
                FROM reels_router.place_candidates c JOIN reels_router.place_mentions m ON m.id=c.mention_id
                JOIN reels_router.videos v ON v.id=m.video_id ORDER BY m.id LIMIT 1""")
        app.state.repository = Repository(pool, settings.kafka_topic_media, settings.kafka_topic_events)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            path = f"/v1/jobs/{row['job_id']}/selections"
            bad = {"selections": [{"mention_id": str(row["mention_id"]), "candidate_id": str(uuid4())}]}
            assert (await client.post(path, json=bad)).status_code == 422
            selection = {"selections": [{"mention_id": str(row["mention_id"]), "candidate_id": str(row["candidate_id"])}]}
            result = await client.post(path, json=selection)
            assert result.status_code == 200 and result.json()["status"] == "completed"
            assert result.json()["places"][0]["has_selection"]
            assert (await client.post(f"/v1/jobs/{uuid4()}/selections", json=selection)).status_code == 404
        print("HTTP selections integration passed: valid, unrelated candidate, unknown job")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

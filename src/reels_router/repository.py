"""Атомарное создание задачи и сообщений outbox в PostgreSQL."""

import hashlib
import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

log = logging.getLogger(__name__)


class IdempotencyConflict(Exception):
    pass


class InvalidSelection(Exception):
    pass


class InvalidStage(Exception):
    pass


class Repository:
    def __init__(self, pool, media_topic, events_topic="reels.pipeline.events.v1"):
        self.pool = pool
        self.media_topic = media_topic
        self.events_topic = events_topic

    async def create(self, request, key):
        content = request.model_dump()
        digest = hashlib.sha256(json.dumps(content, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        async with self.pool.acquire() as conn, conn.transaction():
            job_id = uuid4()
            row = await conn.fetchrow(
                """INSERT INTO reels_router.jobs (id,idempotency_key,request_hash,context)
                   VALUES ($1,$2,$3,$4) ON CONFLICT (idempotency_key) DO NOTHING
                   RETURNING id,status""", job_id, key, digest, request.context,
            )
            if row is None:
                existing = await conn.fetchrow(
                    "SELECT id,status,request_hash FROM reels_router.jobs WHERE idempotency_key=$1", key
                )
                if existing["request_hash"] != digest:
                    raise IdempotencyConflict
                return dict(existing), False
            for url in request.urls:
                video_id, event_id, stage_id = uuid4(), uuid4(), uuid4()
                await conn.execute(
                    "INSERT INTO reels_router.videos (id,job_id,url) VALUES ($1,$2,$3)",
                    video_id, job_id, url,
                )
                event = {
                    "schema_version": 1, "event_id": str(event_id), "job_id": str(job_id),
                    "video_id": str(video_id), "stage_id": str(stage_id), "attempt": 1,
                    "type": "media.requested", "artifact_ids": [],
                    "created_at": datetime.now(timezone.utc).isoformat(), "trace_id": str(job_id),
                }
                await conn.execute(
                    "INSERT INTO reels_router.outbox (id,topic,message_key,payload) VALUES ($1,$2,$3,$4::jsonb)",
                    event_id, self.media_topic, str(video_id), json.dumps(event),
                )
            return dict(row), True

    async def enqueue_stage(self, body, kind, topic):
        async with self.pool.acquire() as conn, conn.transaction():
            video = await conn.fetchrow("SELECT status FROM reels_router.videos WHERE id=$1 AND job_id=$2 FOR UPDATE",
                                        body.video_id, body.job_id)
            if video is None or video["status"] not in {"queued", "running", "media_ready"}:
                raise InvalidStage
            if len(set(body.artifact_ids)) != len(body.artifact_ids):
                raise InvalidStage
            rows = await conn.fetch("""SELECT a.kind FROM reels_router.artifacts a
                JOIN reels_router.stages s ON s.id=a.stage_id WHERE s.video_id=$1 AND a.id=ANY($2::uuid[])""",
                body.video_id, body.artifact_ids)
            if len(rows) != len(body.artifact_ids):
                raise InvalidStage
            kinds = {row["kind"] for row in rows}
            valid = {"media": not kinds, "frames": "video" in kinds, "asr": "audio" in kinds,
                     "vlm": "frames_manifest" in kinds and any(k.startswith("frame_") for k in kinds),
                     "llm": bool(kinds & {"metadata", "transcript", "frame_descriptions"}),
                     "geo": "place_mentions" in kinds}
            if not valid[kind] or await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM reels_router.outbox
                WHERE payload->>'video_id'=$1 AND payload->>'type'=$2)""", str(body.video_id), f"{kind}.requested"):
                raise InvalidStage
            event_id, stage_id = uuid4(), uuid4()
            event = {"schema_version": 1, "event_id": str(event_id), "job_id": str(body.job_id),
                     "video_id": str(body.video_id), "stage_id": str(stage_id), "attempt": 1,
                     "type": f"{kind}.requested", "artifact_ids": [str(i) for i in body.artifact_ids],
                     "created_at": datetime.now(timezone.utc).isoformat(), "trace_id": str(body.job_id)}
            await conn.execute("INSERT INTO reels_router.outbox (id,topic,message_key,payload) VALUES ($1,$2,$3,$4::jsonb)",
                               event_id, topic, str(body.video_id), json.dumps(event))
        return stage_id

    async def get(self, job_id):
        async with self.pool.acquire() as conn:
            job = await conn.fetchrow(
                "SELECT id,status,context,created_at FROM reels_router.jobs WHERE id=$1", job_id
            )
            if job is None:
                return None
            videos = await conn.fetch(
                "SELECT id,url,status,created_at FROM reels_router.videos WHERE job_id=$1 ORDER BY created_at,id", job_id
            )
            stages = await conn.fetch(
                """SELECT s.id,s.video_id,s.kind,s.status,s.error,s.finished_at
                   FROM reels_router.stages s JOIN reels_router.videos v ON v.id=s.video_id
                   WHERE v.job_id=$1 ORDER BY s.finished_at""", job_id
            )
            artifacts = await conn.fetch("""SELECT a.id,a.stage_id,a.kind,a.size_bytes,a.sha256,a.created_at
                FROM reels_router.artifacts a JOIN reels_router.stages s ON s.id=a.stage_id
                JOIN reels_router.videos v ON v.id=s.video_id WHERE v.job_id=$1""", job_id)
            mentions = await conn.fetch(
                """SELECT m.id,m.video_id,m.name,m.original_name,m.context,m.evidence,
                   s.mention_id IS NOT NULL AS has_selection,s.candidate_id
                   FROM reels_router.place_mentions m JOIN reels_router.videos v ON v.id=m.video_id
                   LEFT JOIN reels_router.place_selections s ON s.mention_id=m.id WHERE v.job_id=$1""", job_id
            )
            candidates = await conn.fetch(
                """SELECT c.id,c.mention_id,c.provider_place_id,c.label,c.latitude,c.longitude,c.confidence
                   FROM reels_router.place_candidates c JOIN reels_router.place_mentions m ON m.id=c.mention_id
                   JOIN reels_router.videos v ON v.id=m.video_id WHERE v.job_id=$1""", job_id
            )
        return {**dict(job), "videos": [dict(video) for video in videos],
                "stages": [dict(stage) for stage in stages],
                "artifacts": [dict(artifact) for artifact in artifacts],
                "places": [{**dict(item), "evidence": json.loads(item["evidence"]),
                            "candidates": [dict(c) for c in candidates if c["mention_id"] == item["id"]]}
                           for item in mentions]}

    @staticmethod
    async def refresh_job(conn, job_id):
        await conn.execute("""UPDATE reels_router.jobs SET status=CASE
            WHEN EXISTS (SELECT 1 FROM reels_router.videos WHERE job_id=$1 AND status NOT IN ('needs_input','partial','failed','completed')) THEN 'running'
            WHEN EXISTS (SELECT 1 FROM reels_router.videos WHERE job_id=$1 AND status='needs_input') THEN 'needs_input'
            WHEN NOT EXISTS (SELECT 1 FROM reels_router.videos WHERE job_id=$1 AND status <> 'failed') THEN 'failed'
            WHEN EXISTS (SELECT 1 FROM reels_router.videos WHERE job_id=$1 AND status IN ('partial','failed')) THEN 'partial'
            ELSE 'completed' END WHERE id=$1""", job_id)

    async def select_places(self, job_id, selections):
        async with self.pool.acquire() as conn, conn.transaction():
            exists = await conn.fetchval("SELECT id FROM reels_router.jobs WHERE id=$1 FOR UPDATE", job_id)
            if not exists:
                return False
            video_ids = set()
            for item in selections:
                video_id = await conn.fetchval("""SELECT m.video_id FROM reels_router.place_mentions m
                    JOIN reels_router.videos v ON v.id=m.video_id WHERE m.id=$1 AND v.job_id=$2""", item.mention_id, job_id)
                if video_id is None:
                    raise InvalidSelection
                if item.candidate_id and not await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM reels_router.place_candidates WHERE id=$1 AND mention_id=$2)",
                    item.candidate_id, item.mention_id,
                ):
                    raise InvalidSelection
                await conn.execute("""INSERT INTO reels_router.place_selections (mention_id,candidate_id) VALUES ($1,$2)
                    ON CONFLICT (mention_id) DO UPDATE SET candidate_id=excluded.candidate_id,selected_at=now()""",
                    item.mention_id, item.candidate_id)
                video_ids.add(video_id)
            for video_id in video_ids:
                await conn.execute("""UPDATE reels_router.videos SET status=CASE
                    WHEN EXISTS (SELECT 1 FROM reels_router.place_mentions m LEFT JOIN reels_router.place_selections s
                                 ON s.mention_id=m.id WHERE m.video_id=$1 AND s.mention_id IS NULL) THEN 'needs_input'
                    WHEN EXISTS (SELECT 1 FROM reels_router.place_mentions m JOIN reels_router.place_selections s
                                 ON s.mention_id=m.id WHERE m.video_id=$1 AND s.candidate_id IS NULL) THEN 'partial'
                    ELSE 'completed' END WHERE id=$1""", video_id)
            await self.refresh_job(conn, job_id)
        return True

    async def finish_geo(self, event, places, error=None):
        event_id = uuid4()
        async with self.pool.acquire() as conn, conn.transaction():
            created = await conn.fetchval("""INSERT INTO reels_router.stages (id,video_id,kind,status,error,finished_at)
                VALUES ($1,$2,'geo',$3,$4,now()) ON CONFLICT (id) DO NOTHING RETURNING id""",
                event["stage_id"], event["video_id"], "failed" if error else "completed", error)
            if created is None:
                return False
            for place in places:
                mention_id = uuid4()
                await conn.execute("""INSERT INTO reels_router.place_mentions (id,video_id,stage_id,name,original_name,context,evidence)
                    VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)""", mention_id, event["video_id"], event["stage_id"],
                    place["name"], place["original_name"], place.get("context"), json.dumps(place["evidence"], ensure_ascii=False))
                for candidate in place["candidates"]:
                    await conn.execute("""INSERT INTO reels_router.place_candidates
                        (id,mention_id,provider_place_id,label,latitude,longitude,confidence,details)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)""", uuid4(), mention_id,
                        candidate.get("place_id"), candidate["label"], candidate["lat"], candidate["lon"],
                        candidate.get("confidence"), json.dumps(candidate["details"], ensure_ascii=False))
            await conn.execute("UPDATE reels_router.videos SET status=$1 WHERE id=$2",
                               "needs_input" if places and not error else "partial", event["video_id"])
            await self.refresh_job(conn, event["job_id"])
            output = {**event, "event_id": str(event_id), "type": "geo.failed" if error else "geo.completed",
                      "artifact_ids": [], "created_at": datetime.now(timezone.utc).isoformat()}
            await conn.execute("INSERT INTO reels_router.outbox (id,topic,message_key,payload) VALUES ($1,$2,$3,$4::jsonb)",
                               event_id, self.events_topic, str(event["video_id"]), json.dumps(output))
        return True

    async def media_input(self, event):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                """SELECT v.url, s.status AS stage_status FROM reels_router.videos v
                   LEFT JOIN reels_router.stages s ON s.id=$3
                   WHERE v.id=$1 AND v.job_id=$2""",
                event["video_id"], event["job_id"], event["stage_id"],
            )

    async def coordinate(self, event, frames_topic, asr_topic=None, vlm_topic=None, llm_topic=None, geo_topic=None):
        async with self.pool.acquire() as conn, conn.transaction():
            inserted = await conn.fetchval(
                "INSERT INTO reels_router.processed_messages (event_id) VALUES ($1) ON CONFLICT DO NOTHING RETURNING event_id",
                event["event_id"],
            )
            if not inserted:
                return False
            if event["type"] == "frames.completed" and vlm_topic:
                command_id = uuid4()
                command = {**event, "event_id": str(command_id), "stage_id": str(uuid4()),
                           "type": "vlm.requested", "attempt": 1,
                           "created_at": datetime.now(timezone.utc).isoformat()}
                await conn.execute(
                    "INSERT INTO reels_router.outbox (id,topic,message_key,payload) VALUES ($1,$2,$3,$4::jsonb)",
                    command_id, vlm_topic, str(event["video_id"]), json.dumps(command),
                )
                return True
            if event["type"] == "llm.completed" and geo_topic:
                command_id = uuid4()
                command = {**event, "event_id": str(command_id), "stage_id": str(uuid4()),
                           "type": "geo.requested", "attempt": 1,
                           "created_at": datetime.now(timezone.utc).isoformat()}
                await conn.execute(
                    "INSERT INTO reels_router.outbox (id,topic,message_key,payload) VALUES ($1,$2,$3,$4::jsonb)",
                    command_id, geo_topic, str(event["video_id"]), json.dumps(command),
                )
                return True
            if event["type"] == "llm.failed":
                await conn.execute("UPDATE reels_router.videos SET status='partial' WHERE id=$1", event["video_id"])
                await self.refresh_job(conn, event["job_id"])
                return True
            if llm_topic and event["type"] in {"asr.completed", "asr.failed", "vlm.completed", "vlm.failed", "frames.failed"}:
                await conn.fetchval("SELECT id FROM reels_router.videos WHERE id=$1 FOR UPDATE", event["video_id"])
                statuses = await conn.fetch(
                    "SELECT kind,status FROM reels_router.stages WHERE video_id=$1 ORDER BY finished_at", event["video_id"]
                )
                statuses = {row["kind"]: row["status"] for row in statuses}
                has_audio = await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM reels_router.artifacts a
                       JOIN reels_router.stages s ON s.id=a.stage_id WHERE s.video_id=$1 AND a.kind='audio')""",
                    event["video_id"],
                )
                frames_done = statuses.get("frames") == "failed" or (
                    statuses.get("frames") == "completed" and statuses.get("vlm") in {"completed", "failed"}
                )
                if not frames_done or (has_audio and statuses.get("asr") not in {"completed", "failed"}):
                    return False
                exists = await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM reels_router.outbox
                       WHERE payload->>'video_id'=$1 AND payload->>'type'='llm.requested')""", str(event["video_id"])
                )
                if exists:
                    return False
                inputs = await conn.fetch(
                    """SELECT a.id FROM reels_router.artifacts a JOIN reels_router.stages s ON s.id=a.stage_id
                       WHERE s.video_id=$1 AND a.kind=ANY($2::text[])""", event["video_id"],
                    ["metadata", "transcript", "frame_descriptions"],
                )
                command_id = uuid4()
                command = {**event, "event_id": str(command_id), "stage_id": str(uuid4()),
                           "type": "llm.requested", "attempt": 1,
                           "artifact_ids": [str(row["id"]) for row in inputs],
                           "created_at": datetime.now(timezone.utc).isoformat()}
                await conn.execute(
                    "INSERT INTO reels_router.outbox (id,topic,message_key,payload) VALUES ($1,$2,$3,$4::jsonb)",
                    command_id, llm_topic, str(event["video_id"]), json.dumps(command),
                )
                return True
            if event["type"] != "media.completed":
                return False
            command_id, stage_id = uuid4(), uuid4()
            command = {**event, "event_id": str(command_id), "stage_id": str(stage_id),
                       "type": "frames.requested", "attempt": 1,
                       "created_at": datetime.now(timezone.utc).isoformat()}
            await conn.execute(
                "INSERT INTO reels_router.outbox (id,topic,message_key,payload) VALUES ($1,$2,$3,$4::jsonb)",
                command_id, frames_topic, str(event["video_id"]), json.dumps(command),
            )
            if asr_topic and await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM reels_router.artifacts WHERE id=ANY($1::uuid[]) AND kind='audio')",
                event["artifact_ids"],
            ):
                command_id = uuid4()
                command = {**command, "event_id": str(command_id), "stage_id": str(uuid4()), "type": "asr.requested"}
                await conn.execute(
                    "INSERT INTO reels_router.outbox (id,topic,message_key,payload) VALUES ($1,$2,$3,$4::jsonb)",
                    command_id, asr_topic, str(event["video_id"]), json.dumps(command),
                )
            return True

    async def artifacts_input(self, event):
        async with self.pool.acquire() as conn:
            stage = await conn.fetchval("SELECT status FROM reels_router.stages WHERE id=$1", event["stage_id"])
            rows = await conn.fetch(
                """SELECT a.id,a.kind,a.bucket,a.object_key FROM reels_router.artifacts a
                   JOIN reels_router.stages source ON source.id=a.stage_id
                   JOIN reels_router.videos v ON v.id=source.video_id
                   WHERE v.id=$1 AND v.job_id=$2 AND a.id=ANY($3::uuid[]) ORDER BY a.kind""",
                event["video_id"], event["job_id"], event["artifact_ids"],
            )
        return stage, [dict(row) for row in rows]

    async def frames_input(self, event, kind="video"):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                """SELECT a.bucket,a.object_key, s.status AS stage_status
                   FROM reels_router.artifacts a
                   JOIN reels_router.stages source ON source.id=a.stage_id
                   JOIN reels_router.videos v ON v.id=source.video_id
                   LEFT JOIN reels_router.stages s ON s.id=$3
                   WHERE v.id=$1 AND v.job_id=$2 AND a.kind=$5 AND a.id=ANY($4::uuid[])
                   LIMIT 1""", event["video_id"], event["job_id"], event["stage_id"], event["artifact_ids"], kind,
            )

    async def finish_frames(self, event, records, error=None, kind="frames"):
        event_id = uuid4()
        async with self.pool.acquire() as conn, conn.transaction():
            created = await conn.fetchval(
                """INSERT INTO reels_router.stages (id,video_id,kind,status,error,finished_at)
                   VALUES ($1,$2,$5,$3,$4,now()) ON CONFLICT (id) DO NOTHING RETURNING id""",
                event["stage_id"], event["video_id"], "failed" if error else "completed", error, kind,
            )
            if created is None:
                return False
            for record in records:
                await conn.execute(
                    """INSERT INTO reels_router.artifacts (id,stage_id,kind,bucket,object_key,size_bytes,sha256)
                       VALUES ($1,$2,$3,$4,$5,$6,$7)""", record["id"], event["stage_id"], record["kind"],
                    record["bucket"], record["object_key"], record["size_bytes"], record["sha256"],
                )
            output = {**event, "event_id": str(event_id), "type": f"{kind}.failed" if error else f"{kind}.completed",
                      "artifact_ids": [str(record["id"]) for record in records],
                      "created_at": datetime.now(timezone.utc).isoformat()}
            await conn.execute(
                "INSERT INTO reels_router.outbox (id,topic,message_key,payload) VALUES ($1,$2,$3,$4::jsonb)",
                event_id, self.events_topic, str(event["video_id"]), json.dumps(output),
            )
        return True

    async def finish_media(self, event, records, error=None):
        event_id = uuid4()
        async with self.pool.acquire() as conn, conn.transaction():
            # Один и тот же stage не должен опубликовать результат дважды.
            created = await conn.fetchval(
                """INSERT INTO reels_router.stages (id,video_id,kind,status,error,finished_at)
                   VALUES ($1,$2,'media',$3,$4,now()) ON CONFLICT (id) DO NOTHING RETURNING id""",
                event["stage_id"], event["video_id"], "failed" if error else "completed", error,
            )
            if created is None:
                return False
            for record in records:
                await conn.execute(
                    """INSERT INTO reels_router.artifacts (id,stage_id,kind,bucket,object_key,size_bytes,sha256)
                       VALUES ($1,$2,$3,$4,$5,$6,$7)""", record["id"], event["stage_id"],
                    record["kind"], record["bucket"], record["object_key"], record["size_bytes"], record["sha256"],
                )
            await conn.execute("UPDATE reels_router.videos SET status=$1 WHERE id=$2",
                               "failed" if error else "media_ready", event["video_id"])
            await self.refresh_job(conn, event["job_id"])
            output = {**event, "event_id": str(event_id), "type": "media.failed" if error else "media.completed",
                      "artifact_ids": [str(record["id"]) for record in records],
                      "created_at": datetime.now(timezone.utc).isoformat()}
            await conn.execute(
                "INSERT INTO reels_router.outbox (id,topic,message_key,payload) VALUES ($1,$2,$3,$4::jsonb)",
                event_id, self.events_topic, str(event["video_id"]), json.dumps(output),
            )
        return True

    async def publish_one(self, producer):
        # Блокировка до отметки публикации допускает дубль после сбоя — это at-least-once.
        async with self.pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                """SELECT id,topic,message_key,payload FROM reels_router.outbox
                   WHERE published_at IS NULL AND (next_attempt_at IS NULL OR next_attempt_at <= now()) ORDER BY created_at,id
                   FOR UPDATE SKIP LOCKED LIMIT 1"""
            )
            if row is None:
                return False
            try:
                await producer.send_and_wait(row["topic"], row["payload"].encode(), key=row["message_key"].encode())
            except Exception:
                await conn.execute("UPDATE reels_router.outbox SET next_attempt_at=now()+interval '5 seconds' WHERE id=$1", row["id"])
                log.warning("Не удалось отправить событие Kafka; повтор через 5 секунд")
                return True
            await conn.execute("UPDATE reels_router.outbox SET published_at=now() WHERE id=$1", row["id"])
        return True

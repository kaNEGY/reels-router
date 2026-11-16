"""Отбор информативных кадров EmbeddingGemma на CPU."""

import asyncio
import json
import logging
import ssl
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import UUID

import asyncpg
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, HTTPException, Request

from .media_worker import consume
from .repository import Repository
from .settings import Settings
from .storage import Storage

log = logging.getLogger(__name__)


async def handle_frames(event, repository, storage, settings):
    for field in ("event_id", "job_id", "video_id", "stage_id"):
        UUID(event[field])
    if event.get("type") != "frames.requested" or event.get("schema_version") != 1:
        raise ValueError("Неизвестный тип команды")
    row = await repository.frames_input(event)
    if row is None:
        raise ValueError("Исходное видео не найдено")
    if row["stage_status"]:
        return
    for attempt in range(3):
        try:
            with TemporaryDirectory(prefix="reels-frames-", dir=settings.scratch_dir) as folder:
                root = Path(folder)
                video = root / "video.mp4"
                await storage.download(row["bucket"], row["object_key"], video)
                settings.ranker.output_dir = root
                result_dir = await asyncio.to_thread(settings.ranker.rank_video, video, settings.frame_top_k)
                result_path = result_dir / "results.json"
                report = json.loads(result_path.read_text(encoding="utf-8"))
                files = {"frames_manifest": result_path}
                for index, frame in enumerate(report["diverse"], 1):
                    files[f"frame_{index}"] = result_dir / frame["frame"]
                records = await storage.upload(event["job_id"], event["video_id"], event["stage_id"], files)
            await repository.finish_frames(event, records)
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            log.warning("Ошибка frames, попытка %s из 3", attempt + 1)
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    await repository.finish_frames(event, [], error="Не удалось обработать кадры после 3 попыток")


@asynccontextmanager
async def lifespan(app: FastAPI):
    from gemma_frames import FrameRanker

    settings = Settings()
    pool = await asyncpg.create_pool(host=settings.pg_host, port=settings.pg_port, database=settings.pg_database,
                                     user=settings.pg_user, password=settings.pg_password, ssl=settings.pg_sslmode)
    options = {"bootstrap_servers": settings.kafka_bootstrap_servers,
               "security_protocol": settings.kafka_security_protocol}
    if settings.kafka_security_protocol in {"SSL", "SASL_SSL"}:
        options["ssl_context"] = ssl.create_default_context(cafile=settings.kafka_ssl_ca_file or None)
    if settings.kafka_security_protocol.startswith("SASL"):
        options.update(sasl_mechanism=settings.kafka_sasl_mechanism,
                       sasl_plain_username=settings.kafka_sasl_username,
                       sasl_plain_password=settings.kafka_sasl_password)
    consumer = AIOKafkaConsumer(settings.kafka_topic_frames, group_id=settings.kafka_group_frames,
                                enable_auto_commit=False, auto_offset_reset="earliest",
                                max_poll_interval_ms=max(3600000, settings.stage_timeout_seconds * 8000), **options)
    try:
        # Модель загружается единожды; локальный путь к весам задаётся через GEMMA_MODEL.
        with TemporaryDirectory(prefix="reels-frame-model-", dir=settings.scratch_dir) as folder:
            ranker = await asyncio.to_thread(FrameRanker, folder, settings.gemma_model, settings.gemma_cpu_threads)
            # Не передаём модель в сериализацию настроек и не меняем объект Pydantic динамически.
            class WorkerSettings:
                def __getattr__(self, name):
                    return getattr(settings, name)
            worker_settings = WorkerSettings()
            worker_settings.ranker = ranker
            await consumer.start()
            repository = Repository(pool, settings.kafka_topic_media, settings.kafka_topic_events)
            app.state.task = asyncio.create_task(consume(consumer, repository, Storage(settings), worker_settings,
                                                         handler=handle_frames))
            app.state.pool = pool
            try:
                yield
            finally:
                app.state.task.cancel()
                with suppress(asyncio.CancelledError):
                    await app.state.task
    finally:
        await consumer.stop()
        await pool.close()


app = FastAPI(title="Reels Router frames", lifespan=lifespan)


@app.get("/health/live")
async def live():
    return {"status": "alive"}


@app.get("/health/ready")
async def ready(request: Request):
    try:
        if request.app.state.task.done():
            raise RuntimeError("consumer stopped")
        async with request.app.state.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
    except Exception:
        raise HTTPException(status_code=503, detail="Сервис не готов") from None
    return {"status": "ready"}

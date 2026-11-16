"""FastAPI: приём задач и публикация команд через transactional outbox."""

import asyncio
import hmac
import logging
import ssl
from contextlib import asynccontextmanager, suppress
from uuid import UUID

import asyncpg
import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, Header, HTTPException, Request, Response

from .repository import IdempotencyConflict, InvalidSelection, InvalidStage, Repository
from .asr_worker import handle_asr
from .media_worker import consume
from .providers import OpenAICompatible
from .schemas import CreateJob, JobAccepted, Selections, StageRequest
from .settings import Settings
from .storage import Storage
from .vlm_worker import handle_vlm
from .llm_worker import handle_llm
from .geo_worker import Geoapify, handle_geo

log = logging.getLogger(__name__)


async def publish_loop(repository, producer):
    while True:
        try:
            if not await repository.publish_one(producer):
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Не удалось опубликовать outbox; повтор через 5 секунд")
            await asyncio.sleep(5)


async def coordinate_loop(repository, consumer, frames_topic, asr_topic, vlm_topic, llm_topic, geo_topic):
    import json

    async for message in consumer:
        event = json.loads(message.value)
        while True:
            try:
                await repository.coordinate(event, frames_topic, asr_topic, vlm_topic, llm_topic, geo_topic)
                await consumer.commit()
                break
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Ошибка обработки события; повтор через 5 секунд")
                await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    asr_config = (settings.asr_provider, settings.asr_base_url, settings.asr_api_key, settings.asr_model)
    vlm_config = (settings.vlm_provider, settings.vlm_base_url, settings.vlm_api_key, settings.vlm_model)
    llm_config = (settings.llm_provider, settings.llm_base_url, settings.llm_api_key, settings.llm_model)
    if any(asr_config) and not all(asr_config):
        raise RuntimeError("Укажите все ASR_PROVIDER, ASR_BASE_URL, ASR_API_KEY и ASR_MODEL")
    if any(vlm_config) and not all(vlm_config):
        raise RuntimeError("Укажите все VLM_PROVIDER, VLM_BASE_URL, VLM_API_KEY и VLM_MODEL")
    if any(llm_config) and not all(llm_config):
        raise RuntimeError("Укажите все LLM_PROVIDER, LLM_BASE_URL, LLM_API_KEY и LLM_MODEL")
    pool = await asyncpg.create_pool(
        host=settings.pg_host, port=settings.pg_port, database=settings.pg_database,
        user=settings.pg_user, password=settings.pg_password, ssl=settings.pg_sslmode,
        min_size=1, max_size=5,
    )
    options = {"bootstrap_servers": settings.kafka_bootstrap_servers,
               "security_protocol": settings.kafka_security_protocol}
    if settings.kafka_security_protocol in {"SSL", "SASL_SSL"}:
        options["ssl_context"] = ssl.create_default_context(cafile=settings.kafka_ssl_ca_file or None)
    if settings.kafka_security_protocol.startswith("SASL"):
        options.update(sasl_mechanism=settings.kafka_sasl_mechanism,
                       sasl_plain_username=settings.kafka_sasl_username,
                       sasl_plain_password=settings.kafka_sasl_password)
    producer = AIOKafkaProducer(**options)
    consumer = AIOKafkaConsumer(settings.kafka_topic_events, group_id=settings.kafka_group_coordinator,
                                enable_auto_commit=False, auto_offset_reset="earliest", **options)
    asr_consumer = (AIOKafkaConsumer(settings.kafka_topic_asr, group_id=settings.kafka_group_asr,
                                    enable_auto_commit=False, auto_offset_reset="earliest",
                                    max_poll_interval_ms=1800000, **options)
                    if all(asr_config) else None)
    vlm_consumer = (AIOKafkaConsumer(settings.kafka_topic_vlm, group_id=settings.kafka_group_vlm,
                                    enable_auto_commit=False, auto_offset_reset="earliest",
                                    max_poll_interval_ms=7200000, **options)
                    if all(vlm_config) else None)
    llm_consumer = (AIOKafkaConsumer(settings.kafka_topic_llm, group_id=settings.kafka_group_llm,
                                    enable_auto_commit=False, auto_offset_reset="earliest",
                                    max_poll_interval_ms=1800000, **options)
                    if all(llm_config) else None)
    geo_consumer = (AIOKafkaConsumer(settings.kafka_topic_geo, group_id=settings.kafka_group_geo,
                                    enable_auto_commit=False, auto_offset_reset="earliest",
                                    max_poll_interval_ms=7200000, **options)
                    if settings.geoapify_api_key else None)
    client = httpx.AsyncClient(timeout=httpx.Timeout(120.0)) if any((asr_consumer, vlm_consumer, llm_consumer, geo_consumer)) else None
    try:
        await producer.start()
        await consumer.start()
        if asr_consumer:
            await asr_consumer.start()
        if vlm_consumer:
            await vlm_consumer.start()
        if llm_consumer:
            await llm_consumer.start()
        if geo_consumer:
            await geo_consumer.start()
        app.state.repository = Repository(pool, settings.kafka_topic_media, settings.kafka_topic_events)
        app.state.settings = settings
        app.state.publisher = asyncio.create_task(publish_loop(app.state.repository, producer))
        app.state.coordinator = asyncio.create_task(coordinate_loop(app.state.repository, consumer,
                                                                    settings.kafka_topic_frames, settings.kafka_topic_asr,
                                                                    settings.kafka_topic_vlm, settings.kafka_topic_llm,
                                                                    settings.kafka_topic_geo))
        app.state.tasks = [app.state.publisher, app.state.coordinator]
        if asr_consumer:
            provider = OpenAICompatible(client, settings.asr_base_url, settings.asr_api_key,
                                        settings.asr_model, settings.asr_provider)
            async def asr_handler(event, repository, storage, worker_settings):
                await handle_asr(event, repository, storage, worker_settings, provider)
            app.state.tasks.append(asyncio.create_task(consume(asr_consumer, app.state.repository,
                                                              Storage(settings), settings, handler=asr_handler)))
        if vlm_consumer:
            vlm_provider = OpenAICompatible(client, settings.vlm_base_url, settings.vlm_api_key,
                                            settings.vlm_model, settings.vlm_provider)
            async def vlm_handler(event, repository, storage, worker_settings):
                await handle_vlm(event, repository, storage, worker_settings, vlm_provider)
            app.state.tasks.append(asyncio.create_task(consume(vlm_consumer, app.state.repository,
                                                              Storage(settings), settings, handler=vlm_handler)))
        if llm_consumer:
            llm_provider = OpenAICompatible(client, settings.llm_base_url, settings.llm_api_key,
                                            settings.llm_model, settings.llm_provider)
            async def llm_handler(event, repository, storage, worker_settings):
                await handle_llm(event, repository, storage, worker_settings, llm_provider)
            app.state.tasks.append(asyncio.create_task(consume(llm_consumer, app.state.repository,
                                                              Storage(settings), settings, handler=llm_handler)))
        if geo_consumer:
            geo_provider = Geoapify(client, settings.geoapify_api_key, settings.geoapify_base_url)
            async def geo_handler(event, repository, storage, worker_settings):
                await handle_geo(event, repository, storage, worker_settings, geo_provider)
            app.state.tasks.append(asyncio.create_task(consume(geo_consumer, app.state.repository,
                                                              Storage(settings), settings, handler=geo_handler)))
        try:
            yield
        finally:
            for task in app.state.tasks:
                task.cancel()
            for task in app.state.tasks:
                with suppress(asyncio.CancelledError):
                    await task
    finally:
        if geo_consumer:
            await geo_consumer.stop()
        if llm_consumer:
            await llm_consumer.stop()
        if vlm_consumer:
            await vlm_consumer.stop()
        if asr_consumer:
            await asr_consumer.stop()
        if client:
            await client.aclose()
        await consumer.stop()
        await producer.stop()
        await pool.close()


app = FastAPI(title="Reels Router", lifespan=lifespan)


@app.get("/health/live")
async def live():
    return {"status": "alive"}


@app.get("/health/ready")
async def ready(request: Request):
    try:
        if any(task.done() for task in request.app.state.tasks):
            raise RuntimeError("background task stopped")
        async with request.app.state.repository.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
    except Exception:
        raise HTTPException(status_code=503, detail="Сервис не готов") from None
    return {"status": "ready"}


@app.post("/v1/jobs", response_model=JobAccepted, status_code=202)
async def create_job(body: CreateJob, request: Request, response: Response,
                     idempotency_key: str | None = Header(default=None, min_length=1, max_length=200)):
    try:
        job, created = await request.app.state.repository.create(body, idempotency_key)
    except IdempotencyConflict:
        raise HTTPException(status_code=409, detail="Idempotency-Key уже использован с другим запросом") from None
    if not created:
        response.status_code = 200
    return {"job_id": job["id"], "status": job["status"]}


@app.get("/v1/jobs/{job_id}")
async def get_job(job_id: UUID, request: Request):
    job = await request.app.state.repository.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Задача не найдена")
    return job


@app.post("/v1/jobs/{job_id}/selections")
async def select_places(job_id: UUID, body: Selections, request: Request):
    try:
        found = await request.app.state.repository.select_places(job_id, body.selections)
    except InvalidSelection:
        raise HTTPException(status_code=422, detail="Кандидат или упоминание не относятся к этой задаче") from None
    if not found:
        raise HTTPException(status_code=404, detail="Задача не найдена")
    return await request.app.state.repository.get(job_id)


@app.post("/internal/stages/{kind}", status_code=202)
async def enqueue_stage(kind: str, body: StageRequest, request: Request,
                        x_internal_token: str = Header(default="")):
    settings = request.app.state.settings
    if not settings.internal_api_token:
        raise HTTPException(status_code=503, detail="Внутренний API отключён")
    if not hmac.compare_digest(settings.internal_api_token.encode(), x_internal_token.encode()):
        raise HTTPException(status_code=401, detail="Неверный токен")
    topics = {"media": settings.kafka_topic_media, "frames": settings.kafka_topic_frames,
              "asr": settings.kafka_topic_asr, "vlm": settings.kafka_topic_vlm,
              "llm": settings.kafka_topic_llm, "geo": settings.kafka_topic_geo}
    if kind not in topics:
        raise HTTPException(status_code=404, detail="Неизвестный этап")
    try:
        stage_id = await request.app.state.repository.enqueue_stage(body, kind, topics[kind])
    except InvalidStage:
        raise HTTPException(status_code=409, detail="Неверные входы, состояние или этап уже поставлен") from None
    return {"stage_id": stage_id}

"""FastAPI и Kafka consumer для одного media-процесса."""

import asyncio
import json
import logging
import ssl
from contextlib import asynccontextmanager, suppress
from tempfile import TemporaryDirectory
from uuid import UUID

import asyncpg
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, HTTPException, Request

from .media import prepare
from .repository import Repository
from .settings import Settings
from .storage import Storage

log = logging.getLogger(__name__)


async def handle(event, repository, storage, settings):
    for field in ("event_id", "job_id", "video_id", "stage_id"):
        event[field] = UUID(event[field])
    event = {key: str(value) if isinstance(value, UUID) else value for key, value in event.items()}
    if event.get("type") != "media.requested" or event.get("schema_version") != 1:
        raise ValueError("Неизвестный тип команды")
    row = await repository.media_input(event)
    if row is None:
        raise ValueError("Команда ссылается на неизвестный ролик")
    if row["stage_status"]:
        return
    for attempt in range(3):
        try:
            with TemporaryDirectory(prefix="reels-media-", dir=settings.scratch_dir) as folder:
                files = await prepare(row["url"], folder, settings)
                records = await storage.upload(event["job_id"], event["video_id"], event["stage_id"], files)
            await repository.finish_media(event, records)
            return
        except (asyncio.CancelledError, KeyboardInterrupt):
            raise
        except Exception:
            log.warning("Ошибка media, попытка %s из 3", attempt + 1)
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    # Не записываем сообщение исключения: оно может содержать временные URL или секреты.
    await repository.finish_media(event, [], error="Не удалось обработать видео после 3 попыток")


async def consume(consumer, repository, storage, settings, handler=handle):
    async for message in consumer:
        # Пока запись не сохранена в БД, не переходим к следующему сообщению и не коммитим offset.
        while True:
            try:
                await handler(json.loads(message.value), repository, storage, settings)
                await consumer.commit()
                break
            except asyncio.CancelledError:
                raise
            except (ValueError, KeyError, TypeError, json.JSONDecodeError):
                log.error("Некорректная команда media; обработчик остановлен")
                raise
            except Exception:
                log.exception("Ошибка сохранения media; повтор через 5 секунд")
                await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
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
    consumer = AIOKafkaConsumer(settings.kafka_topic_media, group_id=settings.kafka_group_media,
                                enable_auto_commit=False, auto_offset_reset="earliest",
                                max_poll_interval_ms=max(1800000, settings.stage_timeout_seconds * 8000), **options)
    try:
        await consumer.start()
        repository = Repository(pool, settings.kafka_topic_media, settings.kafka_topic_events)
        app.state.task = asyncio.create_task(consume(consumer, repository, Storage(settings), settings))
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


app = FastAPI(title="Reels Router media", lifespan=lifespan)


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

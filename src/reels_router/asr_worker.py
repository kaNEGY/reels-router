"""Очередь ASR для независимого внешнего провайдера."""

import asyncio
import json
import logging
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import UUID

log = logging.getLogger(__name__)


async def handle_asr(event, repository, storage, settings, provider):
    for field in ("event_id", "job_id", "video_id", "stage_id"):
        UUID(event[field])
    if event.get("type") != "asr.requested" or event.get("schema_version") != 1:
        raise ValueError("Неизвестный тип команды")
    row = await repository.frames_input(event, kind="audio")
    if row is None:
        raise ValueError("Аудио не найдено")
    if row["stage_status"]:
        return
    for attempt in range(3):
        try:
            with TemporaryDirectory(prefix="reels-asr-", dir=settings.scratch_dir) as folder:
                audio = Path(folder) / "audio.flac"
                await storage.download(row["bucket"], row["object_key"], audio)
                if audio.stat().st_size > settings.asr_max_bytes:
                    raise ValueError("Аудио превышает лимит провайдера")
                transcript = await provider.transcribe(audio)
                output = Path(folder) / "transcript.json"
                output.write_text(json.dumps(asdict(transcript), ensure_ascii=False), encoding="utf-8")
                records = await storage.upload(event["job_id"], event["video_id"], event["stage_id"],
                                               {"transcript": output})
            await repository.finish_frames(event, records, kind="asr")
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            log.warning("Ошибка ASR, попытка %s из 3", attempt + 1)
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    await repository.finish_frames(event, [], error="Не удалось выполнить ASR после 3 попыток", kind="asr")

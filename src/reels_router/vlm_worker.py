"""Описание выбранных кадров через сменяемого внешнего провайдера."""

import asyncio
import json
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import UUID

log = logging.getLogger(__name__)
PROMPT = ("Describe the visible geographic clues in this travel video frame. "
          "Transcribe readable place names or signs exactly. Distinguish visible evidence from guesses; "
          "if the location is uncertain, say so. Do not invent an address.")


async def handle_vlm(event, repository, storage, settings, provider):
    for field in ("event_id", "job_id", "video_id", "stage_id"):
        UUID(event[field])
    if event.get("type") != "vlm.requested" or event.get("schema_version") != 1:
        raise ValueError("Неизвестный тип команды")
    stage, records = await repository.artifacts_input(event)
    if stage:
        return
    manifest = next((row for row in records if row["kind"] == "frames_manifest"), None)
    frames = [row for row in records if row["kind"].startswith("frame_")]
    if manifest is None or not frames:
        raise ValueError("Кадры или их manifest не найдены")
    for attempt in range(3):
        try:
            with TemporaryDirectory(prefix="reels-vlm-", dir=settings.scratch_dir) as folder:
                root = Path(folder)
                manifest_path = root / "manifest.json"
                await storage.download(manifest["bucket"], manifest["object_key"], manifest_path)
                report = json.loads(manifest_path.read_text(encoding="utf-8"))
                timestamps = {item["frame"]: item["time_s"] for item in report["diverse"]}
                descriptions = []
                for row in frames:
                    frame = root / Path(row["object_key"]).name
                    if frame.name not in timestamps:
                        raise ValueError("Кадр отсутствует в manifest")
                    await storage.download(row["bucket"], row["object_key"], frame)
                    result = await provider.describe(PROMPT, frame)
                    descriptions.append({"frame_artifact_id": str(row["id"]), "time_s": timestamps[frame.name],
                                         "description": result.text, "provider": result.provider, "model": result.model,
                                         "usage": result.usage})
                output = root / "frame_descriptions.json"
                output.write_text(json.dumps(descriptions, ensure_ascii=False), encoding="utf-8")
                result_records = await storage.upload(event["job_id"], event["video_id"], event["stage_id"],
                                                      {"frame_descriptions": output})
            await repository.finish_frames(event, result_records, kind="vlm")
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            log.warning("Ошибка VLM, попытка %s из 3", attempt + 1)
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    await repository.finish_frames(event, [], error="Не удалось описать кадры после 3 попыток", kind="vlm")

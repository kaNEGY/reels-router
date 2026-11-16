"""Поиск неподтверждённых кандидатов Geoapify по упоминаниям."""

import asyncio
import json
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import UUID

import httpx

log = logging.getLogger(__name__)


class Geoapify:
    def __init__(self, client, api_key, base_url="https://api.geoapify.com"):
        self.client, self.api_key, self.base_url = client, api_key, base_url.rstrip("/")

    async def search(self, name):
        try:
            response = await self.client.get(self.base_url + "/v1/geocode/search",
                                             params={"text": name, "format": "json", "limit": 5, "apiKey": self.api_key})
            response.raise_for_status()
            payload = response.json()
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(f"Geoapify HTTP {exc.response.status_code}") from None
        except (httpx.RequestError, ValueError):
            raise RuntimeError("Geoapify недоступен или вернул некорректный JSON") from None
        result = []
        for row in payload.get("results", []):
            lat, lon, label = row.get("lat"), row.get("lon"), row.get("formatted")
            if not isinstance(lat, (float, int)) or not isinstance(lon, (float, int)) or not isinstance(label, str):
                continue
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                continue
            confidence = (row.get("rank") or {}).get("confidence")
            if not isinstance(confidence, (float, int)):
                confidence = None
            result.append({"place_id": row.get("place_id"), "label": label, "lat": lat, "lon": lon,
                           "confidence": confidence, "details": row})
        return result


async def handle_geo(event, repository, storage, settings, provider):
    for field in ("event_id", "job_id", "video_id", "stage_id"):
        UUID(event[field])
    if event.get("type") != "geo.requested" or event.get("schema_version") != 1:
        raise ValueError("Неизвестный тип команды")
    stage, artifacts = await repository.artifacts_input(event)
    if stage:
        return
    row = next((item for item in artifacts if item["kind"] == "place_mentions"), None)
    if row is None:
        raise ValueError("Упоминания мест не найдены")
    for attempt in range(3):
        try:
            with TemporaryDirectory(prefix="reels-geo-", dir=settings.scratch_dir) as folder:
                path = Path(folder) / "places.json"
                await storage.download(row["bucket"], row["object_key"], path)
                mentions = json.loads(path.read_text(encoding="utf-8"))["places"]
                places = []
                for mention in mentions:
                    candidates = await provider.search(mention["name"])
                    places.append({**mention, "candidates": candidates})
            await repository.finish_geo(event, places)
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            log.warning("Ошибка геопоиска, попытка %s из 3", attempt + 1)
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    await repository.finish_geo(event, [], error="Не удалось найти кандидатов после 3 попыток")

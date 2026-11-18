"""Контракты API-моделей и адаптер для OpenAI-совместимого протокола."""

import asyncio
import base64
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import httpx


@dataclass(frozen=True)
class ModelResult:
    text: str
    model: str
    provider: str
    usage: dict | None = None
    segments: list | None = None


class ASRProvider(Protocol):
    async def transcribe(self, audio: Path) -> ModelResult: ...


class TextProvider(Protocol):
    async def generate(self, prompt: str) -> ModelResult: ...


class VisionProvider(Protocol):
    async def describe(self, prompt: str, image: Path) -> ModelResult: ...


class ProviderError(Exception):
    """Ошибка без тела ответа и URL: они могут содержать чувствительные данные."""


class OpenAICompatible:
    def __init__(self, client: httpx.AsyncClient, base_url: str, api_key: str, model: str, provider: str):
        if not all((base_url, api_key, model, provider)):
            raise ValueError("Не заданы настройки провайдера")
        self.client, self.base_url, self.model, self.provider = client, base_url.rstrip("/"), model, provider
        self.headers = {"Authorization": f"Bearer {api_key}"}

    async def _post(self, path, **kwargs):
        try:
            response = await self.client.post(self.base_url + path, headers=self.headers, **kwargs)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("Ожидался JSON-объект")
            return payload
        except httpx.HTTPStatusError as exc:
            raise ProviderError(f"Провайдер вернул HTTP {exc.response.status_code}") from None
        except (httpx.RequestError, ValueError):
            raise ProviderError("Провайдер недоступен или вернул некорректный JSON") from None

    async def transcribe(self, audio: Path) -> ModelResult:
        content = await asyncio.to_thread(audio.read_bytes)
        data = await self._post("/audio/transcriptions", data={"model": self.model, "response_format": "json"},
                                files={"file": (audio.name, content, "audio/flac")})
        if not isinstance(data.get("text"), str):
            raise ProviderError("В ответе ASR нет текста")
        return ModelResult(data["text"], self.model, self.provider, data.get("usage"), data.get("segments"))

    async def _chat(self, content) -> ModelResult:
        data = await self._post("/chat/completions", json={"model": self.model,
                                "messages": [{"role": "user", "content": content}]})
        try:
            text = data["choices"][0]["message"]["content"]
            if not isinstance(text, str):
                raise TypeError
        except (KeyError, IndexError, TypeError):
            raise ProviderError("В ответе модели нет текста") from None
        return ModelResult(text, self.model, self.provider, data.get("usage"))

    async def generate(self, prompt: str) -> ModelResult:
        return await self._chat(prompt)

    async def describe(self, prompt: str, image: Path) -> ModelResult:
        encoded = base64.b64encode(await asyncio.to_thread(image.read_bytes)).decode("ascii")
        return await self._chat([{"type": "text", "text": prompt},
                                 {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + encoded}}])

"""Внешний контракт первого этапа."""

import re
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class CreateJob(BaseModel):
    urls: list[str] = Field(min_length=1, max_length=10)
    context: str | None = Field(default=None, max_length=2000)

    @field_validator("urls")
    @classmethod
    def normalize_urls(cls, urls: list[str]) -> list[str]:
        result = []
        for url in urls:
            parts = urlsplit(url)
            if (
                parts.scheme != "https"
                or parts.netloc.lower() not in {"instagram.com", "www.instagram.com"}
                or not re.fullmatch(r"/reel/[A-Za-z0-9_-]+/?", parts.path)
            ):
                raise ValueError("Ожидается HTTPS-ссылка на Instagram Reel")
            result.append("https://www.instagram.com" + parts.path.rstrip("/") + "/")
        if len(set(result)) != len(result):
            raise ValueError("Ссылки на ролики не должны повторяться")
        return result


class JobAccepted(BaseModel):
    job_id: UUID
    status: str


class Selection(BaseModel):
    mention_id: UUID
    candidate_id: UUID | None = None


class Selections(BaseModel):
    selections: list[Selection] = Field(min_length=1, max_length=100)

    @field_validator("selections")
    @classmethod
    def unique_mentions(cls, selections):
        if len({item.mention_id for item in selections}) != len(selections):
            raise ValueError("Каждое упоминание можно выбрать только один раз")
        return selections


class StageRequest(BaseModel):
    job_id: UUID
    video_id: UUID
    artifact_ids: list[UUID] = Field(default_factory=list, max_length=25)

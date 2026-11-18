from unittest.mock import AsyncMock
import json
from uuid import uuid4

import pytest

from reels_router import media_worker
from reels_router.asr_worker import handle_asr
from reels_router.frames_worker import handle_frames
from reels_router.providers import ModelResult
from reels_router.vlm_worker import handle_vlm
from reels_router.storage import Storage


@pytest.mark.anyio
async def test_media_handler_uploads_and_records(monkeypatch, tmp_path):
    ids = [str(uuid4()) for _ in range(4)]
    event = dict(zip(("event_id", "job_id", "video_id", "stage_id"), ids))
    event.update(type="media.requested", schema_version=1, attempt=1, artifact_ids=[])
    repository = type("Repo", (), {})()
    repository.media_input = AsyncMock(return_value={"url": "https://example.test", "stage_status": None})
    repository.finish_media = AsyncMock()
    storage = type("Store", (), {})()
    storage.upload = AsyncMock(return_value=[{"id": uuid4()}])
    async def prepare(url, folder, settings):
        return {"video": tmp_path / "video.mp4"}
    monkeypatch.setattr(media_worker, "prepare", prepare)
    settings = type("Settings", (), {"scratch_dir": str(tmp_path)})()
    await media_worker.handle(event, repository, storage, settings)
    repository.finish_media.assert_awaited_once()
    storage.upload.assert_awaited_once()


@pytest.mark.anyio
async def test_media_duplicate_does_not_upload(tmp_path):
    event = {name: str(uuid4()) for name in ("event_id", "job_id", "video_id", "stage_id")}
    event.update(type="media.requested", schema_version=1)
    repository = type("Repo", (), {})()
    repository.media_input = AsyncMock(return_value={"url": "https://example.test", "stage_status": "completed"})
    storage = type("Store", (), {})()
    storage.upload = AsyncMock()
    await media_worker.handle(event, repository, storage, object())
    storage.upload.assert_not_awaited()


def test_storage_hash(tmp_path):
    path = tmp_path / "sample"
    path.write_text("abc", encoding="utf-8")
    assert Storage._hash(path) == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


@pytest.mark.anyio
async def test_asr_duplicate_does_not_call_provider():
    event = {name: str(uuid4()) for name in ("event_id", "job_id", "video_id", "stage_id")}
    event.update(type="asr.requested", schema_version=1)
    repository = type("Repo", (), {})()
    repository.frames_input = AsyncMock(return_value={"stage_status": "completed"})
    provider = type("Provider", (), {})()
    provider.transcribe = AsyncMock()
    await handle_asr(event, repository, object(), object(), provider)
    provider.transcribe.assert_not_awaited()


@pytest.mark.anyio
async def test_frames_handler_uploads_selected_frames(tmp_path):
    event = {name: str(uuid4()) for name in ("event_id", "job_id", "video_id", "stage_id")}
    event.update(type="frames.requested", schema_version=1, artifact_ids=[])
    repository = type("Repo", (), {})()
    repository.frames_input = AsyncMock(return_value={"bucket": "bucket", "object_key": "video", "stage_status": None})
    repository.finish_frames = AsyncMock()
    storage = type("Store", (), {})()
    storage.download = AsyncMock()
    storage.upload = AsyncMock(return_value=[{"id": uuid4()}])
    class Ranker:
        def rank_video(self, video, k):
            output = self.output_dir / "result"
            output.mkdir()
            (output / "frame.jpg").write_bytes(b"image")
            (output / "results.json").write_text(json.dumps({"diverse": [{"frame": "frame.jpg"}]}), encoding="utf-8")
            return output
    settings = type("Settings", (), {"scratch_dir": str(tmp_path), "ranker": Ranker(), "frame_top_k": 3})()
    await handle_frames(event, repository, storage, settings)
    assert set(storage.upload.await_args.args[3]) == {"frames_manifest", "frame_1"}
    repository.finish_frames.assert_awaited_once()


@pytest.mark.anyio
async def test_vlm_keeps_frame_evidence(tmp_path):
    event = {name: str(uuid4()) for name in ("event_id", "job_id", "video_id", "stage_id")}
    event.update(type="vlm.requested", schema_version=1, artifact_ids=[])
    frame_id = uuid4()
    rows = [{"id": uuid4(), "kind": "frames_manifest", "bucket": "test", "object_key": "manifest.json"},
            {"id": frame_id, "kind": "frame_1", "bucket": "test", "object_key": "frame.jpg"}]
    repository = type("Repo", (), {})()
    repository.artifacts_input = AsyncMock(return_value=(None, rows))
    repository.finish_frames = AsyncMock()
    storage = type("Store", (), {})()
    async def download(bucket, key, path):
        if key == "manifest.json":
            path.write_text(json.dumps({"diverse": [{"frame": "frame.jpg", "time_s": 2.5}]}), encoding="utf-8")
        else:
            path.write_bytes(b"image")
    async def upload(job_id, video_id, stage_id, files):
        data = json.loads(files["frame_descriptions"].read_text(encoding="utf-8"))
        assert data[0]["frame_artifact_id"] == str(frame_id) and data[0]["time_s"] == 2.5
        return [{"id": uuid4()}]
    storage.download, storage.upload = download, upload
    provider = type("Provider", (), {})()
    provider.describe = AsyncMock(return_value=ModelResult("A sign", "model", "provider"))
    settings = type("Settings", (), {"scratch_dir": str(tmp_path)})()
    await handle_vlm(event, repository, storage, settings, provider)
    repository.finish_frames.assert_awaited_once()

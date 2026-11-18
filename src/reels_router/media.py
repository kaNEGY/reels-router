"""Скачивание одного ролика и подготовка первой аудиодорожки."""

import asyncio
import json
import os
import signal
from pathlib import Path


async def run_command(*args, timeout):
    process = await asyncio.create_subprocess_exec(
        *map(str, args), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        stdout, _ = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await process.wait()
        raise
    if process.returncode:
        # Вывод утилиты может содержать cookies или временные URL, поэтому не логируем его.
        raise RuntimeError(f"{Path(str(args[0])).name}: ошибка обработки")
    return stdout


async def prepare(url, folder, settings):
    folder = Path(folder)
    cmd = ["yt-dlp", "--no-playlist", "--no-progress", "--no-warnings", "--write-info-json",
           "--max-filesize", str(settings.max_download_bytes), "--output", str(folder / "video.%(ext)s")]
    if settings.instagram_cookies_file:
        cmd += ["--cookies", settings.instagram_cookies_file]
    await run_command(*cmd, "--", url, timeout=settings.stage_timeout_seconds)
    files = [p for p in folder.glob("video.*") if p.is_file() and not p.name.endswith(".info.json")]
    if len(files) != 1 or files[0].stat().st_size > settings.max_download_bytes:
        raise ValueError("Видео не скачано или превышает лимит размера")
    video = files[0]
    info_files = list(folder.glob("video.*.info.json")) + list(folder.glob("video.info.json"))
    info = json.loads(info_files[0].read_text(encoding="utf-8")) if info_files else {}
    probe = json.loads(await run_command("ffprobe", "-v", "error", "-show_format", "-show_streams",
                                         "-of", "json", video, timeout=30))
    duration = float(probe.get("format", {}).get("duration", 0))
    if duration <= 0 or duration > settings.max_video_seconds:
        raise ValueError("Длительность видео недоступна или превышает лимит")
    metadata = {key: info.get(key) for key in ("id", "title", "description", "uploader", "upload_date")}
    metadata.update(duration_seconds=duration,
                    subtitle_languages=list((info.get("subtitles") or {}).keys()),
                    automatic_caption_languages=list((info.get("automatic_captions") or {}).keys()))
    metadata_path = folder / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False), encoding="utf-8")
    result = {"video": video, "metadata": metadata_path}
    if any(stream.get("codec_type") == "audio" for stream in probe.get("streams", [])):
        audio = folder / "audio.flac"
        await run_command("ffmpeg", "-nostdin", "-v", "error", "-i", video, "-map", "0:a:0",
                          "-ar", "16000", "-ac", "1", "-c:a", "flac", audio,
                          timeout=settings.stage_timeout_seconds)
        result["audio"] = audio
    return result

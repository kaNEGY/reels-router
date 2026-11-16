"""Проверка FFmpeg на ранее скачанном видео без повторного доступа к Instagram."""

import asyncio
import json
import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

from reels_router import media
from reels_router.settings import Settings


async def main():
    original = media.run_command

    async def supplied_video(*args, timeout):
        if args[0] == "yt-dlp":
            target = Path(args[args.index("--output") + 1].replace("%(ext)s", "mp4"))
            shutil.copyfile(os.environ["TEST_VIDEO_PATH"], target)
            target.with_suffix(".info.json").write_text(json.dumps({"id": "local-test"}), encoding="utf-8")
            return b""
        return await original(*args, timeout=timeout)

    media.run_command = supplied_video
    with TemporaryDirectory() as folder:
        files = await media.prepare("https://instagram.com/reel/De4IKljoBfq/", folder, Settings())
        assert {"video", "audio", "metadata"} == set(files)
        probe = json.loads(await original("ffprobe", "-v", "error", "-show_streams", "-of", "json",
                                          files["audio"], timeout=30))
        stream = probe["streams"][0]
        assert stream["codec_name"] == "flac" and stream["sample_rate"] == "16000" and stream["channels"] == 1
        print("Media preparation passed on locally downloaded reel: mono 16 kHz FLAC")


if __name__ == "__main__":
    asyncio.run(main())

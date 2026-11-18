#!/usr/local/bin/python
"""Подмена только скачивания: видео уже получено для теста и смонтировано отдельно."""

import json
import shutil
import sys
from pathlib import Path

output = Path(sys.argv[sys.argv.index("--output") + 1].replace("%(ext)s", "mp4"))
shutil.copyfile("/fixture.mp4", output)
output.with_suffix(".info.json").write_text(json.dumps({"id": "e2e", "description": "Kyoto Station"}), encoding="utf-8")

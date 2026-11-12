"""Отбор кадров EmbeddingGemma 2 по пути к локальному видео."""

import json
import re
import subprocess
from pathlib import Path
from tempfile import mkdtemp
from time import perf_counter, process_time

import imageio_ffmpeg
import numpy as np
import torch
from PIL import Image, ImageDraw, ImageFont
from transformers import AutoConfig, AutoModel, AutoProcessor

MODEL_ID = "google/embeddinggemma-2"
REVISION = "f9972ac22e696a105c482202646979e33e4b5a1e"
QUERIES = (
    "a legible sign or storefront showing the name of a place",
    "a street sign, station name or direction sign identifying the location",
    "a distinctive landmark, monument or building that identifies this place",
    "a travel video frame with a readable place name on screen",
    "a scenic natural landmark with a distinctive identifiable shape",
)


def _sample_frames(video, folder):
    # Сохраняем первый кадр в каждой занятой секунде и его таймкод относительно начала ролика.
    command = [
        imageio_ffmpeg.get_ffmpeg_exe(),
        "-nostdin",
        "-hide_banner",
        "-nostats",
        "-threads",
        "1",
        "-i",
        str(video),
        "-map",
        "0:v:0",
        "-an",
        "-filter_threads",
        "1",
        "-vf",
        r"setpts=PTS-STARTPTS,select=isnan(prev_selected_t)+gt(floor(t)\,floor(prev_selected_t)),showinfo",
        "-fps_mode",
        "vfr",
        "-threads",
        "1",
        "-q:v",
        "2",
        str(folder / "%06d.jpg"),
    ]
    run = subprocess.run(command, check=False, capture_output=True, text=True, errors="replace")
    if run.returncode:
        raise ValueError(f"FFmpeg: {video.name}: {run.stderr[-450:]}")
    times = [
        float(t)
        for t in re.findall(r"\bn:\s*\d+.*?\bpts_time:\s*([0-9.eE+-]+)", run.stderr)
    ]
    paths = sorted(folder.glob("*.jpg"))
    if not paths or len(paths) != len(times):
        raise ValueError(f"Не удалось сопоставить кадры с таймкодами: {video.name}")
    return paths, times


class FrameRanker:
    """Загружает модель и пять текстовых запросов один раз на серию роликов."""

    def __init__(self, output_dir, model_path=MODEL_ID, threads=3):
        if threads < 1:
            raise ValueError("threads должен быть положительным")
        started = perf_counter()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.threads = threads
        torch.set_num_threads(threads)
        options = (
            {"local_files_only": True}
            if Path(model_path).is_dir()
            else {"revision": REVISION}
        )
        self.processor = AutoProcessor.from_pretrained(model_path, **options)
        config = AutoConfig.from_pretrained(model_path, **options)
        config.audio_config = None
        self.model = (
            AutoModel.from_pretrained(
                model_path, config=config, dtype=torch.float32, **options
            )
            .cpu()
            .eval()
        )
        self.query_vectors = self._embed(
            self.processor(
                text=["task: search result | query: " + query for query in QUERIES],
                padding=True,
                return_tensors="pt",
            )
        )
        self.setup_seconds = perf_counter() - started

    @torch.inference_mode()
    def _embed(self, inputs):
        hidden = self.model(**inputs).last_hidden_state.float()
        mask = inputs["attention_mask"].unsqueeze(-1)
        pooled = (hidden * mask).sum(1) / mask.sum(1).clamp(min=1)
        return torch.nn.functional.normalize(pooled, dim=-1).cpu().numpy()

    def rank_video(self, video_path, k=10):
        """Обрабатывает один файл, сохраняет JPEG и JSON, возвращает каталог результата."""
        video = Path(video_path)
        if not video.is_file():
            raise FileNotFoundError(video)
        if k < 1:
            raise ValueError("k должен быть положительным")
        started = perf_counter()
        label = video.parent.name if video.stem == "video" else video.stem
        folder = Path(mkdtemp(prefix=f"{label}-", dir=self.output_dir))
        paths, times = _sample_frames(video, folder)
        decoded = perf_counter()
        cpu_started = process_time()
        vectors = []
        for path in paths:
            with Image.open(path) as image:
                inputs = self.processor(
                    images=[image.convert("RGB")],
                    max_soft_tokens=140,
                    return_tensors="pt",
                )
                vectors.append(self._embed(inputs)[0])
        cpu_seconds = process_time() - cpu_started
        encoded = perf_counter()
        vectors = np.stack(vectors)
        scores = vectors @ self.query_vectors.T
        if not np.isfinite(scores).all():
            raise ValueError("Получены неконечные оценки кадров")
        best = scores.max(axis=1)
        order = np.argsort(-best, kind="stable").tolist()
        ranking = [
            {
                "rank": n,
                "frame": paths[i].name,
                "time_s": times[i],
                "score": float(best[i]),
                "query": QUERIES[scores[i].argmax()],
                "scores": scores[i].tolist(),
            }
            for n, i in enumerate(order, 1)
        ]
        selected = []
        diverse = []
        for row, i in zip(ranking, order):
            if all(
                abs(times[i] - times[j]) >= 2 and vectors[i] @ vectors[j] < 0.965
                for j in selected
            ):
                selected.append(i)
                diverse.append(row)
                if len(diverse) == k:
                    break
        elapsed = perf_counter() - started
        report = {
            "video": label,
            "model": MODEL_ID,
            "reference_revision": REVISION,
            "queries": QUERIES,
            "sampling": "first frame per occupied second",
            "soft_tokens": 140,
            "threads": self.threads,
            "metrics": {
                "candidates": len(paths),
                "decode_s": decoded - started,
                "encode_s": encoded - decoded,
                "encode_cpu_s": cpu_seconds,
                "wall_s": elapsed,
                "s_per_frame": (encoded - decoded) / len(paths),
            },
            "ranking": ranking,
            "top": ranking[:k],
            "bottom": ranking[::-1][:k],
            "diverse": diverse,
        }
        (folder / "results.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return folder


def contact_sheet(folder, rows, columns=5):
    """Собирает переданные строки рейтинга в порядке списка."""
    width, height = 155, 222
    sheet = Image.new(
        "RGB",
        (columns * width, ((len(rows) + columns - 1) // columns) * height),
        "#171a1e",
    )
    draw = ImageDraw.Draw(sheet)
    font = ImageFont.load_default(size=13)
    for pos, row in enumerate(rows):
        x, y = (pos % columns) * width, (pos // columns) * height
        with Image.open(Path(folder) / row["frame"]) as frame:
            frame.thumbnail((width - 4, height - 26))
            sheet.paste(frame, (x + (width - frame.width) // 2, y))
        draw.text(
            (x + 2, y + height - 21),
            f"#{row['rank']}   {row['time_s']:.1f}s   {row['score']:.3f}",
            font=font,
            fill="white",
        )
    return sheet

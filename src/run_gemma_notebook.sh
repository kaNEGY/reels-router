#!/bin/sh
# Опциональный повторный запуск на Linux-сервере с подготовленными Docker volumes.
# Для обычного Jupyter достаточно самого ноутбука и requirements.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$root/.env"

: "${GEMMA_CPU_IMAGE:?Укажите образ с совместимым системным Python}"
: "${GEMMA_ENV_VOLUME:?Укажите том с готовым окружением и nb}"
: "${GEMMA_MODEL_VOLUME:?Укажите том с весами EmbeddingGemma 2}"
: "${REELS_HOST_VIDEO_DIR:?Укажите каталог видео на Docker-хосте}"
docker volume inspect "$GEMMA_ENV_VOLUME" "$GEMMA_MODEL_VOLUME" >/dev/null
test -d "$REELS_HOST_VIDEO_DIR"

# REELS_VIDEO_DIR и GEMMA_MODEL — абсолютные пути, видимые ядру контейнера.
docker run --rm --network none --cpus "$GEMMA_CPU_THREADS" --memory 6g \
  --pids-limit 160 --user "$(id -u):$(id -g)" \
  -e HOME=/tmp -e OPENBLAS_NUM_THREADS=1 -e HF_HUB_OFFLINE=1 \
  -e HF_HUB_DISABLE_PROGRESS_BARS=1 -e TOKENIZERS_PARALLELISM=false \
  -e PATH=/tmp/env/bin:/usr/local/bin:/usr/bin:/bin \
  -v "$GEMMA_ENV_VOLUME:/tmp/env:ro" -v "$GEMMA_MODEL_VOLUME:$GEMMA_MODEL:ro" \
  -v "$REELS_HOST_VIDEO_DIR:$REELS_VIDEO_DIR:ro" -v "$root:/work" -w /work \
  "$GEMMA_CPU_IMAGE" sh -ec '
    python -m ipykernel install --user --name python3 >/dev/null
    nb execute research/notebooks/gemma_frame_selection.ipynb --kernel python3 --timeout 3600 "$@" >/dev/null
  ' sh "$@"

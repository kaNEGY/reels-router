FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
ENV OPENBLAS_NUM_THREADS=1 TOKENIZERS_PARALLELISM=false HF_HUB_DISABLE_PROGRESS_BARS=1
WORKDIR /app
COPY requirements.txt requirements-frames.txt ./
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch==2.15.0 torchvision==0.29.2 \
    && pip install --no-cache-dir -r requirements-frames.txt \
    && useradd --system --uid 10001 app
COPY src/reels_router ./src/reels_router
COPY src/gemma_frames.py ./src/gemma_frames.py
RUN chmod -R a+rX /app/src
ENV HOME=/tmp
USER 10001
EXPOSE 8000
CMD ["uvicorn", "reels_router.frames_worker:app", "--host", "0.0.0.0", "--port", "8000"]

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt && useradd --system --uid 10001 app
COPY src/reels_router ./src/reels_router
RUN chmod -R a+rX /app/src
USER 10001
EXPOSE 8000
CMD ["uvicorn", "reels_router.main:app", "--host", "0.0.0.0", "--port", "8000"]

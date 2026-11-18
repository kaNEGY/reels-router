-- Выполняется вручную в выделенной базе проекта; существующие данные не удаляются.
BEGIN;
CREATE SCHEMA IF NOT EXISTS reels_router;
CREATE TABLE IF NOT EXISTS reels_router.jobs (
    id uuid PRIMARY KEY,
    idempotency_key text UNIQUE,
    request_hash text NOT NULL,
    context text,
    status text NOT NULL DEFAULT 'queued',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS reels_router.videos (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES reels_router.jobs(id),
    url text NOT NULL,
    status text NOT NULL DEFAULT 'queued',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS reels_router.outbox (
    id uuid PRIMARY KEY,
    topic text NOT NULL,
    message_key text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON reels_router.outbox (created_at) WHERE published_at IS NULL;
COMMIT;

BEGIN;
CREATE TABLE IF NOT EXISTS reels_router.stages (
    id uuid PRIMARY KEY,
    video_id uuid NOT NULL REFERENCES reels_router.videos(id),
    kind text NOT NULL,
    status text NOT NULL,
    error text,
    finished_at timestamptz
);
CREATE TABLE IF NOT EXISTS reels_router.artifacts (
    id uuid PRIMARY KEY,
    stage_id uuid NOT NULL REFERENCES reels_router.stages(id),
    kind text NOT NULL,
    bucket text NOT NULL,
    object_key text NOT NULL,
    size_bytes bigint NOT NULL,
    sha256 text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (bucket, object_key)
);
COMMIT;

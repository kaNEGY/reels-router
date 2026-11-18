BEGIN;
CREATE TABLE IF NOT EXISTS reels_router.place_mentions (
    id uuid PRIMARY KEY,
    video_id uuid NOT NULL REFERENCES reels_router.videos(id),
    stage_id uuid NOT NULL REFERENCES reels_router.stages(id),
    name text NOT NULL,
    original_name text NOT NULL,
    context text,
    evidence jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS reels_router.place_candidates (
    id uuid PRIMARY KEY,
    mention_id uuid NOT NULL REFERENCES reels_router.place_mentions(id),
    provider_place_id text,
    label text NOT NULL,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    confidence double precision,
    details jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS reels_router.place_selections (
    mention_id uuid PRIMARY KEY REFERENCES reels_router.place_mentions(id),
    candidate_id uuid REFERENCES reels_router.place_candidates(id),
    selected_at timestamptz NOT NULL DEFAULT now()
);
COMMIT;

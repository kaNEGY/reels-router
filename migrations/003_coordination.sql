BEGIN;
CREATE TABLE IF NOT EXISTS reels_router.processed_messages (
    event_id uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);
COMMIT;

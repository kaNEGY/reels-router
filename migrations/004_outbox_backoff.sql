BEGIN;
ALTER TABLE reels_router.outbox ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz;
COMMIT;

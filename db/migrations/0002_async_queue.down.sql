-- 0002_async_queue.down.sql
DROP INDEX IF EXISTS idx_oracle_submissions_resumable;
ALTER TABLE oracle_submissions DROP COLUMN IF EXISTS expires_at;
ALTER TABLE oracle_submissions DROP COLUMN IF EXISTS queued_at;
-- submitted_price is intentionally left nullable: re-adding NOT NULL could
-- fail against rows where a queued/expired request never carried a price.
COMMENT ON COLUMN oracle_submissions.status IS
    'pending | confirmed | failed | dropped';

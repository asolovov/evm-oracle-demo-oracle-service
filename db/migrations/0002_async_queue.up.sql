-- 0002_async_queue.up.sql — async request processing (task 06.1).
--
-- The reaction path is now: stream consumer durably enqueues a `queued` row
-- and advances the cursor immediately; an async worker pool fetches the price
-- + signs; a single sender goroutine serializes on-chain broadcast (nonce).
-- A request that cannot be fulfilled within its TTL is marked `expired`.

-- The price is unknown at enqueue time (a worker fetches it AFTER the request
-- is durably queued), so submitted_price can no longer be NOT NULL.
ALTER TABLE oracle_submissions ALTER COLUMN submitted_price DROP NOT NULL;

-- Per-request bookkeeping for the queue + TTL.
ALTER TABLE oracle_submissions ADD COLUMN IF NOT EXISTS queued_at  TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE oracle_submissions ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ NULL;

-- Fast scan for startup recovery + the overdue-expiry sweep (non-terminal rows).
CREATE INDEX IF NOT EXISTS idx_oracle_submissions_resumable
    ON oracle_submissions (id)
    WHERE status IN ('queued', 'processing', 'sending');

COMMENT ON COLUMN oracle_submissions.status IS
    'queued | processing | sending | pending | confirmed | failed | dropped | expired';

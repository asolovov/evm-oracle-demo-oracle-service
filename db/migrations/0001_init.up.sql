-- 0001_init.up.sql — oracle-service v1 schema.
--
-- Owned exclusively by oracle-service (architecture rule 7). No other
-- service touches `evm_oracle`.

-- Persisted submission lifecycle. One row per fulfillPrice attempt
-- (consumer-driven OR heartbeat).
CREATE TABLE IF NOT EXISTS oracle_submissions (
    id               BIGSERIAL    PRIMARY KEY,
    -- Decimal-string uint256. "0" indicates a heartbeat submission.
    req_id           TEXT         NOT NULL,
    -- Canonical asset id (lowercase symbol for our own universe; the on-chain
    -- bytes32 is keccak256(asset_id)).
    asset_id         TEXT         NOT NULL,
    -- 20-byte aggregator address, 0x-prefixed lowercase hex.
    aggregator       TEXT         NOT NULL,
    -- 32-byte tx hash, 0x-prefixed lowercase hex. Empty until first broadcast.
    tx_hash          TEXT         NOT NULL DEFAULT '',
    -- int256 in Chainlink 8-decimal scale, decimal-string.
    submitted_price  TEXT         NOT NULL,
    -- When the tx was first broadcast (or rebroadcast after replace).
    submitted_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- pending | confirmed | failed | dropped (see models.SubmissionStatus).
    status           TEXT         NOT NULL,
    -- Replace-by-fee retry counter. 0 means the tx was broadcast once.
    retry_count      INT          NOT NULL DEFAULT 0,
    -- Free-form error text from the last failed broadcast or revert.
    last_error       TEXT         NOT NULL DEFAULT '',

    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Lookup by req_id (consumer-driven requests). Not unique because the same
-- req_id can have multiple submissions if we retry past the replace window.
CREATE INDEX IF NOT EXISTS idx_oracle_submissions_req_id
    ON oracle_submissions (req_id);

-- Lookup by tx_hash (most submissions land via this path once broadcast).
CREATE INDEX IF NOT EXISTS idx_oracle_submissions_tx_hash
    ON oracle_submissions (tx_hash)
    WHERE tx_hash <> '';

-- Listing: most-recent first, optionally filtered by asset.
CREATE INDEX IF NOT EXISTS idx_oracle_submissions_asset_submitted
    ON oracle_submissions (asset_id, submitted_at DESC);

-- In-flight tx tracking across restarts. Persisted so a crash mid-broadcast
-- doesn't lose the nonce reservation; the submitter reconciles on startup.
CREATE TABLE IF NOT EXISTS pending_txs (
    id                 BIGSERIAL    PRIMARY KEY,
    submission_id      BIGINT       NOT NULL REFERENCES oracle_submissions(id) ON DELETE CASCADE,
    tx_hash            TEXT         NOT NULL,
    nonce              BIGINT       NOT NULL,
    -- JSON snapshot of the gas strategy used (base fee, tip cap, multiplier,
    -- replace-after). Used for replace-by-fee diagnostics.
    gas_strategy_json  JSONB        NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pending_txs_tx_hash
    ON pending_txs (tx_hash);

-- Single-row table tracking the resume cursor for the indexer.StreamEvents
-- consumer. id is constrained to 1 so the row is effectively a singleton.
CREATE TABLE IF NOT EXISTS stream_cursor (
    id                 INT          PRIMARY KEY CHECK (id = 1),
    last_acked_block   BIGINT       NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

INSERT INTO stream_cursor (id, last_acked_block)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;

-- Persisted per-asset heartbeat schedule. Updated via SetHeartbeat RPC; read
-- by the heartbeat scheduler on every tick so admin changes take effect on
-- the next tick.
CREATE TABLE IF NOT EXISTS heartbeat_schedules (
    asset_id        TEXT         PRIMARY KEY,
    interval_sec    INT          NOT NULL DEFAULT 0,
    deviation_bps   INT          NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

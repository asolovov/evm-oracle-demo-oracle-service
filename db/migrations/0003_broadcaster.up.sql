-- 0003_broadcaster.up.sql — multi-wallet broadcaster rotation (task 06.3).
--
-- The submitter now broadcasts from a pool of reporter EOAs with per-wallet
-- nonce management + failover, instead of always reporter[0]. Record which
-- wallet sent each tx so (a) replace-by-fee re-broadcasts from the same
-- wallet+nonce after a restart, and (b) operators can see per-wallet activity.
--
-- 0x-prefixed lowercase hex address; empty until first broadcast.
ALTER TABLE oracle_submissions ADD COLUMN IF NOT EXISTS broadcaster TEXT NOT NULL DEFAULT '';
ALTER TABLE pending_txs        ADD COLUMN IF NOT EXISTS broadcaster TEXT NOT NULL DEFAULT '';

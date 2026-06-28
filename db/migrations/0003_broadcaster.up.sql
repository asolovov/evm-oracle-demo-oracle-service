-- 0003_broadcaster.up.sql — multi-wallet broadcaster rotation (task 06.3).
--
-- The submitter now broadcasts from a pool of reporter EOAs with per-wallet
-- nonce management + failover, instead of always reporter[0]. Record which
-- wallet sent each tx so operators can attribute per-wallet activity.
--
-- NOTE: currently WRITE-ONLY. In-process replace-by-fee already re-broadcasts
-- from the same wallet+nonce (the watcher closure carries the per-wallet auth).
-- This column is the persisted form for a FUTURE restart-reconciliation path —
-- restart recovery of in-flight `pending` rows is the documented v1 gap (see
-- submitter.recover), so nothing reads the column back yet.
--
-- 0x-prefixed lowercase hex address; empty until first broadcast.
ALTER TABLE oracle_submissions ADD COLUMN IF NOT EXISTS broadcaster TEXT NOT NULL DEFAULT '';
ALTER TABLE pending_txs        ADD COLUMN IF NOT EXISTS broadcaster TEXT NOT NULL DEFAULT '';

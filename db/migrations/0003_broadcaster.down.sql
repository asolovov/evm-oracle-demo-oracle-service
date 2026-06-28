-- 0003_broadcaster.down.sql
ALTER TABLE pending_txs        DROP COLUMN IF EXISTS broadcaster;
ALTER TABLE oracle_submissions DROP COLUMN IF EXISTS broadcaster;

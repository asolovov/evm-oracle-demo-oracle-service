-- 0001_init.down.sql — drop oracle-service v1 schema.
DROP TABLE IF EXISTS heartbeat_schedules;
DROP TABLE IF EXISTS stream_cursor;
DROP TABLE IF EXISTS pending_txs;
DROP TABLE IF EXISTS oracle_submissions;

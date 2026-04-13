-- V001: initial control-plane schema.
--
-- Migrations manage long-lived tables whose shape is stable across
-- deployments: the schema_migrations tracker and the partitions
-- metadata table. They do NOT manage the daily log partition tables
-- (logs_YYYY_MM_DD, fts_logs_YYYY_MM_DD) — those are created lazily by
-- the writer on first insert into a new day, using a CREATE TABLE
-- template defined in Go code.
--
-- Rationale: log partitions are append-only and dropped wholesale on
-- retention. Schema changes to the partition shape only affect future
-- partitions; old ones remain as-is until retention removes them.

CREATE TABLE partitions (
    day            TEXT    PRIMARY KEY,          -- 'YYYY-MM-DD' in UTC
    row_count      INTEGER NOT NULL DEFAULT 0,
    byte_size      INTEGER NOT NULL DEFAULT 0,
    created_at     INTEGER NOT NULL,             -- unix seconds
    last_ingest_at INTEGER NOT NULL              -- unix seconds
);

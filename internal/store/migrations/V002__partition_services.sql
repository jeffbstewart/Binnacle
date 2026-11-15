-- V002: per-day service index.
--
-- The viewer's service dropdown used to run SELECT DISTINCT service
-- across every log partition, a full scan that grows with retained
-- volume. partition_services records each (day, service) pair once;
-- the writer adds a row the first time it sees a pair, and
-- DropPartition removes a day's rows with the partition.
--
-- Partitions that existed before this migration have
-- services_indexed = 0 and are backfilled from their log table by the
-- background maintenance loop (store.BackfillServiceIndex). Partitions
-- created afterwards start at 1 because the writer indexes every
-- record it stores.

CREATE TABLE partition_services (
    day     TEXT NOT NULL,                       -- 'YYYY-MM-DD' in UTC
    service TEXT NOT NULL,
    PRIMARY KEY (day, service)
) WITHOUT ROWID;

CREATE INDEX partition_services_by_service ON partition_services (service);

ALTER TABLE partitions ADD COLUMN services_indexed INTEGER NOT NULL DEFAULT 0;

package store

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// PartitionName returns the SQLite table name for the partition
// covering the given UTC day. Underscores (not hyphens) because they
// make the identifier usable without quoting.
func PartitionName(day time.Time) string {
	return "logs_" + day.UTC().Format("2006_01_02")
}

// ftsPartitionName returns the FTS5 shadow-table name for the given
// day's partition.
func ftsPartitionName(day time.Time) string {
	return "fts_logs_" + day.UTC().Format("2006_01_02")
}

// dayKey returns the canonical 'YYYY-MM-DD' string used as the primary
// key in the partitions metadata table.
func dayKey(day time.Time) string {
	return day.UTC().Format("2006-01-02")
}

// partitionDDL is the multi-statement CREATE block that spins up a
// new daily partition. It covers:
//
//   - the partition table itself
//   - the (service, severity, time_ns DESC) index for the query layer
//   - the trace-id index for correlation lookups
//   - an FTS5 virtual table shadowing `message` and `service`
//   - insert/delete triggers keeping FTS5 in sync with the base table
//
// All statements are IF NOT EXISTS so calling this on an already-
// initialized day is a cheap no-op. The writer caches per-day so the
// DDL usually runs once per day per process lifetime.
//
// Substitutions:
//
//	{{TABLE}} → logs_YYYY_MM_DD
//	{{FTS}}   → fts_logs_YYYY_MM_DD
const partitionDDL = `
CREATE TABLE IF NOT EXISTS {{TABLE}} (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    time_ns        INTEGER NOT NULL,
    ingest_ns      INTEGER NOT NULL,
    severity       INTEGER NOT NULL,
    service        TEXT    NOT NULL,
    instance       TEXT    NOT NULL,
    version        TEXT,
    logger         TEXT,
    trace_id       BLOB,
    span_id        BLOB,
    message        TEXT    NOT NULL,
    attrs_json     TEXT,
    exception_json TEXT
);

CREATE INDEX IF NOT EXISTS ix_{{TABLE}}_svc_sev_ts
    ON {{TABLE}} (service, severity, time_ns DESC);

CREATE INDEX IF NOT EXISTS ix_{{TABLE}}_trace
    ON {{TABLE}} (trace_id) WHERE trace_id IS NOT NULL;

CREATE VIRTUAL TABLE IF NOT EXISTS {{FTS}} USING fts5(
    message,
    service UNINDEXED,
    content='{{TABLE}}',
    content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS {{TABLE}}_ai
AFTER INSERT ON {{TABLE}} BEGIN
    INSERT INTO {{FTS}}(rowid, message, service)
    VALUES (new.id, new.message, new.service);
END;

CREATE TRIGGER IF NOT EXISTS {{TABLE}}_ad
AFTER DELETE ON {{TABLE}} BEGIN
    INSERT INTO {{FTS}}({{FTS}}, rowid, message, service)
    VALUES ('delete', old.id, old.message, old.service);
END;
`

// ensurePartition is idempotent: it runs CREATE TABLE IF NOT EXISTS
// (and friends) for the given UTC day and records the day in the
// partitions metadata table. Safe to call on every write path — the
// SQLite checks short-circuit quickly when everything is already in
// place.
func ensurePartition(db *sql.DB, day time.Time) error {
	name := PartitionName(day)
	fts := ftsPartitionName(day)
	ddl := strings.NewReplacer("{{TABLE}}", name, "{{FTS}}", fts).Replace(partitionDDL)
	if _, err := db.Exec(ddl); err != nil {
		return fmt.Errorf("create partition %s: %w", name, err)
	}
	now := time.Now().Unix()
	_, err := db.Exec(
		`INSERT INTO partitions (day, created_at, last_ingest_at) VALUES (?, ?, ?)
		 ON CONFLICT(day) DO NOTHING`,
		dayKey(day), now, now,
	)
	if err != nil {
		return fmt.Errorf("record partition metadata: %w", err)
	}
	return nil
}

// ListPartitionDays returns every known partition day as its
// canonical YYYY-MM-DD string, in ascending order. Used by the query
// layer to decide which tables to UNION across, and by retention to
// know what's droppable.
func ListPartitionDays(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`SELECT day FROM partitions ORDER BY day`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var days []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		days = append(days, d)
	}
	return days, rows.Err()
}

// DropPartition removes a day's partition: the log table, its FTS5
// shadow, the indexes/triggers (dropped implicitly with the tables),
// and the partitions metadata row. All-or-nothing via a transaction.
// Idempotent — dropping a non-existent partition is a no-op.
func DropPartition(db *sql.DB, day string) error {
	t, err := time.Parse("2006-01-02", day)
	if err != nil {
		return fmt.Errorf("bad day %q: %w", day, err)
	}
	name := PartitionName(t)
	fts := ftsPartitionName(t)

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DROP TABLE IF EXISTS ` + fts); err != nil {
		return fmt.Errorf("drop fts %s: %w", fts, err)
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS ` + name); err != nil {
		return fmt.Errorf("drop table %s: %w", name, err)
	}
	if _, err := tx.Exec(`DELETE FROM partitions WHERE day = ?`, day); err != nil {
		return fmt.Errorf("delete metadata %s: %w", day, err)
	}
	return tx.Commit()
}

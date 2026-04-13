package store

import (
	"testing"
	"time"
)

func TestPartitionName(t *testing.T) {
	day := time.Date(2026, 4, 13, 23, 59, 59, 0, time.UTC)
	if got := PartitionName(day); got != "logs_2026_04_13" {
		t.Fatalf("PartitionName: got %q, want logs_2026_04_13", got)
	}
	if got := ftsPartitionName(day); got != "fts_logs_2026_04_13" {
		t.Fatalf("ftsPartitionName: got %q, want fts_logs_2026_04_13", got)
	}
	if got := dayKey(day); got != "2026-04-13" {
		t.Fatalf("dayKey: got %q, want 2026-04-13", got)
	}
}

func TestPartitionName_TimezoneAgnostic(t *testing.T) {
	// A client in New York reporting 2026-04-13 00:30 EDT should land
	// in the UTC 2026-04-13 04:30 partition — same day in UTC.
	et, _ := time.LoadLocation("America/New_York")
	clientTime := time.Date(2026, 4, 13, 0, 30, 0, 0, et)
	if got := PartitionName(clientTime); got != "logs_2026_04_13" {
		t.Fatalf("PartitionName across timezones: got %q, want logs_2026_04_13", got)
	}
	// A client reporting 2026-04-13 22:00 EDT (== 2026-04-14 02:00 UTC)
	// should land in the UTC 2026-04-14 partition.
	lateClientTime := time.Date(2026, 4, 13, 22, 0, 0, 0, et)
	if got := PartitionName(lateClientTime); got != "logs_2026_04_14" {
		t.Fatalf("PartitionName rolls to UTC day: got %q, want logs_2026_04_14", got)
	}
}

func TestEnsurePartition_CreatesTablesAndMetadata(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	day := time.Date(2026, 4, 13, 0, 0, 0, 0, time.UTC)
	if err := ensurePartition(db, day); err != nil {
		t.Fatalf("ensurePartition: %v", err)
	}

	// Main table exists and is empty.
	if _, err := db.Exec(`SELECT * FROM logs_2026_04_13 LIMIT 0`); err != nil {
		t.Fatalf("logs_2026_04_13 should exist: %v", err)
	}
	// FTS shadow exists.
	if _, err := db.Exec(`SELECT * FROM fts_logs_2026_04_13 LIMIT 0`); err != nil {
		t.Fatalf("fts_logs_2026_04_13 should exist: %v", err)
	}
	// Metadata row.
	var count int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM partitions WHERE day = ?`, "2026-04-13",
	).Scan(&count); err != nil {
		t.Fatalf("check partitions metadata: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 metadata row, got %d", count)
	}
}

func TestEnsurePartition_Idempotent(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	day := time.Date(2026, 4, 13, 0, 0, 0, 0, time.UTC)
	for range 3 {
		if err := ensurePartition(db, day); err != nil {
			t.Fatalf("ensurePartition should be idempotent: %v", err)
		}
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM partitions`).Scan(&count); err != nil {
		t.Fatalf("count metadata: %v", err)
	}
	if count != 1 {
		t.Fatalf("metadata count: got %d, want 1", count)
	}
}

func TestEnsurePartition_FTSStaysInSync(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	day := time.Date(2026, 4, 13, 0, 0, 0, 0, time.UTC)
	if err := ensurePartition(db, day); err != nil {
		t.Fatalf("ensurePartition: %v", err)
	}

	// Insert a base-table row; FTS5 trigger should shadow it.
	_, err = db.Exec(`INSERT INTO logs_2026_04_13
		(time_ns, ingest_ns, severity, service, instance, message)
		VALUES (?, ?, ?, ?, ?, ?)`,
		int64(1), int64(2), 17, "test-svc", "host-A", "transcode buddy disconnected")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	var ftsHits int
	err = db.QueryRow(
		`SELECT COUNT(*) FROM fts_logs_2026_04_13 WHERE fts_logs_2026_04_13 MATCH 'buddy'`,
	).Scan(&ftsHits)
	if err != nil {
		t.Fatalf("FTS query: %v", err)
	}
	if ftsHits != 1 {
		t.Fatalf("FTS trigger failed: got %d matches, want 1", ftsHits)
	}
}

func TestListPartitionDays_Ordered(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Create out of order to prove ORDER BY.
	for _, d := range []string{"2026-04-15", "2026-04-13", "2026-04-14"} {
		parsed, _ := time.Parse("2006-01-02", d)
		if err := ensurePartition(db, parsed); err != nil {
			t.Fatalf("ensurePartition %s: %v", d, err)
		}
	}

	days, err := ListPartitionDays(db)
	if err != nil {
		t.Fatalf("ListPartitionDays: %v", err)
	}
	want := []string{"2026-04-13", "2026-04-14", "2026-04-15"}
	if len(days) != len(want) {
		t.Fatalf("got %d days, want %d", len(days), len(want))
	}
	for i, d := range days {
		if d != want[i] {
			t.Errorf("days[%d] = %q, want %q", i, d, want[i])
		}
	}
}

func TestDropPartition_RemovesEverything(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	day := time.Date(2026, 4, 13, 0, 0, 0, 0, time.UTC)
	if err := ensurePartition(db, day); err != nil {
		t.Fatalf("ensurePartition: %v", err)
	}
	if err := DropPartition(db, "2026-04-13"); err != nil {
		t.Fatalf("DropPartition: %v", err)
	}

	// Both tables gone.
	if _, err := db.Exec(`SELECT * FROM logs_2026_04_13 LIMIT 0`); err == nil {
		t.Fatalf("logs_2026_04_13 should have been dropped")
	}
	if _, err := db.Exec(`SELECT * FROM fts_logs_2026_04_13 LIMIT 0`); err == nil {
		t.Fatalf("fts_logs_2026_04_13 should have been dropped")
	}
	// Metadata row gone.
	var count int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM partitions WHERE day = ?`, "2026-04-13",
	).Scan(&count); err != nil {
		t.Fatalf("check metadata: %v", err)
	}
	if count != 0 {
		t.Fatalf("metadata row should have been deleted, got %d", count)
	}
}

func TestDropPartition_Idempotent(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Drop a partition that never existed.
	if err := DropPartition(db, "2099-01-01"); err != nil {
		t.Fatalf("DropPartition of never-existed: %v", err)
	}
}

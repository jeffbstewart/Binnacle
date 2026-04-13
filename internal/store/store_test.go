package store

import (
	"os"
	"path/filepath"
	"testing"
)

// TestOpen_CreatesDirectoryAndRunsMigrations verifies the full startup
// contract: data dir is created, SQLite file is opened, and every
// pending migration is applied before Open returns.
func TestOpen_CreatesDirectoryAndRunsMigrations(t *testing.T) {
	// Nested directory that doesn't exist — Open must create it.
	dataDir := filepath.Join(t.TempDir(), "nested", "data")

	db, err := Open(dataDir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// The DB file exists on disk.
	dbPath := filepath.Join(dataDir, "binnacle.db")
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("expected %s to exist: %v", dbPath, err)
	}

	// schema_migrations has at least V001 recorded.
	var appliedCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&appliedCount); err != nil {
		t.Fatalf("count schema_migrations: %v", err)
	}
	if appliedCount < 1 {
		t.Fatalf("expected at least one applied migration, got %d", appliedCount)
	}

	// The partitions control-plane table created by V001 exists.
	if _, err := db.Exec(`SELECT * FROM partitions LIMIT 0`); err != nil {
		t.Fatalf("partitions table should exist after Open: %v", err)
	}
}

// TestOpen_Idempotent verifies that opening the same data directory a
// second time doesn't re-apply migrations. The recovery path after a
// planned restart depends on this.
func TestOpen_Idempotent(t *testing.T) {
	dataDir := t.TempDir()

	db1, err := Open(dataDir)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	var firstCount int
	if err := db1.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&firstCount); err != nil {
		t.Fatalf("count after first Open: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close first DB: %v", err)
	}

	db2, err := Open(dataDir)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })

	var secondCount int
	if err := db2.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&secondCount); err != nil {
		t.Fatalf("count after second Open: %v", err)
	}
	if firstCount != secondCount {
		t.Fatalf("migration re-applied across restart: first=%d second=%d", firstCount, secondCount)
	}
}

// TestOpen_WalMode verifies we end up in WAL journal mode. Readers
// blocking writers (or vice versa) would be a serious regression for a
// log-ingest workload.
func TestOpen_WalMode(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var mode string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Fatalf("expected journal_mode=wal, got %q", mode)
	}
}

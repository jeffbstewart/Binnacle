package store

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

// openTestDB returns a fresh in-memory SQLite handle. Each test gets
// its own clean DB — in-memory is cheap and isolation is worth the ns.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open in-memory sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMigrate_FreshDB_AppliesAll(t *testing.T) {
	db := openTestDB(t)

	if err := Migrate(db); err != nil {
		t.Fatalf("Migrate on fresh DB: %v", err)
	}

	// schema_migrations should have at least V001 recorded.
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("count schema_migrations: %v", err)
	}
	if count < 1 {
		t.Fatalf("expected >=1 recorded migrations, got %d", count)
	}

	// The V001 migration creates the partitions table.
	if _, err := db.Exec(`SELECT * FROM partitions LIMIT 0`); err != nil {
		t.Fatalf("partitions table should exist after V001: %v", err)
	}
}

func TestMigrate_Idempotent(t *testing.T) {
	db := openTestDB(t)

	if err := Migrate(db); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}

	var firstCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&firstCount); err != nil {
		t.Fatalf("count after first Migrate: %v", err)
	}

	if err := Migrate(db); err != nil {
		t.Fatalf("second Migrate should be a no-op: %v", err)
	}

	var secondCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&secondCount); err != nil {
		t.Fatalf("count after second Migrate: %v", err)
	}
	if firstCount != secondCount {
		t.Fatalf("migrations re-applied; expected %d rows, got %d", firstCount, secondCount)
	}
}

func TestMigrate_RecordsVersionAndDescription(t *testing.T) {
	db := openTestDB(t)

	if err := Migrate(db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	var version int
	var description string
	err := db.QueryRow(
		`SELECT version, description FROM schema_migrations ORDER BY version LIMIT 1`,
	).Scan(&version, &description)
	if err != nil {
		t.Fatalf("read first recorded migration: %v", err)
	}
	if version != 1 {
		t.Fatalf("first migration should be version 1, got %d", version)
	}
	if description == "" {
		t.Fatalf("description should be non-empty")
	}
}

func TestLoadMigrations_OrderedByVersion(t *testing.T) {
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	if len(migrations) == 0 {
		t.Fatalf("expected at least one embedded migration")
	}
	for i := 1; i < len(migrations); i++ {
		if migrations[i].Version <= migrations[i-1].Version {
			t.Errorf("migrations out of order at index %d: V%d then V%d",
				i, migrations[i-1].Version, migrations[i].Version)
		}
	}
}

func TestMigrate_TransactionalFailure(t *testing.T) {
	// Sanity: if a migration's SQL is invalid, Migrate returns an error
	// AND leaves schema_migrations in a consistent state. We can't easily
	// inject a bad file into the embed.FS at test time, so we drive the
	// lower-level helper directly.
	db := openTestDB(t)
	if err := ensureMigrationsTable(db); err != nil {
		t.Fatalf("ensureMigrationsTable: %v", err)
	}

	bad := Migration{
		Version:     999,
		Description: "deliberately broken",
		Filename:    "V999__broken.sql",
		SQL:         `THIS IS NOT SQL`,
	}
	if err := applyMigration(db, bad); err == nil {
		t.Fatalf("applyMigration should have failed on bad SQL")
	}

	// schema_migrations should NOT have a row for V999 (transaction rolled back).
	var count int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM schema_migrations WHERE version = 999`,
	).Scan(&count); err != nil {
		t.Fatalf("count V999: %v", err)
	}
	if count != 0 {
		t.Fatalf("failed migration recorded a row; transaction did not roll back")
	}
}

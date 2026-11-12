// Package store owns Binnacle's SQLite persistence: schema migrations,
// the log writer, and the query layer. This file implements a
// Flyway-style migration framework.
//
// Conventions:
//   - Migration files live in migrations/ and follow the pattern
//     V{NNN}__{description}.sql where NNN is a zero-padded integer.
//   - Files are embedded into the binary via go:embed so there's no
//     filesystem dependency at runtime.
//   - Applied migrations are tracked in the schema_migrations table.
//     Each migration runs inside a transaction; a failure aborts the
//     whole migration and leaves the prior state intact.
//   - Migrations are immutable once applied. If you need to change an
//     already-applied migration, add a new one that does the work.
package store

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// A Migration is one versioned schema change loaded from the embedded
// migrations directory.
type Migration struct {
	Version     int    // parsed from the filename prefix (V001 → 1)
	Description string // parsed from the part after "__"
	Filename    string // e.g. V001__initial_schema.sql
	SQL         string // file contents, applied verbatim in a transaction
}

// migrationFilenameRegex matches V001__initial_schema.sql and captures
// the version number and description.
var migrationFilenameRegex = regexp.MustCompile(`^V(\d+)__(.+)\.sql$`)

// Migrate applies every pending migration to db in version order. It
// creates the schema_migrations table on first run and skips migrations
// already recorded there. Safe to call repeatedly — no-op when
// everything is current.
func Migrate(db *sql.DB) error {
	if err := ensureMigrationsTable(db); err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}

	applied, err := appliedVersions(db)
	if err != nil {
		return fmt.Errorf("read applied versions: %w", err)
	}

	migrations, err := loadMigrations()
	if err != nil {
		return fmt.Errorf("load embedded migrations: %w", err)
	}

	for _, m := range migrations {
		if _, done := applied[m.Version]; done {
			continue
		}
		if err := applyMigration(db, m); err != nil {
			return fmt.Errorf("apply V%03d (%s): %w", m.Version, m.Description, err)
		}
		slog.Info("applied migration", "version", m.Version, "description", m.Description)
	}
	return nil
}

func ensureMigrationsTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     INTEGER PRIMARY KEY,
			description TEXT    NOT NULL,
			applied_at  INTEGER NOT NULL
		)
	`)
	return err
}

func appliedVersions(db *sql.DB) (map[int]struct{}, error) {
	rows, err := db.Query(`SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[int]struct{})
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = struct{}{}
	}
	return applied, rows.Err()
}

// loadMigrations reads every V*.sql file from the embedded migrations/
// directory and returns them sorted ascending by version. Duplicate
// version numbers are a programming error and return an error.
func loadMigrations() ([]Migration, error) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return nil, err
	}

	var out []Migration
	seen := make(map[int]string)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		match := migrationFilenameRegex.FindStringSubmatch(name)
		if match == nil {
			return nil, fmt.Errorf("migration filename %q does not match V{N}__{desc}.sql", name)
		}
		version, err := strconv.Atoi(match[1])
		if err != nil {
			return nil, fmt.Errorf("migration %q: version is not an integer: %w", name, err)
		}
		if prior, ok := seen[version]; ok {
			return nil, fmt.Errorf("duplicate migration version %d: %s and %s", version, prior, name)
		}
		seen[version] = name

		content, err := fs.ReadFile(migrationsFS, "migrations/"+name)
		if err != nil {
			return nil, err
		}
		out = append(out, Migration{
			Version:     version,
			Description: strings.ReplaceAll(match[2], "_", " "),
			Filename:    name,
			SQL:         string(content),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// applyMigration runs a single migration inside a transaction and
// records it in schema_migrations. If the migration's SQL or the
// tracker update fails, the transaction is rolled back and the DB
// returns to its pre-migration state.
func applyMigration(db *sql.DB, m Migration) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	// Rollback is a no-op after a successful Commit.
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(m.SQL); err != nil {
		return fmt.Errorf("execute migration SQL: %w", err)
	}
	if _, err := tx.Exec(
		`INSERT INTO schema_migrations (version, description, applied_at) VALUES (?, ?, ?)`,
		m.Version, m.Description, time.Now().Unix(),
	); err != nil {
		return fmt.Errorf("record migration: %w", err)
	}
	return tx.Commit()
}

// ErrNoMigrations is returned by AppliedMigrations when the tracking
// table doesn't exist yet. Callers generally don't need to distinguish
// this from "the table exists but has zero rows"; exposed for tests.
var ErrNoMigrations = errors.New("schema_migrations table does not exist")

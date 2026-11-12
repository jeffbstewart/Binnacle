package store

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	// Registers the "sqlite" driver for database/sql. Pure Go (no cgo),
	// so the distroless base image doesn't need glibc.
	_ "modernc.org/sqlite"
)

// Open creates (or opens) the Binnacle SQLite database at
// {dataDir}/binnacle.db, configures it for concurrent log writes and
// query reads, and applies any pending schema migrations.
//
// The dataDir is created if missing. The caller owns the returned DB
// and is responsible for Close() at shutdown.
func Open(dataDir string) (*sql.DB, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", dataDir, err)
	}

	dbPath := filepath.Join(dataDir, "binnacle.db")

	// SQLite pragma details:
	//   _pragma=journal_mode(WAL)    — writers don't block readers.
	//   _pragma=synchronous(NORMAL)  — acceptable durability for logs;
	//                                  at most the last few records lost
	//                                  on kernel panic. FULL would sync
	//                                  on every insert, hurting ingest.
	//   _pragma=foreign_keys(ON)     — catch referential mistakes early
	//                                  (no FKs today, but cheap to enable).
	//   _pragma=busy_timeout(5000)   — wait up to 5s on a locked table
	//                                  rather than erroring immediately.
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)", dbPath)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite at %s: %w", dbPath, err)
	}

	// Single connection at the database/sql layer: SQLite serializes
	// writes anyway, and a pool would only add lock contention. The
	// writer goroutine holds this connection; readers use separate DB
	// handles opened in read-only mode (added in a later phase).
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite at %s: %w", dbPath, err)
	}

	if err := Migrate(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply migrations: %w", err)
	}

	slog.Info("store ready", "path", dbPath)
	return db, nil
}

package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// VacuumTimeout bounds one VACUUM. VACUUM rewrites the whole database
// while holding the store's single connection, so ingest and reads
// wait (and time out) for its duration; the budget is generous because
// an interrupted VACUUM rolls back and the time is wasted.
const VacuumTimeout = 30 * time.Minute

// Thresholds for the maintenance loop's automatic VACUUM. Normal
// operation frees about one partition in (retention+1) per day and new
// partitions reuse those pages, so the free fraction stays well under
// VacuumFreeFraction; crossing it means a large one-off release such
// as the first retention sweep after an upgrade. VacuumMinFreeBytes
// keeps small stores from being rewritten to reclaim trivia.
const (
	VacuumFreeFraction = 0.25
	VacuumMinFreeBytes = 64 << 20
)

// DBSpace describes the database file's page usage.
type DBSpace struct {
	PageSize  int64
	Pages     int64 // total pages in the file
	FreePages int64 // pages on the freelist (allocated but unused)
}

// Bytes is the database file size implied by the page count.
func (s DBSpace) Bytes() int64 { return s.PageSize * s.Pages }

// FreeBytes is the space VACUUM could reclaim.
func (s DBSpace) FreeBytes() int64 { return s.PageSize * s.FreePages }

// FreeFraction is FreePages/Pages, or 0 for an empty file.
func (s DBSpace) FreeFraction() float64 {
	if s.Pages == 0 {
		return 0
	}
	return float64(s.FreePages) / float64(s.Pages)
}

// ReadDBSpace reads page_size, page_count and freelist_count.
func ReadDBSpace(ctx context.Context, db *sql.DB) (DBSpace, error) {
	var s DBSpace
	for _, p := range []struct {
		pragma string
		dst    *int64
	}{
		{"page_size", &s.PageSize},
		{"page_count", &s.Pages},
		{"freelist_count", &s.FreePages},
	} {
		if err := db.QueryRowContext(ctx, "PRAGMA "+p.pragma).Scan(p.dst); err != nil {
			return s, fmt.Errorf("PRAGMA %s: %w", p.pragma, err)
		}
	}
	return s, nil
}

// VacuumIfFragmented runs VACUUM when at least minFraction of the file
// and at least minFreeBytes are free pages, then truncates the WAL
// (which VACUUM grows to roughly the database's size). It reports
// whether VACUUM ran and the space before and after.
func VacuumIfFragmented(ctx context.Context, db *sql.DB, minFraction float64, minFreeBytes int64) (ran bool, before, after DBSpace, err error) {
	before, err = ReadDBSpace(ctx, db)
	if err != nil {
		return false, before, before, err
	}
	if before.FreeFraction() < minFraction || before.FreeBytes() < minFreeBytes {
		return false, before, before, nil
	}
	if _, err := db.ExecContext(ctx, "VACUUM"); err != nil {
		return false, before, before, fmt.Errorf("VACUUM: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return true, before, before, fmt.Errorf("checkpoint after VACUUM: %w", err)
	}
	after, err = ReadDBSpace(ctx, db)
	return true, before, after, err
}

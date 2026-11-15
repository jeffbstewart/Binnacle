package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// DropTimeout bounds dropping one partition (log table, FTS shadow,
// metadata rows). Each drop is its own transaction so the writer can
// interleave inserts between drops instead of waiting out a whole
// sweep.
const DropTimeout = time.Minute

// BackfillTimeout bounds indexing one pre-V002 partition's services.
const BackfillTimeout = time.Minute

// ExpiredPartitionDays returns the days from days whose entire UTC day
// ends at or before now-retention: every record such a partition can
// hold is older than the retention window. Unparseable entries are
// skipped. Input order is preserved.
func ExpiredPartitionDays(days []string, retention time.Duration, now time.Time) []string {
	cutoff := now.Add(-retention)
	var out []string
	for _, d := range days {
		t, err := time.Parse("2006-01-02", d)
		if err != nil {
			continue
		}
		if !t.Add(24 * time.Hour).After(cutoff) {
			out = append(out, d)
		}
	}
	return out
}

// EnforceRetention drops every partition that ExpiredPartitionDays
// selects and returns the days actually dropped. A failed drop is
// recorded and the sweep continues; the joined errors are returned.
func EnforceRetention(ctx context.Context, db *sql.DB, retention time.Duration, now time.Time) ([]string, error) {
	days, err := ListPartitionDays(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("list partitions: %w", err)
	}
	var dropped []string
	var errs []error
	for _, day := range ExpiredPartitionDays(days, retention, now) {
		if ctx.Err() != nil {
			errs = append(errs, ctx.Err())
			break
		}
		dctx, cancel := context.WithTimeout(ctx, DropTimeout)
		err := DropPartition(dctx, db, day)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("drop %s: %w", day, err))
			continue
		}
		dropped = append(dropped, day)
	}
	return dropped, errors.Join(errs...)
}

// BackfillServiceIndex fills partition_services for partitions created
// before the index existed (services_indexed = 0), one partition per
// transaction, and returns how many it indexed.
func BackfillServiceIndex(ctx context.Context, db *sql.DB) (int, error) {
	days, err := unindexedDays(ctx, db)
	if err != nil {
		return 0, fmt.Errorf("list unindexed partitions: %w", err)
	}
	var n int
	var errs []error
	for _, day := range days {
		if ctx.Err() != nil {
			errs = append(errs, ctx.Err())
			break
		}
		bctx, cancel := context.WithTimeout(ctx, BackfillTimeout)
		err := backfillDay(bctx, db, day)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("backfill %s: %w", day, err))
			continue
		}
		n++
	}
	return n, errors.Join(errs...)
}

func unindexedDays(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT day FROM partitions WHERE services_indexed = 0 ORDER BY day`)
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

func backfillDay(ctx context.Context, db *sql.DB, day string) error {
	t, err := time.Parse("2006-01-02", day)
	if err != nil {
		return fmt.Errorf("bad day %q: %w", day, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Table name is derived from a parsed date, never from user input.
	if _, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO partition_services (day, service)
		 SELECT DISTINCT ?, service FROM `+PartitionName(t), day); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE partitions SET services_indexed = 1 WHERE day = ?`, day); err != nil {
		return err
	}
	return tx.Commit()
}

// Maintain runs a maintenance pass immediately and then every interval
// until ctx is cancelled. A pass enforces retention first, so the
// one-time service-index backfill after an upgrade only scans
// partitions that are being kept. Outcomes are logged; nothing is
// returned because maintenance must never stop the service.
func Maintain(ctx context.Context, db *sql.DB, retention, interval time.Duration) {
	for {
		maintainOnce(ctx, db, retention)
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

func maintainOnce(ctx context.Context, db *sql.DB, retention time.Duration) {
	start := time.Now()
	dropped, err := EnforceRetention(ctx, db, retention, start)
	attrs := []any{"dropped", len(dropped), "retention", retention.String(),
		"elapsed", time.Since(start).String()}
	if len(dropped) > 0 {
		attrs = append(attrs, "oldest", dropped[0], "newest", dropped[len(dropped)-1])
	}
	if err != nil {
		slog.Error("retention sweep had errors", append(attrs, "error", err)...)
	} else {
		// Logged even when nothing was dropped: one line an hour is
		// the stderr proof that retention is running.
		slog.Info("retention sweep complete", attrs...)
	}

	start = time.Now()
	n, err := BackfillServiceIndex(ctx, db)
	if err != nil {
		slog.Error("service index backfill had errors", "indexed", n,
			"elapsed", time.Since(start).String(), "error", err)
	} else if n > 0 {
		slog.Info("service index backfilled", "partitions", n,
			"elapsed", time.Since(start).String())
	}

	maybeVacuum(ctx, db)
}

// maybeVacuum runs VacuumIfFragmented with the package thresholds and
// logs the outcome. Runs after retention so the space it reclaims
// includes that pass's drops.
func maybeVacuum(ctx context.Context, db *sql.DB) {
	vctx, cancel := context.WithTimeout(ctx, VacuumTimeout)
	defer cancel()
	start := time.Now()
	ran, before, after, err := VacuumIfFragmented(vctx, db, VacuumFreeFraction, VacuumMinFreeBytes)
	switch {
	case err != nil:
		slog.Error("vacuum failed", "ran", ran, "size_bytes", before.Bytes(),
			"free_bytes", before.FreeBytes(), "elapsed", time.Since(start).String(), "error", err)
	case ran:
		slog.Info("vacuum complete", "size_before_bytes", before.Bytes(),
			"size_after_bytes", after.Bytes(), "reclaimed_bytes", before.Bytes()-after.Bytes(),
			"elapsed", time.Since(start).String())
	}
}

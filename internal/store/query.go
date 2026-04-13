package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// QueryFilter bundles the Phase 1 query parameters. All fields are
// optional; zero-value means "no filter on that dimension."
type QueryFilter struct {
	Services    []string  // exact-match OR filter; empty = any service
	MinSeverity Severity  // severity >= this; 0 = no filter
	Since       time.Time // time_ns >= Since.UnixNano(); zero = open-start
	Until       time.Time // time_ns <  Until.UnixNano(); zero = open-end
	Limit       int       // default 100; hard-capped at 1000
}

// QueriedRecord is the on-the-wire shape returned by Query. Distinct
// from the internal Record so the user-facing API can present
// friendlier types: time.Time for RFC3339 JSON, TraceID/SpanID for
// hex strings. Fields use omitempty so unset optionals don't clutter
// the response.
type QueriedRecord struct {
	Time      time.Time      `json:"time"`
	Ingest    time.Time      `json:"ingest"`
	Severity  Severity       `json:"severity"`
	Service   string         `json:"service"`
	Instance  string         `json:"instance,omitempty"`
	Version   string         `json:"version,omitempty"`
	Logger    string         `json:"logger,omitempty"`
	TraceID   TraceID        `json:"trace_id,omitempty"`
	SpanID    SpanID         `json:"span_id,omitempty"`
	Message   string         `json:"message"`
	Attrs     map[string]any `json:"attrs,omitempty"`
	Exception *Exception     `json:"exception,omitempty"`
}

// QueryResult is the top-level response shape. Records are ordered
// time DESC (newest first).
type QueryResult struct {
	Records       []QueriedRecord `json:"records"`
	CountReturned int             `json:"count_returned"`
}

// Query reads records matching f from every partition whose day
// overlaps f's time range. Implementation is a SQL UNION ALL across
// per-day partition tables. At 7-day retention this tops out at 7
// sub-queries, each hitting the (service, severity, time_ns DESC)
// index.
func Query(db *sql.DB, f QueryFilter) (*QueryResult, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	days, err := ListPartitionDays(db)
	if err != nil {
		return nil, fmt.Errorf("list partitions: %w", err)
	}
	days = filterDaysByRange(days, f.Since, f.Until)
	if len(days) == 0 {
		return &QueryResult{Records: []QueriedRecord{}}, nil
	}

	where, args := buildWhereClause(f)

	// The read-side column list is derived from the same columnSpec
	// source of truth as the write side, so they cannot drift.
	readCols := recordColumnList

	// One sub-query per day, each capped at `limit` — we can't need
	// more than `limit` from any single partition after the outer
	// ORDER BY. The outer ORDER BY then picks the top `limit`.
	//
	// Each sub-query is wrapped in parentheses: SQLite's compound-
	// SELECT grammar forbids ORDER BY inside a simple SELECT that's
	// part of a UNION ALL, but allows it inside a parenthesised
	// sub-query.
	var parts []string
	multiArgs := make([]any, 0, len(args)*len(days))
	for _, day := range days {
		t, err := time.Parse("2006-01-02", day)
		if err != nil {
			continue
		}
		parts = append(parts, fmt.Sprintf(
			`SELECT * FROM (SELECT %s FROM %s %s ORDER BY time_ns DESC LIMIT %d)`,
			readCols, PartitionName(t), where, limit,
		))
		multiArgs = append(multiArgs, args...)
	}

	fullSQL := strings.Join(parts, " UNION ALL ") +
		fmt.Sprintf(" ORDER BY time_ns DESC LIMIT %d", limit)

	rows, err := db.Query(fullSQL, multiArgs...)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	records := make([]QueriedRecord, 0, limit)
	for rows.Next() {
		r, err := scanQueriedRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &QueryResult{
		Records:       records,
		CountReturned: len(records),
	}, nil
}

// buildWhereClause produces the per-partition WHERE clause and its
// positional args. Returns "" + nil when there are no filters.
func buildWhereClause(f QueryFilter) (string, []any) {
	var clauses []string
	var args []any

	if len(f.Services) > 0 {
		ph := strings.TrimRight(strings.Repeat("?,", len(f.Services)), ",")
		clauses = append(clauses, "service IN ("+ph+")")
		for _, s := range f.Services {
			args = append(args, s)
		}
	}
	if f.MinSeverity > 0 {
		clauses = append(clauses, "severity >= ?")
		args = append(args, int32(f.MinSeverity))
	}
	if !f.Since.IsZero() {
		clauses = append(clauses, "time_ns >= ?")
		args = append(args, f.Since.UnixNano())
	}
	if !f.Until.IsZero() {
		clauses = append(clauses, "time_ns < ?")
		args = append(args, f.Until.UnixNano())
	}
	if len(clauses) == 0 {
		return "", nil
	}
	return "WHERE " + strings.Join(clauses, " AND "), args
}

// filterDaysByRange drops partition days entirely outside [since,
// until). Partition resolution is 1 day, so this is conservative at
// the edges — the per-partition WHERE clause tightens to nanosecond
// precision.
func filterDaysByRange(days []string, since, until time.Time) []string {
	if since.IsZero() && until.IsZero() {
		return days
	}
	var out []string
	for _, d := range days {
		t, err := time.Parse("2006-01-02", d)
		if err != nil {
			continue
		}
		dayStart := t
		dayEnd := t.Add(24 * time.Hour)
		if !since.IsZero() && dayEnd.Before(since) {
			continue
		}
		if !until.IsZero() && !dayStart.Before(until) {
			continue
		}
		out = append(out, d)
	}
	return out
}

// scanQueriedRecord reads one row into a QueriedRecord. The column
// order matches recordColumnList (which is generated from the same
// columnSpec source of truth the writer uses), so read and write are
// automatically in sync.
//
// TraceID / SpanID implement sql.Scanner, so database/sql delegates
// to them for NULL/empty/length validation — no byte-level
// conversion noise at this level.
func scanQueriedRecord(rows *sql.Rows) (QueriedRecord, error) {
	var (
		r        QueriedRecord
		timeNs   int64
		ingestNs int64
		severity int32

		// Optional TEXT columns come back as NullString so we can
		// distinguish "" from SQL NULL. The writer maps zero-value
		// fields to NULL, so queries reverse it.
		instanceNull, versionNull, loggerNull sql.NullString
		attrsJSON, exceptionJSON              sql.NullString
	)

	if err := rows.Scan(
		&timeNs, &ingestNs, &severity, &r.Service,
		&instanceNull, &versionNull, &loggerNull,
		&r.TraceID, &r.SpanID,
		&r.Message, &attrsJSON, &exceptionJSON,
	); err != nil {
		return r, err
	}

	r.Time = time.Unix(0, timeNs).UTC()
	r.Ingest = time.Unix(0, ingestNs).UTC()
	r.Severity = Severity(severity)
	r.Instance = instanceNull.String
	r.Version = versionNull.String
	r.Logger = loggerNull.String

	if attrsJSON.Valid && attrsJSON.String != "" {
		if err := json.Unmarshal([]byte(attrsJSON.String), &r.Attrs); err != nil {
			return r, fmt.Errorf("unmarshal attrs_json: %w", err)
		}
	}
	if exceptionJSON.Valid && exceptionJSON.String != "" {
		var e Exception
		if err := json.Unmarshal([]byte(exceptionJSON.String), &e); err != nil {
			return r, fmt.Errorf("unmarshal exception_json: %w", err)
		}
		r.Exception = &e
	}
	return r, nil
}

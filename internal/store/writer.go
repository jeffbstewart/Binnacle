package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// MaxRecordAge caps how far in the past a log record's client
// timestamp can be, relative to ingest time. Records older than this
// are rejected at Submit and never touch the database.
//
// Rationale:
//
//   - Retention drops whole partitions on a schedule similar to this
//     window. Accepting a 3-day-old record when retention is 1 day
//     would have us CREATE the partition table only for the
//     retention sweep to DROP it again minutes later.
//   - A client with a wildly wrong clock (Roku reverting to 1970,
//     a phone on cellular after a DST bug) would otherwise scatter
//     records across phantom historical partitions.
//   - Attribution: under Phase 1's single-shared-key model, an
//     attacker with the key could retroactively insert records into
//     yesterday's partition to pollute a post-incident analysis.
//     This check makes retroactive dating impossible beyond 24 h.
const MaxRecordAge = 24 * time.Hour

// Writer serializes log-record writes onto a single SQLite connection.
// Everything upstream (OTLP HTTP handler, gRPC handler) submits
// records via Submit; one background goroutine drains the queue.
//
// Design choices:
//
//   - **Single writer goroutine.** SQLite serializes writes anyway;
//     one goroutine + one connection avoids lock contention and gives
//     us an easy path to batched transactions in Phase 2.
//   - **Bounded queue with drop-on-full backpressure.** Submit is
//     non-blocking; if the writer falls behind, we return
//     ErrQueueFull rather than stalling the HTTP listener.
//   - **Per-day partition cache.** ensurePartition's CREATE IF NOT
//     EXISTS is idempotent but still a DB round-trip; the cache
//     makes the hot path exactly one INSERT per record.
//   - **Age check at ingress.** Records older than MaxRecordAge never
//     enter the queue, so the write path never has to reason about
//     partitions that may be about to be dropped.
type Writer struct {
	db    *sql.DB
	queue chan Record

	mu      sync.Mutex
	ensured map[string]struct{} // day key → known

	done chan struct{}

	submitted uint64
	dropped   uint64
	tooOld    uint64
	written   uint64
}

// NewWriter constructs a Writer bound to db with a queue of queueSize
// records (default 1024). Call Run once in a goroutine to start draining.
func NewWriter(db *sql.DB, queueSize int) *Writer {
	if queueSize <= 0 {
		queueSize = 1024
	}
	return &Writer{
		db:      db,
		queue:   make(chan Record, queueSize),
		ensured: make(map[string]struct{}),
		done:    make(chan struct{}),
	}
}

// ErrQueueFull is returned by Submit when the bounded queue is
// saturated. The caller should drop the record (after counting it).
var ErrQueueFull = errors.New("writer queue full")

// ErrRecordTooOld is returned by Submit when the record's client
// timestamp is more than MaxRecordAge in the past. Typically
// indicates a client clock problem, not a protocol error, but we
// reject rather than silently corrupting historical partitions.
var ErrRecordTooOld = errors.New("record timestamp older than MaxRecordAge")

// Submit enqueues r for background writing. Non-blocking. Returns:
//
//   - ErrRecordTooOld  — r.TimeNs is more than MaxRecordAge before
//     wall-clock now. Caller should drop the record; a warning log
//     about clock skew is appropriate.
//   - ErrQueueFull     — writer is backed up. Caller should drop.
//   - nil              — record is queued for eventual write.
func (w *Writer) Submit(r Record) error {
	w.submitted++

	if time.Since(time.Unix(0, r.TimeNs)) > MaxRecordAge {
		w.tooOld++
		return ErrRecordTooOld
	}

	select {
	case w.queue <- r:
		return nil
	default:
		w.dropped++
		return ErrQueueFull
	}
}

// Run drains the queue until ctx is cancelled. On cancel, Run
// finishes writing records already enqueued, then closes Done().
func (w *Writer) Run(ctx context.Context) {
	defer close(w.done)
	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case r := <-w.queue:
					w.writeOne(r)
				default:
					return
				}
			}
		case r := <-w.queue:
			w.writeOne(r)
		}
	}
}

// Done returns a channel closed after Run has fully exited.
func (w *Writer) Done() <-chan struct{} { return w.done }

// WriterStats is a point-in-time snapshot of writer counters.
type WriterStats struct {
	Submitted uint64
	Dropped   uint64 // queue was full
	TooOld    uint64 // client timestamp older than MaxRecordAge
	Written   uint64
	QueueLen  int
}

// Stats returns the current counter snapshot.
func (w *Writer) Stats() WriterStats {
	return WriterStats{
		Submitted: w.submitted,
		Dropped:   w.dropped,
		TooOld:    w.tooOld,
		Written:   w.written,
		QueueLen:  len(w.queue),
	}
}

// --- column schema: single source of truth ---
//
// A persistable column is described by its DB name and a function
// that extracts its value from a Record. Keeping both in one slice
// means the INSERT statement text and the argument list are
// generated from the same source — they cannot drift. Add a column:
// one entry here + a partition DDL change. Remove one: delete an
// entry + schema change.
type columnSpec struct {
	name    string
	extract func(r Record) any
}

var recordColumns = []columnSpec{
	{"time_ns", func(r Record) any { return r.TimeNs }},
	{"ingest_ns", func(r Record) any { return r.IngestNs }},
	{"severity", func(r Record) any { return int32(r.Severity) }},
	{"service", func(r Record) any { return r.Service }},
	{"instance", func(r Record) any { return r.Instance }},
	{"version", func(r Record) any { return nullableString(r.Version) }},
	{"logger", func(r Record) any { return nullableString(r.Logger) }},
	{"trace_id", func(r Record) any { return nullableBytes(r.TraceID) }},
	{"span_id", func(r Record) any { return nullableBytes(r.SpanID) }},
	{"message", func(r Record) any { return r.Message }},
	{"attrs_json", func(r Record) any {
		return nullableString(mustMarshalAttrs(r.Attrs))
	}},
	{"exception_json", func(r Record) any {
		return nullableString(mustMarshalException(r.Exception))
	}},
}

// Pre-compute the column-list and placeholder fragments once.
var (
	recordColumnList  = buildColumnList(recordColumns)
	recordPlaceholder = buildPlaceholders(len(recordColumns))
)

func buildColumnList(specs []columnSpec) string {
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = s.name
	}
	return strings.Join(names, ", ")
}

func buildPlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}

// writeOne inserts a single record. Errors are logged and counted but
// do not propagate — losing one record on failure is preferable to
// killing the writer goroutine.
func (w *Writer) writeOne(r Record) {
	// A record belongs to the partition for its CLIENT timestamp,
	// not ingest time. Late-arriving logs (within MaxRecordAge)
	// land in the right bucket.
	t := time.Unix(0, r.TimeNs)
	if err := w.ensureDay(t); err != nil {
		slog.Error("ensure partition failed",
			"day", dayKey(t), "error", err)
		return
	}

	args := make([]any, len(recordColumns))
	for i, c := range recordColumns {
		args[i] = c.extract(r)
	}

	// Partition name is derived in code from r.TimeNs, never from a
	// user-provided string — no identifier injection concern.
	stmt := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)`,
		PartitionName(t), recordColumnList, recordPlaceholder,
	)
	if _, err := w.db.Exec(stmt, args...); err != nil {
		slog.Error("insert log record failed",
			"service", r.Service, "error", err)
		return
	}

	// Best-effort freshness update; errors here don't fail the write.
	_, _ = w.db.Exec(
		`UPDATE partitions SET last_ingest_at = ?, row_count = row_count + 1 WHERE day = ?`,
		time.Now().Unix(), dayKey(t),
	)

	w.written++
}

// ensureDay short-circuits on the cache hit, only calling
// ensurePartition for never-before-seen days.
func (w *Writer) ensureDay(t time.Time) error {
	key := dayKey(t)
	w.mu.Lock()
	_, seen := w.ensured[key]
	w.mu.Unlock()
	if seen {
		return nil
	}
	if err := ensurePartition(w.db, t); err != nil {
		return err
	}
	w.mu.Lock()
	w.ensured[key] = struct{}{}
	w.mu.Unlock()
	return nil
}

// --- small helpers ---

// nullableString turns the empty string into SQL NULL.
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullableBytes turns empty slices into SQL NULL.
func nullableBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

// mustMarshalAttrs serializes the attribute map to compact JSON, or
// returns "" on nil/empty or on marshal failure (which is logged —
// a pathological attribute value should lose its attrs, not kill the
// whole record).
func mustMarshalAttrs(m map[string]any) string {
	if len(m) == 0 {
		return ""
	}
	b, err := json.Marshal(m)
	if err != nil {
		slog.Error("marshal attrs failed", "error", err)
		return ""
	}
	return string(b)
}

func mustMarshalException(e *Exception) string {
	if e == nil {
		return ""
	}
	b, err := json.Marshal(e)
	if err != nil {
		slog.Error("marshal exception failed", "error", err)
		return ""
	}
	return string(b)
}

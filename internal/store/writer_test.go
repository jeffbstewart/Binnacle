package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// drainWriter runs w.Run in a goroutine and tears it down cleanly
// when the test ends.
func drainWriter(t *testing.T, w *Writer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)
	t.Cleanup(func() {
		cancel()
		<-w.Done()
	})
}

// waitForWrite spins until Stats().Written >= want, bounded by d.
func waitForWrite(t *testing.T, w *Writer, want uint64, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if w.Stats().Written >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("writer never wrote %d records; stats=%+v", want, w.Stats())
}

// --- column-schema invariant ---

func TestColumnSchema_StaysBalanced(t *testing.T) {
	// The whole point of the columnSpec abstraction is that SQL text
	// and argument count are generated from the same source and
	// cannot drift. Lock that in.
	placeholders := 0
	for _, ch := range recordPlaceholder {
		if ch == '?' {
			placeholders++
		}
	}
	if placeholders != len(recordColumns) {
		t.Fatalf("placeholder/column mismatch: %d placeholders, %d columns",
			placeholders, len(recordColumns))
	}

	// columnList is comma-joined: n names → n-1 commas.
	commas := 0
	for _, ch := range recordColumnList {
		if ch == ',' {
			commas++
		}
	}
	if commas != len(recordColumns)-1 {
		t.Fatalf("column-list/column mismatch: %d commas, want %d",
			commas, len(recordColumns)-1)
	}
}

// --- happy path ---

func TestWriter_SubmitAndWrite(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	w := NewWriter(db, 16)
	drainWriter(t, w)

	now := time.Now()
	rec := Record{
		TimeNs:   now.UnixNano(),
		IngestNs: now.UnixNano() + 100,
		Severity: SeverityError,
		Service:  "mediamanager-server",
		Instance: "nas-01",
		Message:  "transcode buddy disconnected",
	}
	if err := w.Submit(rec); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitForWrite(t, w, 1, 2*time.Second)

	var count int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM `+PartitionName(now)+` WHERE service = ?`,
		"mediamanager-server",
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("partition row count: got %d, want 1", count)
	}
}

// TestWriter_PersistsEveryColumnExtractor is the real guard against
// positional-arg drift: if an extractor points at the wrong Record
// field, the round-trip assertion fails.
func TestWriter_PersistsEveryColumnExtractor(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	w := NewWriter(db, 16)
	drainWriter(t, w)

	now := time.Now()
	in := Record{
		TimeNs:   now.UnixNano(),
		IngestNs: now.UnixNano() + 100,
		Severity: SeverityWarn,
		Service:  "transcode-buddy",
		Instance: "desktop-4090",
		Version:  "202604131806",
		Logger:   "net.stewart.transcodebuddy.Worker",
		TraceID: []byte{
			0xaa, 0xbb, 0xcc, 0xdd, 0x11, 0x22, 0x33, 0x44,
			0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22,
		},
		SpanID:  []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		Message: "ffmpeg exited non-zero",
		Attrs: map[string]any{
			"http.method":      "POST",
			"http.status_code": float64(500),
		},
		Exception: &Exception{
			Type:       "RuntimeException",
			Message:    "boom",
			StackTrace: "…",
		},
	}
	if err := w.Submit(in); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitForWrite(t, w, 1, 2*time.Second)

	var (
		timeNs, ingestNs                      int64
		severity                              int32
		service, instance, message            string
		version, logger, attrsJSON, exception any
		traceID, spanID                       []byte
	)
	err = db.QueryRow(
		`SELECT time_ns, ingest_ns, severity, service, instance, version, logger,
		        trace_id, span_id, message, attrs_json, exception_json
		 FROM `+PartitionName(now)+` LIMIT 1`,
	).Scan(&timeNs, &ingestNs, &severity, &service, &instance,
		&version, &logger, &traceID, &spanID, &message, &attrsJSON, &exception)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	if timeNs != in.TimeNs {
		t.Errorf("time_ns: got %d, want %d", timeNs, in.TimeNs)
	}
	if ingestNs != in.IngestNs {
		t.Errorf("ingest_ns: got %d, want %d", ingestNs, in.IngestNs)
	}
	if severity != int32(in.Severity) {
		t.Errorf("severity: got %d, want %d", severity, in.Severity)
	}
	if service != in.Service {
		t.Errorf("service: got %q, want %q", service, in.Service)
	}
	if instance != in.Instance {
		t.Errorf("instance: got %q, want %q", instance, in.Instance)
	}
	if message != in.Message {
		t.Errorf("message: got %q, want %q", message, in.Message)
	}
	if s, _ := version.(string); s != in.Version {
		t.Errorf("version: got %v, want %q", version, in.Version)
	}
	if s, _ := logger.(string); s != in.Logger {
		t.Errorf("logger: got %v, want %q", logger, in.Logger)
	}
	if len(traceID) != 16 {
		t.Errorf("trace_id length: got %d, want 16", len(traceID))
	}
	if len(spanID) != 8 {
		t.Errorf("span_id length: got %d, want 8", len(spanID))
	}
	if s, _ := attrsJSON.(string); s == "" {
		t.Errorf("attrs_json should be non-empty, got %v", attrsJSON)
	}
	if s, _ := exception.(string); s == "" {
		t.Errorf("exception_json should be non-empty, got %v", exception)
	}
}

func TestWriter_NullableFieldsBecomeNull(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	w := NewWriter(db, 16)
	drainWriter(t, w)

	now := time.Now()
	if err := w.Submit(Record{
		TimeNs:   now.UnixNano(),
		IngestNs: now.UnixNano(),
		Severity: SeverityInfo,
		Service:  "x",
		Instance: "h",
		Message:  "m",
		// Version, Logger, TraceID, SpanID, Attrs, Exception all zero-value
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitForWrite(t, w, 1, 2*time.Second)

	var nullVersion, nullLogger, nullTrace, nullSpan, nullAttrs, nullException int
	err = db.QueryRow(
		`SELECT version IS NULL, logger IS NULL, trace_id IS NULL, span_id IS NULL,
		        attrs_json IS NULL, exception_json IS NULL
		 FROM `+PartitionName(now)+` LIMIT 1`,
	).Scan(&nullVersion, &nullLogger, &nullTrace, &nullSpan, &nullAttrs, &nullException)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	for i, got := range []int{nullVersion, nullLogger, nullTrace, nullSpan, nullAttrs, nullException} {
		if got != 1 {
			t.Errorf("optional field %d expected NULL (1), got %d", i, got)
		}
	}
}

// --- MaxRecordAge enforcement ---

func TestWriter_RejectsRecordsOlderThanMaxAge(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	w := NewWriter(db, 16)
	drainWriter(t, w)

	// Timestamp 2 days ago — safely past the 24 h limit.
	old := time.Now().Add(-48 * time.Hour)
	err = w.Submit(Record{
		TimeNs:   old.UnixNano(),
		IngestNs: time.Now().UnixNano(),
		Severity: SeverityInfo,
		Service:  "x",
		Instance: "h",
		Message:  "late arrival",
	})
	if !errors.Is(err, ErrRecordTooOld) {
		t.Fatalf("Submit: expected ErrRecordTooOld, got %v", err)
	}

	// Counter was bumped.
	if got := w.Stats().TooOld; got != 1 {
		t.Errorf("TooOld counter: got %d, want 1", got)
	}
	// The partition for "2 days ago" must not exist — rejection must
	// happen before any DB side-effect.
	oldTable := PartitionName(old)
	if _, err := db.Exec(`SELECT * FROM ` + oldTable + ` LIMIT 0`); err == nil {
		t.Fatalf("%s should not exist; rejection must occur before partition creation", oldTable)
	}
}

func TestWriter_AcceptsRecordsRightAtMaxAge(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	w := NewWriter(db, 16)
	drainWriter(t, w)

	// Slightly newer than 24 h — should pass. Use a 1-minute buffer
	// to avoid flakiness from test-runtime clock drift.
	borderline := time.Now().Add(-(MaxRecordAge - time.Minute))
	err = w.Submit(Record{
		TimeNs:   borderline.UnixNano(),
		IngestNs: time.Now().UnixNano(),
		Severity: SeverityInfo,
		Service:  "x",
		Instance: "h",
		Message:  "just in time",
	})
	if err != nil {
		t.Fatalf("borderline record (< MaxRecordAge) should be accepted; got %v", err)
	}
	waitForWrite(t, w, 1, 2*time.Second)
	if w.Stats().TooOld != 0 {
		t.Fatalf("TooOld should be 0, got %d", w.Stats().TooOld)
	}
}

// --- metadata + multi-day partitions ---

func TestWriter_PartitionMetadataMaintained(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	w := NewWriter(db, 16)
	drainWriter(t, w)

	now := time.Now()
	for i := range 3 {
		if err := w.Submit(Record{
			TimeNs:   now.UnixNano() + int64(i),
			IngestNs: now.UnixNano(),
			Severity: SeverityInfo,
			Service:  "x",
			Instance: "h",
			Message:  "m",
		}); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}
	waitForWrite(t, w, 3, 2*time.Second)

	var rowCount int
	var lastIngestAt int64
	err = db.QueryRow(
		`SELECT row_count, last_ingest_at FROM partitions WHERE day = ?`,
		dayKey(now),
	).Scan(&rowCount, &lastIngestAt)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	if rowCount != 3 {
		t.Fatalf("row_count: got %d, want 3", rowCount)
	}
	if lastIngestAt == 0 {
		t.Fatalf("last_ingest_at should be updated; got 0")
	}
}

// --- backpressure ---

func TestWriter_QueueFullReturnsError(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Tiny queue, NO drainer — submissions accumulate and overflow.
	w := NewWriter(db, 2)

	now := time.Now()
	rec := Record{
		TimeNs: now.UnixNano(), IngestNs: now.UnixNano(),
		Severity: SeverityInfo, Service: "x", Instance: "h", Message: "m",
	}

	if err := w.Submit(rec); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	if err := w.Submit(rec); err != nil {
		t.Fatalf("submit 2: %v", err)
	}
	if err := w.Submit(rec); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("submit 3: expected ErrQueueFull, got %v", err)
	}
	if w.Stats().Dropped != 1 {
		t.Fatalf("Dropped counter: got %d, want 1", w.Stats().Dropped)
	}
}

// --- shutdown ---

func TestWriter_DrainsOnCancel(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	w := NewWriter(db, 16)
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)

	now := time.Now()
	for i := range 5 {
		if err := w.Submit(Record{
			TimeNs: now.UnixNano() + int64(i), IngestNs: now.UnixNano(),
			Severity: SeverityInfo, Service: "x", Instance: "h", Message: "m",
		}); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}
	cancel()

	select {
	case <-w.Done():
	case <-time.After(3 * time.Second):
		t.Fatalf("writer did not exit within 3s of cancel")
	}
	if w.Stats().Written != 5 {
		t.Fatalf("writer dropped records on shutdown; written=%d, want 5", w.Stats().Written)
	}
}

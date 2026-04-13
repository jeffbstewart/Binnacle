package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// helper: open a store with a running writer and return both.
func openWithWriter(t *testing.T) (*sql.DB, *Writer) {
	t.Helper()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	w := NewWriter(db, 64)
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)
	t.Cleanup(func() {
		cancel()
		<-w.Done()
	})
	return db, w
}

// helper: block until the writer has drained at least want records.
func mustWrite(t *testing.T, w *Writer, want uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if w.Stats().Written >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("writer never wrote %d records; stats=%+v", want, w.Stats())
}

// helper: submit a record, fail if Submit errors.
func submit(t *testing.T, w *Writer, r Record) {
	t.Helper()
	if err := w.Submit(r); err != nil {
		t.Fatalf("Submit: %v", err)
	}
}

func TestQuery_EmptyStoreReturnsEmpty(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	result, err := Query(db, QueryFilter{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if result.CountReturned != 0 {
		t.Errorf("count: got %d, want 0", result.CountReturned)
	}
	if len(result.Records) != 0 {
		t.Errorf("records: got %d, want 0", len(result.Records))
	}
}

func TestQuery_RoundTripsAllFields(t *testing.T) {
	db, w := openWithWriter(t)

	now := time.Now()
	tid, _ := NewTraceID([]byte{
		0xaa, 0xbb, 0xcc, 0xdd, 0x11, 0x22, 0x33, 0x44,
		0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22,
	})
	sid, _ := NewSpanID([]byte{1, 2, 3, 4, 5, 6, 7, 8})

	submit(t, w, Record{
		TimeNs:   now.UnixNano(),
		IngestNs: now.UnixNano() + 100,
		Severity: SeverityError,
		Service:  "mediamanager-server",
		Instance: "nas-01",
		Version:  "abc",
		Logger:   "net.stewart.x.Y",
		TraceID:  tid,
		SpanID:   sid,
		Message:  "transcode buddy disconnected",
		Attrs:    map[string]any{"http.method": "POST"},
		Exception: &Exception{
			Type: "RuntimeException", Message: "boom", StackTrace: "…",
		},
	})
	mustWrite(t, w, 1)

	result, err := Query(db, QueryFilter{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if result.CountReturned != 1 {
		t.Fatalf("count: got %d, want 1", result.CountReturned)
	}
	got := result.Records[0]
	if got.Severity != SeverityError {
		t.Errorf("severity: got %d, want %d", got.Severity, SeverityError)
	}
	if got.Service != "mediamanager-server" {
		t.Errorf("service: got %q", got.Service)
	}
	if got.Message != "transcode buddy disconnected" {
		t.Errorf("message: got %q", got.Message)
	}
	if got.TraceID.String() != tid.String() {
		t.Errorf("trace_id: got %q, want %q", got.TraceID.String(), tid.String())
	}
	if got.SpanID.String() != sid.String() {
		t.Errorf("span_id: got %q, want %q", got.SpanID.String(), sid.String())
	}
	if got.Attrs["http.method"] != "POST" {
		t.Errorf("attrs: got %v", got.Attrs)
	}
	if got.Exception == nil || got.Exception.Type != "RuntimeException" {
		t.Errorf("exception: got %+v", got.Exception)
	}
}

func TestQuery_OrdersByTimeDesc(t *testing.T) {
	db, w := openWithWriter(t)

	base := time.Now().Add(-1 * time.Hour)
	for i := range 3 {
		submit(t, w, Record{
			TimeNs:   base.Add(time.Duration(i) * time.Minute).UnixNano(),
			IngestNs: time.Now().UnixNano(),
			Severity: SeverityInfo,
			Service:  "x", Instance: "h",
			Message: "msg",
		})
	}
	mustWrite(t, w, 3)

	result, err := Query(db, QueryFilter{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Records) != 3 {
		t.Fatalf("records: got %d, want 3", len(result.Records))
	}
	for i := 1; i < len(result.Records); i++ {
		if result.Records[i].Time.After(result.Records[i-1].Time) {
			t.Errorf("records not in time-desc order: [%d]=%s then [%d]=%s",
				i-1, result.Records[i-1].Time, i, result.Records[i].Time)
		}
	}
}

func TestQuery_FilterByService(t *testing.T) {
	db, w := openWithWriter(t)

	now := time.Now()
	for _, svc := range []string{"a", "b", "c"} {
		submit(t, w, Record{
			TimeNs: now.UnixNano(), IngestNs: now.UnixNano(),
			Severity: SeverityInfo, Service: svc, Instance: "h",
			Message: "msg",
		})
	}
	mustWrite(t, w, 3)

	result, err := Query(db, QueryFilter{Services: []string{"a", "c"}})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if result.CountReturned != 2 {
		t.Fatalf("count: got %d, want 2", result.CountReturned)
	}
	seen := map[string]bool{}
	for _, r := range result.Records {
		seen[r.Service] = true
	}
	if !seen["a"] || !seen["c"] || seen["b"] {
		t.Errorf("services: got %v, want {a,c}", seen)
	}
}

func TestQuery_FilterByMinSeverity(t *testing.T) {
	db, w := openWithWriter(t)

	now := time.Now()
	for _, sev := range []Severity{SeverityDebug, SeverityInfo, SeverityWarn, SeverityError} {
		submit(t, w, Record{
			TimeNs: now.UnixNano(), IngestNs: now.UnixNano(),
			Severity: sev, Service: "x", Instance: "h",
			Message: sev.String(),
		})
	}
	mustWrite(t, w, 4)

	result, err := Query(db, QueryFilter{MinSeverity: SeverityWarn})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if result.CountReturned != 2 {
		t.Fatalf("count: got %d, want 2 (WARN + ERROR)", result.CountReturned)
	}
	for _, r := range result.Records {
		if r.Severity < SeverityWarn {
			t.Errorf("record passed severity filter with sev=%d", r.Severity)
		}
	}
}

func TestQuery_FilterByTimeRange(t *testing.T) {
	db, w := openWithWriter(t)

	base := time.Now().Add(-1 * time.Hour)
	for i := range 5 {
		submit(t, w, Record{
			TimeNs:   base.Add(time.Duration(i) * time.Minute).UnixNano(),
			IngestNs: time.Now().UnixNano(),
			Severity: SeverityInfo, Service: "x", Instance: "h",
			Message: "msg",
		})
	}
	mustWrite(t, w, 5)

	// Keep only records with time_ns in [base+1m, base+4m)
	result, err := Query(db, QueryFilter{
		Since: base.Add(1 * time.Minute),
		Until: base.Add(4 * time.Minute),
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if result.CountReturned != 3 {
		t.Fatalf("count: got %d, want 3", result.CountReturned)
	}
}

func TestQuery_LimitClampedToMax(t *testing.T) {
	db, w := openWithWriter(t)

	now := time.Now()
	for i := range 10 {
		submit(t, w, Record{
			TimeNs: now.UnixNano() + int64(i), IngestNs: now.UnixNano(),
			Severity: SeverityInfo, Service: "x", Instance: "h",
			Message: "msg",
		})
	}
	mustWrite(t, w, 10)

	// Under-bound: 0 becomes default.
	r, err := Query(db, QueryFilter{Limit: 0})
	if err != nil {
		t.Fatalf("Limit=0: %v", err)
	}
	if r.CountReturned != 10 {
		t.Errorf("Limit=0 (default): got %d, want 10", r.CountReturned)
	}
	// Explicit small limit.
	r, err = Query(db, QueryFilter{Limit: 5})
	if err != nil {
		t.Fatalf("Limit=5: %v", err)
	}
	if r.CountReturned != 5 {
		t.Errorf("Limit=5: got %d, want 5", r.CountReturned)
	}
	// Excess limit is silently clamped — no error.
	if _, err := Query(db, QueryFilter{Limit: 99999}); err != nil {
		t.Errorf("Limit=99999 should clamp silently, got error: %v", err)
	}
}

func TestQuery_CrossesMultiplePartitions(t *testing.T) {
	// Two records on different UTC days. Query must walk both
	// partition tables and merge their results.
	db, w := openWithWriter(t)

	// Pick two points that are near each other (both within
	// MaxRecordAge) but straddle UTC midnight. Using "a few hours
	// either side of midnight" is safe assuming the test doesn't run
	// at exactly midnight UTC.
	now := time.Now().UTC()
	// Find a midnight-ish pivot within the last day.
	pivot := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	before := pivot.Add(-30 * time.Minute) // previous UTC day
	after := pivot.Add(30 * time.Minute)   // current UTC day

	submit(t, w, Record{
		TimeNs: before.UnixNano(), IngestNs: time.Now().UnixNano(),
		Severity: SeverityInfo, Service: "x", Instance: "h",
		Message: "before",
	})
	submit(t, w, Record{
		TimeNs: after.UnixNano(), IngestNs: time.Now().UnixNano(),
		Severity: SeverityInfo, Service: "x", Instance: "h",
		Message: "after",
	})
	mustWrite(t, w, 2)

	days, _ := ListPartitionDays(db)
	if len(days) != 2 {
		t.Fatalf("expected 2 partitions, got %d: %v", len(days), days)
	}

	result, err := Query(db, QueryFilter{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if result.CountReturned != 2 {
		t.Fatalf("count: got %d, want 2", result.CountReturned)
	}
}

func TestQuery_JSONOutputShape(t *testing.T) {
	// Lock in the externally-visible shape of a response: timestamps
	// as RFC3339, severity as label string, trace/span as hex,
	// unset optionals omitted.
	db, w := openWithWriter(t)

	submit(t, w, Record{
		TimeNs:   time.Now().UnixNano(),
		IngestNs: time.Now().UnixNano(),
		Severity: SeverityWarn,
		Service:  "x", Instance: "h", Message: "m",
	})
	mustWrite(t, w, 1)

	result, err := Query(db, QueryFilter{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	b, err := json.Marshal(result.Records[0])
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	s := string(b)
	// Severity label surfaces in JSON.
	if !strings.Contains(s, `"severity":"WARN"`) {
		t.Errorf("missing severity label in JSON: %s", s)
	}
	// Unset optionals are absent.
	for _, needle := range []string{`"version"`, `"logger"`, `"trace_id"`, `"span_id"`, `"attrs"`, `"exception"`} {
		if strings.Contains(s, needle) {
			t.Errorf("expected %s to be omitted: %s", needle, s)
		}
	}
}

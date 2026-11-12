package ingest

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	collectorpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/jeffbstewart/Binnacle/internal/store"
)

// setupHandler builds a fully wired OTLPHandler over an in-memory
// SQLite store, with a running writer goroutine. Returns the handler
// and the DB so tests can probe persisted state.
func setupHandler(t *testing.T) (*OTLPHandler, *sql.DB) {
	t.Helper()
	db, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	w := store.NewWriter(db, 64)
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)
	t.Cleanup(func() {
		cancel()
		<-w.Done()
	})

	return &OTLPHandler{Writer: w, Converter: NewConverter()}, db
}

// makeBatch builds a minimal OTLP batch with one record on one
// service, stamped at `now` so MaxRecordAge doesn't reject it.
func makeBatch(service, message string, now time.Time) *collectorpb.ExportLogsServiceRequest {
	return &collectorpb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{{
					Key: "service.name",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{StringValue: service},
					},
				}},
			},
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					TimeUnixNano:   uint64(now.UnixNano()),
					SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
					Body: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{StringValue: message},
					},
				}},
			}},
		}},
	}
}

// waitFor spins until predicate returns true, bounded by d. Avoids
// race-prone time.Sleep against the async writer goroutine.
func waitFor(t *testing.T, d time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", d)
}

// --- happy paths: both encodings ---

func TestOTLPHandler_AcceptsProtobuf(t *testing.T) {
	h, db := setupHandler(t)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	batch := makeBatch("svc-proto", "hello", time.Now())
	body, err := proto.Marshal(batch)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-protobuf" {
		t.Errorf("response Content-Type: got %q, want application/x-protobuf", ct)
	}

	waitFor(t, 2*time.Second, func() bool {
		var n int
		_ = db.QueryRow(`SELECT COUNT(*) FROM partitions`).Scan(&n)
		return n == 1
	})
	if s := h.Stats(); s.RecordsAccepted != 1 || s.RecordsDropped != 0 {
		t.Errorf("handler stats: %+v", s)
	}
}

func TestOTLPHandler_AcceptsJSON(t *testing.T) {
	h, _ := setupHandler(t)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	batch := makeBatch("svc-json", "hello", time.Now())
	body, err := protojson.Marshal(batch)
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader(body))
	// Charset parameter exercises the stripCharset helper.
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("response Content-Type: got %q, want application/json", ct)
	}
	waitFor(t, 2*time.Second, func() bool {
		return h.Stats().RecordsAccepted == 1
	})
}

// --- rejection paths ---

func TestOTLPHandler_RejectsWrongMethod(t *testing.T) {
	h, _ := setupHandler(t)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d, want 405", resp.StatusCode)
	}
	if got := resp.Header.Get("Allow"); got != http.MethodPost {
		t.Errorf("Allow header: got %q, want POST", got)
	}
	if h.Stats().RejectedBadMethod != 1 {
		t.Errorf("RejectedBadMethod: got %d, want 1", h.Stats().RejectedBadMethod)
	}
}

func TestOTLPHandler_RejectsUnsupportedContentType(t *testing.T) {
	h, _ := setupHandler(t)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader("{}"))
	req.Header.Set("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status: got %d, want 415", resp.StatusCode)
	}
	if h.Stats().RejectedBadCT != 1 {
		t.Errorf("RejectedBadCT: got %d, want 1", h.Stats().RejectedBadCT)
	}
}

func TestOTLPHandler_RejectsGarbledBody(t *testing.T) {
	h, _ := setupHandler(t)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader("not a proto"))
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}
	if h.Stats().RejectedBadFormat != 1 {
		t.Errorf("RejectedBadFormat: got %d, want 1", h.Stats().RejectedBadFormat)
	}
}

func TestOTLPHandler_RejectsOversizeBody(t *testing.T) {
	h, _ := setupHandler(t)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	big := bytes.Repeat([]byte{0}, MaxBatchBytes+1)
	req, _ := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader(big))
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status: got %d, want 413", resp.StatusCode)
	}
	if h.Stats().RejectedOversize != 1 {
		t.Errorf("RejectedOversize: got %d, want 1", h.Stats().RejectedOversize)
	}
}

// --- partial-success accounting ---

func TestOTLPHandler_ReportsPartialSuccessOnQueueFull(t *testing.T) {
	// 1-slot queue with NO drainer so Submit saturates after the
	// first record. The handler must still return 200 with a
	// PartialSuccess body describing the drops.
	db, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	w := store.NewWriter(db, 1)
	// Deliberately not calling w.Run() — queue stays saturated.
	h := &OTLPHandler{Writer: w, Converter: NewConverter()}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	now := time.Now()
	batch := makeBatch("svc", "m", now)
	for range 4 {
		batch.ResourceLogs[0].ScopeLogs[0].LogRecords = append(
			batch.ResourceLogs[0].ScopeLogs[0].LogRecords,
			&logspb.LogRecord{
				TimeUnixNano:   uint64(now.UnixNano()),
				SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
				Body: &commonpb.AnyValue{
					Value: &commonpb.AnyValue_StringValue{StringValue: "m"},
				},
			},
		)
	}
	body, _ := proto.Marshal(batch)

	req, _ := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d — partial success is still a 200", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var otlpResp collectorpb.ExportLogsServiceResponse
	if err := proto.Unmarshal(respBody, &otlpResp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	ps := otlpResp.GetPartialSuccess()
	if ps == nil {
		t.Fatalf("expected PartialSuccess to be present")
	}
	if ps.GetRejectedLogRecords() != 4 {
		t.Errorf("RejectedLogRecords: got %d, want 4", ps.GetRejectedLogRecords())
	}

	if s := h.Stats(); s.RecordsAccepted != 1 || s.RecordsDropped != 4 {
		t.Errorf("handler stats: %+v (want accepted=1 dropped=4)", s)
	}
}

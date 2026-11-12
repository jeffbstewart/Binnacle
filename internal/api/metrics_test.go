package api

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jeffbstewart/Binnacle/internal/ingest"
	"github.com/jeffbstewart/Binnacle/internal/store"
)

// setupMetricsRig stands up a real SQLite store + writer + OTLP
// handler so the metrics endpoint reads live stats objects, not
// mocks. Tests that want non-zero values drive state through the
// real producers (Submit a record, etc.).
func setupMetricsRig(t *testing.T) (*MetricsHandler, *sql.DB, *store.Writer, *ingest.OTLPHandler) {
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

	otlp := &ingest.OTLPHandler{Writer: w, Converter: ingest.NewConverter()}
	h := &MetricsHandler{DB: db, Writer: w, Ingest: otlp, Version: "test-1.0"}
	return h, db, w, otlp
}

func TestMetricsHandler_RejectsNonGET(t *testing.T) {
	h, _, _, _ := setupMetricsRig(t)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Post(srv.URL, "text/plain", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status: got %d, want 405", resp.StatusCode)
	}
	if got := resp.Header.Get("Allow"); got != http.MethodGet {
		t.Errorf("Allow: got %q, want GET", got)
	}
}

func TestMetricsHandler_ContentType(t *testing.T) {
	h, _, _, _ := setupMetricsRig(t)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	// Prometheus is strict about version=0.0.4 for text format;
	// anything else will cause the scraper to fall back to different
	// parsing rules. Match exactly.
	if ct := resp.Header.Get("Content-Type"); ct != promContentType {
		t.Errorf("Content-Type: got %q, want %q", ct, promContentType)
	}
}

// TestMetricsHandler_EmitsAllExpectedMetrics guards against a silent
// regression where a new stats field is added to a producer but the
// metrics handler forgets to emit it. Every counter/gauge name the
// handler claims to emit must appear in the scrape body.
func TestMetricsHandler_EmitsAllExpectedMetrics(t *testing.T) {
	h, _, _, _ := setupMetricsRig(t)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	body := fetchMetrics(t, srv)

	wantMetrics := []string{
		"binnacle_writer_submitted_total",
		"binnacle_writer_written_total",
		"binnacle_writer_dropped_total",
		"binnacle_writer_too_old_total",
		"binnacle_writer_queue_length",
		"binnacle_ingest_records_accepted_total",
		"binnacle_ingest_records_dropped_total",
		"binnacle_ingest_rejected_bad_method_total",
		"binnacle_ingest_rejected_bad_content_type_total",
		"binnacle_ingest_rejected_bad_format_total",
		"binnacle_ingest_rejected_oversize_total",
		"binnacle_converter_dropped_trace_ids_total",
		"binnacle_converter_dropped_span_ids_total",
		"binnacle_partitions",
		"binnacle_build_info",
	}
	for _, m := range wantMetrics {
		// Each metric must have a HELP line, a TYPE line, and at
		// least one value line — a Prometheus parser needs all three.
		for _, kind := range []string{"# HELP " + m, "# TYPE " + m, "\n" + m} {
			if !strings.Contains("\n"+body, kind) {
				t.Errorf("metric %s: missing %q in body", m, kind)
			}
		}
	}
}

// TestMetricsHandler_TextFormatParses sanity-checks the output shape.
// This isn't a full Prometheus parser, but it catches the common
// breakage modes: a stray line without HELP/TYPE, a value line that
// isn't "name <number>", a # comment that's neither HELP nor TYPE.
func TestMetricsHandler_TextFormatParses(t *testing.T) {
	h, _, _, _ := setupMetricsRig(t)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	body := fetchMetrics(t, srv)

	// Value lines: name{optional labels} integer-or-float.
	// We only emit integers, so use a conservative integer regexp.
	valueLine := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*(\{[^}]*\})? -?\d+$`)
	commentLine := regexp.MustCompile(`^# (HELP|TYPE) [a-zA-Z_][a-zA-Z0-9_]* `)

	for i, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		switch {
		case strings.HasPrefix(line, "#"):
			if !commentLine.MatchString(line) {
				t.Errorf("line %d: malformed comment %q", i, line)
			}
		case line == "":
			// Blank line between metrics is allowed; we don't emit
			// any, but tolerate if that changes.
		default:
			if !valueLine.MatchString(line) {
				t.Errorf("line %d: malformed value %q", i, line)
			}
		}
	}
}

// TestMetricsHandler_ReflectsLiveStats verifies that counters tick
// forward after real activity. Drive the writer through Submit (the
// public API) rather than poking internal fields, so the test would
// still pass if we swapped the underlying counter implementation.
func TestMetricsHandler_ReflectsLiveStats(t *testing.T) {
	h, _, w, _ := setupMetricsRig(t)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	// Baseline: all counters at zero.
	body := fetchMetrics(t, srv)
	assertMetricValue(t, body, "binnacle_writer_submitted_total", "0")

	// Submit 3 valid records; each bumps submitted_total, and the
	// async writer will eventually tick written_total.
	now := time.Now().UnixNano()
	for i := 0; i < 3; i++ {
		if err := w.Submit(store.Record{
			TimeNs:   now + int64(i),
			IngestNs: now,
			Severity: store.SeverityInfo,
			Service:  "metrics-test",
			Instance: "h",
			Message:  "m",
		}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	waitForStat(t, 2*time.Second, func() bool { return w.Stats().Written >= 3 })

	body = fetchMetrics(t, srv)
	assertMetricValue(t, body, "binnacle_writer_submitted_total", "3")
	assertMetricValue(t, body, "binnacle_writer_written_total", "3")
	// A successful write creates exactly one partition; the gauge
	// should reflect that.
	assertMetricValue(t, body, "binnacle_partitions", "1")
	// build_info always emits 1 with the version label.
	if !strings.Contains(body, `binnacle_build_info{version="test-1.0"} 1`) {
		t.Errorf("build_info missing or wrong label: body=\n%s", body)
	}
}

// --- test helpers ---

func fetchMetrics(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(body)
}

// assertMetricValue finds the value line for name in the scrape body
// and compares it to want. Uses regexp so we tolerate label variants
// for future-proofing.
func assertMetricValue(t *testing.T, body, name, want string) {
	t.Helper()
	// Match: "<name> <value>\n" with optional labels between.
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + `(\{[^}]*\})? (.+)$`)
	m := re.FindStringSubmatch(body)
	if m == nil {
		t.Errorf("metric %s: not found in body:\n%s", name, body)
		return
	}
	if m[2] != want {
		t.Errorf("metric %s: got value %q, want %q", name, m[2], want)
	}
}

// waitForStat spins until pred returns true or the deadline fires.
// Fatals on timeout so the test failure points at the condition
// rather than a downstream assertion.
func waitForStat(t *testing.T, d time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("stat predicate not true within %v", d)
}

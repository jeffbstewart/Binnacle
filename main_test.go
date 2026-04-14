package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	collectorpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/jeffbstewart/Binnacle/internal/api"
	"github.com/jeffbstewart/Binnacle/internal/ingest"
	"github.com/jeffbstewart/Binnacle/internal/store"
)

// TestHealthHandler verifies the shape and content of the /api/logs/health
// response. Both humans and Docker's HEALTHCHECK depend on this being
// JSON with a "status" field.
func TestHealthHandler(t *testing.T) {
	// Build the same mux main() builds so the route matching is exercised too.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/logs/health", healthHandler)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/api/logs/health")
	if err != nil {
		t.Fatalf("GET /api/logs/health: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %q", ct)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status=ok, got %q", body["status"])
	}
	if _, hasVersion := body["version"]; !hasVersion {
		t.Fatalf("response missing 'version' field: %v", body)
	}
}

// TestRunHealthcheck_Ok verifies that a 200 OK response from the
// target endpoint produces exit code 0.
func TestRunHealthcheck_Ok(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(healthHandler))
	t.Cleanup(srv.Close)

	port := testServerPort(t, srv)
	if code := runHealthcheck(port); code != 0 {
		t.Fatalf("healthy server: expected exit 0, got %d", code)
	}
}

// TestRunHealthcheck_Non200 verifies that a non-2xx response produces
// exit code 1.
func TestRunHealthcheck_Non200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unhealthy", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	port := testServerPort(t, srv)
	if code := runHealthcheck(port); code != 1 {
		t.Fatalf("unhealthy server: expected exit 1, got %d", code)
	}
}

// TestRunHealthcheck_Unreachable verifies that a connection failure
// (nothing listening on the port) produces exit code 1.
func TestRunHealthcheck_Unreachable(t *testing.T) {
	// Port 1 is the well-known tcpmux port — reliably closed on test
	// machines. If by some accident something is listening there, the
	// test might false-pass, but the test assertion is that we exit
	// nonzero, which is what a closed port produces.
	if code := runHealthcheck(1); code != 1 {
		t.Fatalf("unreachable server: expected exit 1, got %d", code)
	}
}

// testServerPort extracts the numeric port from an httptest.Server URL.
// runHealthcheck takes a port (not a URL) because it's the Dockerfile
// HEALTHCHECK's interface.
func testServerPort(t *testing.T, srv *httptest.Server) int {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("parse test server port: %v", err)
	}
	return port
}

// --- Phase 1 end-to-end integration tests ---
//
// Everything below stands up the full Phase 1 stack in memory (real
// SQLite store with migrations, running writer goroutine, OTLP HTTP
// handler behind the shared-key middleware, query API) via the exact
// mux builders main() uses. A regression anywhere in the POST-to-GET
// pipeline — auth wiring, route method gates, OTLP conversion,
// partition creation, query filter parsing, or JSON round-trip —
// fails one of these tests.

// e2eRig is the wired-together Phase 1 stack.
type e2eRig struct {
	db        *sql.DB
	writer    *store.Writer
	ingestURL string // base URL for the ingest server (mounts OTLP /v1/logs)
	queryURL  string // base URL for the query server (/api/logs/{health,query})
	apiKey    string
}

// newE2ERig assembles the rig and registers cleanup for every moving
// part: two httptest servers, the writer goroutine's context, and the
// DB handle.
func newE2ERig(t *testing.T, apiKey string) *e2eRig {
	t.Helper()

	db, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	writer := store.NewWriter(db, 64)
	writerCtx, stopWriter := context.WithCancel(context.Background())
	go writer.Run(writerCtx)
	t.Cleanup(func() {
		stopWriter()
		<-writer.Done()
	})

	// Reuse the real mux builders: port=0 is fine because httptest
	// supplies its own listener and only the Handler field is read.
	otlp := &ingest.OTLPHandler{Writer: writer, Converter: ingest.NewConverter()}
	query := &api.QueryHandler{DB: db}
	metrics := &api.MetricsHandler{DB: db, Writer: writer, Ingest: otlp, Version: "e2e-test"}
	ingestSrv := httptest.NewServer(buildIngestServer(0, apiKey, otlp).Handler)
	querySrv := httptest.NewServer(buildQueryServer(0, query, metrics).Handler)
	t.Cleanup(ingestSrv.Close)
	t.Cleanup(querySrv.Close)

	return &e2eRig{
		db:        db,
		writer:    writer,
		ingestURL: ingestSrv.URL,
		queryURL:  querySrv.URL,
		apiKey:    apiKey,
	}
}

// postBatch marshals batch as OTLP protobuf and POSTs it. If apiKey
// is empty, the header is omitted (to exercise the auth rejection).
func postBatch(t *testing.T, rig *e2eRig, batch *collectorpb.ExportLogsServiceRequest, apiKey string) *http.Response {
	t.Helper()
	body, err := proto.Marshal(batch)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, rig.ingestURL+ingest.OTLPPath, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	if apiKey != "" {
		req.Header.Set(ingest.APIKeyHeader, apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// fetchQuery hits the query server and decodes the body. Fatals on
// HTTP error.
func fetchQuery(t *testing.T, rig *e2eRig, params url.Values) *store.QueryResult {
	t.Helper()
	resp, err := http.Get(rig.queryURL + api.QueryPath + "?" + params.Encode())
	if err != nil {
		t.Fatalf("query GET: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("query status %d: %s", resp.StatusCode, body)
	}
	var result store.QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return &result
}

// waitForWrites spins until the writer has persisted at least n
// records or the deadline expires. Avoids races against the async
// writer without slowing the common case.
func waitForWrites(t *testing.T, w *store.Writer, n uint64, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if w.Stats().Written >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("writer did not persist %d records within %v (stats=%+v)", n, d, w.Stats())
}

// makeOTLPBatch builds a minimal but fully-populated OTLP batch: one
// resource carrying service.name/version/instance, one log record
// with timestamp, severity, message, and a single attribute so the
// attrs_json path gets exercised too.
func makeOTLPBatch(serviceName, instance, msg string, sev logspb.SeverityNumber, at time.Time) *collectorpb.ExportLogsServiceRequest {
	strAttr := func(k, v string) *commonpb.KeyValue {
		return &commonpb.KeyValue{
			Key: k,
			Value: &commonpb.AnyValue{
				Value: &commonpb.AnyValue_StringValue{StringValue: v},
			},
		}
	}
	return &collectorpb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					strAttr("service.name", serviceName),
					strAttr("service.version", "1.2.3"),
					strAttr("service.instance.id", instance),
				},
			},
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					TimeUnixNano:   uint64(at.UnixNano()),
					SeverityNumber: sev,
					Body: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{StringValue: msg},
					},
					Attributes: []*commonpb.KeyValue{strAttr("http.route", "/api/ping")},
				}},
			}},
		}},
	}
}

// TestEndToEnd_OTLPIngestToQuery is the Phase 1 acceptance test: a
// single log record POSTed via OTLP/HTTP must reappear in the query
// API's JSON response with every field round-tripped correctly.
func TestEndToEnd_OTLPIngestToQuery(t *testing.T) {
	rig := newE2ERig(t, "test-key")

	// Truncate to seconds so the time comparison is stable under JSON
	// marshaling rounding and timezone rendering.
	now := time.Now().UTC().Truncate(time.Second)
	batch := makeOTLPBatch("svc-e2e", "host-1", "hello from e2e",
		logspb.SeverityNumber_SEVERITY_NUMBER_WARN, now)

	resp := postBatch(t, rig, batch, "test-key")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ingest POST: got %d, want 200", resp.StatusCode)
	}

	waitForWrites(t, rig.writer, 1, 2*time.Second)

	params := url.Values{}
	params.Set("service", "svc-e2e")
	result := fetchQuery(t, rig, params)

	if result.CountReturned != 1 {
		t.Fatalf("CountReturned: got %d, want 1", result.CountReturned)
	}
	if len(result.Records) != 1 {
		t.Fatalf("Records len: got %d, want 1", len(result.Records))
	}
	r := result.Records[0]

	if r.Service != "svc-e2e" {
		t.Errorf("service: got %q, want %q", r.Service, "svc-e2e")
	}
	if r.Instance != "host-1" {
		t.Errorf("instance: got %q, want %q", r.Instance, "host-1")
	}
	if r.Version != "1.2.3" {
		t.Errorf("version: got %q, want %q", r.Version, "1.2.3")
	}
	if r.Message != "hello from e2e" {
		t.Errorf("message: got %q", r.Message)
	}
	if r.Severity != store.SeverityWarn {
		t.Errorf("severity: got %d, want %d (WARN)", r.Severity, store.SeverityWarn)
	}
	if got := r.Time.UTC().Truncate(time.Second); !got.Equal(now) {
		t.Errorf("time: got %v, want %v", got, now)
	}
	if got := r.Attrs["http.route"]; got != "/api/ping" {
		t.Errorf("attrs[http.route]: got %v, want /api/ping", got)
	}
}

// TestEndToEnd_IngestRejectsMissingAPIKey proves the auth middleware
// is actually wired in front of the OTLP handler by
// buildIngestServer. auth_test.go covers the middleware in
// isolation; this covers the wiring.
func TestEndToEnd_IngestRejectsMissingAPIKey(t *testing.T) {
	rig := newE2ERig(t, "test-key")

	batch := makeOTLPBatch("svc-denied", "host-1", "should not land",
		logspb.SeverityNumber_SEVERITY_NUMBER_INFO, time.Now())

	// Empty apiKey → no X-Logging-Api-Key header sent.
	resp := postBatch(t, rig, batch, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want 401", resp.StatusCode)
	}

	// Writer must not have seen the record: the middleware short-
	// circuited before the OTLP handler's Submit loop ran.
	if s := rig.writer.Stats(); s.Submitted != 0 || s.Written != 0 {
		t.Errorf("writer stats after rejected request: %+v (want zeros)", s)
	}
}

// TestEndToEnd_QueryRejectsNonGET proves buildQueryServer's method
// gate is wired. The query handler itself has its own 405 test; this
// covers the mux registration.
func TestEndToEnd_QueryRejectsNonGET(t *testing.T) {
	rig := newE2ERig(t, "test-key")

	resp, err := http.Post(rig.queryURL+api.QueryPath, "text/plain", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status: got %d, want 405", resp.StatusCode)
	}
}

// TestEndToEnd_HealthEndpoint proves buildQueryServer mounts the
// /api/logs/health route used by the Docker HEALTHCHECK.
func TestEndToEnd_HealthEndpoint(t *testing.T) {
	rig := newE2ERig(t, "test-key")

	resp, err := http.Get(rig.queryURL + "/api/logs/health")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status field: got %q, want ok", body["status"])
	}
}

// TestEndToEnd_MetricsReflectsIngest proves that after an OTLP POST
// succeeds, the Prometheus scrape shows the accept counter ticked.
// Covers two wiring risks: (1) buildQueryServer mounts /metrics,
// (2) MetricsHandler sees the same OTLPHandler stats object as the
// live ingest path.
func TestEndToEnd_MetricsReflectsIngest(t *testing.T) {
	rig := newE2ERig(t, "test-key")

	// POST one record and wait for it to land.
	batch := makeOTLPBatch("svc-metrics", "h-1", "via metrics test",
		logspb.SeverityNumber_SEVERITY_NUMBER_INFO, time.Now())
	resp := postBatch(t, rig, batch, "test-key")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ingest: got %d, want 200", resp.StatusCode)
	}
	waitForWrites(t, rig.writer, 1, 2*time.Second)

	metricsResp, err := http.Get(rig.queryURL + api.MetricsPath)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	t.Cleanup(func() { _ = metricsResp.Body.Close() })
	if metricsResp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status: got %d, want 200", metricsResp.StatusCode)
	}
	body, err := io.ReadAll(metricsResp.Body)
	if err != nil {
		t.Fatalf("read metrics body: %v", err)
	}
	s := string(body)
	// One accepted record, one written, zero dropped — proves the
	// metrics handler reads the same state objects the live path
	// mutates (as opposed to a fresh zero-valued snapshot).
	for _, want := range []string{
		"binnacle_ingest_records_accepted_total 1",
		"binnacle_writer_written_total 1",
		"binnacle_writer_dropped_total 0",
		`binnacle_build_info{version="e2e-test"} 1`,
	} {
		if !bytes.Contains(body, []byte(want)) {
			t.Errorf("metrics body missing %q; got:\n%s", want, s)
		}
	}
}

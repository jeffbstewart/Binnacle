package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jeffbstewart/Binnacle/internal/store"
)

// setupDB returns a store opened on a temp dir with a running
// writer, so tests can seed records and assert end-to-end behavior.
func setupDB(t *testing.T) (*sql.DB, *store.Writer) {
	t.Helper()
	db, err := store.Open(t.Context(), t.TempDir())
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
	return db, w
}

// seed injects N records at `now` into service `svc`, then blocks
// until the writer has flushed them.
func seed(t *testing.T, w *store.Writer, svc string, sev store.Severity, n int) {
	t.Helper()
	now := time.Now()
	for i := range n {
		if err := w.Submit(store.Record{
			TimeNs:   now.UnixNano() + int64(i),
			IngestNs: now.UnixNano(),
			Severity: sev,
			Service:  svc,
			Instance: "h",
			Message:  "m",
		}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if w.Stats().Written >= uint64(n) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("seed: writer did not flush %d records in time", n)
}

// fetchQuery hits the handler with the given query string and decodes
// the response body as QueryResult. Fatals on HTTP error.
func fetchQuery(t *testing.T, srv *httptest.Server, rawQuery string) *store.QueryResult {
	t.Helper()
	resp, err := http.Get(srv.URL + "?" + rawQuery)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}
	var result store.QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return &result
}

// --- basic routing + method handling ---

func TestQueryHandler_RejectsNonGET(t *testing.T) {
	db, _ := setupDB(t)
	srv := httptest.NewServer(&QueryHandler{DB: db})
	t.Cleanup(srv.Close)

	resp, err := http.Post(srv.URL, "text/plain", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status: got %d, want 405", resp.StatusCode)
	}
}

func TestQueryHandler_EmptyStoreReturnsEmpty(t *testing.T) {
	db, _ := setupDB(t)
	srv := httptest.NewServer(&QueryHandler{DB: db})
	t.Cleanup(srv.Close)

	result := fetchQuery(t, srv, "")
	if result.CountReturned != 0 {
		t.Errorf("empty store: got %d, want 0", result.CountReturned)
	}
}

// --- filter parsing ---

func TestQueryHandler_FilterByService(t *testing.T) {
	db, w := setupDB(t)
	seed(t, w, "a", store.SeverityInfo, 2)
	seed(t, w, "b", store.SeverityInfo, 3)

	srv := httptest.NewServer(&QueryHandler{DB: db})
	t.Cleanup(srv.Close)

	result := fetchQuery(t, srv, "service=a")
	if result.CountReturned != 2 {
		t.Errorf("service=a: got %d, want 2", result.CountReturned)
	}

	// Repeated ?service= gives an OR filter.
	result = fetchQuery(t, srv, "service=a&service=b")
	if result.CountReturned != 5 {
		t.Errorf("service=a&service=b: got %d, want 5", result.CountReturned)
	}
}

func TestQueryHandler_FilterBySeverityLabel(t *testing.T) {
	db, w := setupDB(t)
	seed(t, w, "x", store.SeverityInfo, 2)
	seed(t, w, "x", store.SeverityError, 3)

	srv := httptest.NewServer(&QueryHandler{DB: db})
	t.Cleanup(srv.Close)

	result := fetchQuery(t, srv, "severity=ERROR")
	if result.CountReturned != 3 {
		t.Errorf("severity=ERROR: got %d, want 3", result.CountReturned)
	}
}

func TestQueryHandler_FilterBySeverityNumber(t *testing.T) {
	db, w := setupDB(t)
	seed(t, w, "x", store.SeverityInfo, 2)
	seed(t, w, "x", store.SeverityError, 3)

	srv := httptest.NewServer(&QueryHandler{DB: db})
	t.Cleanup(srv.Close)

	// 17 is SeverityError — agents may filter either way.
	result := fetchQuery(t, srv, "severity=17")
	if result.CountReturned != 3 {
		t.Errorf("severity=17: got %d, want 3", result.CountReturned)
	}
}

func TestQueryHandler_FilterByTimeRange(t *testing.T) {
	db, w := setupDB(t)
	now := time.Now()

	submit := func(ts time.Time) {
		if err := w.Submit(store.Record{
			TimeNs:   ts.UnixNano(),
			IngestNs: now.UnixNano(),
			Severity: store.SeverityInfo,
			Service:  "x",
			Instance: "h",
			Message:  "m",
		}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	submit(now.Add(-10 * time.Minute))
	submit(now.Add(-5 * time.Minute))
	submit(now.Add(-1 * time.Minute))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if w.Stats().Written >= 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	srv := httptest.NewServer(&QueryHandler{DB: db})
	t.Cleanup(srv.Close)

	// Window containing only the middle record.
	params := url.Values{}
	params.Set("since", now.Add(-7*time.Minute).Format(time.RFC3339))
	params.Set("until", now.Add(-3*time.Minute).Format(time.RFC3339))
	result := fetchQuery(t, srv, params.Encode())
	if result.CountReturned != 1 {
		t.Errorf("time range filter: got %d, want 1", result.CountReturned)
	}
}

func TestQueryHandler_LimitHonored(t *testing.T) {
	db, w := setupDB(t)
	seed(t, w, "x", store.SeverityInfo, 10)

	srv := httptest.NewServer(&QueryHandler{DB: db})
	t.Cleanup(srv.Close)

	result := fetchQuery(t, srv, "limit=3")
	if result.CountReturned != 3 {
		t.Errorf("limit=3: got %d, want 3", result.CountReturned)
	}
}

// --- parse errors ---

func TestQueryHandler_BadSeverityReturns400(t *testing.T) {
	db, _ := setupDB(t)
	srv := httptest.NewServer(&QueryHandler{DB: db})
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "?severity=not-a-thing")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

func TestQueryHandler_BadSinceReturns400(t *testing.T) {
	db, _ := setupDB(t)
	srv := httptest.NewServer(&QueryHandler{DB: db})
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "?since=yesterday")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

func TestQueryHandler_BadLimitReturns400(t *testing.T) {
	db, _ := setupDB(t)
	srv := httptest.NewServer(&QueryHandler{DB: db})
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "?limit=-1")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// --- round-trip output shape ---

func TestQueryHandler_JSONShape(t *testing.T) {
	db, w := setupDB(t)
	seed(t, w, "mediamanager-server", store.SeverityWarn, 1)

	srv := httptest.NewServer(&QueryHandler{DB: db})
	t.Cleanup(srv.Close)

	result := fetchQuery(t, srv, "")
	if len(result.Records) != 1 {
		t.Fatalf("records: got %d, want 1", len(result.Records))
	}
	r := result.Records[0]
	if r.Service != "mediamanager-server" {
		t.Errorf("service: got %q", r.Service)
	}
	if r.Severity != store.SeverityWarn {
		t.Errorf("severity: got %d, want %d", r.Severity, store.SeverityWarn)
	}
	if r.Time.IsZero() {
		t.Errorf("time should be populated")
	}
}

// TestQueryHandler_TimesOutWhenConnectionBusy pins the store's single
// connection and checks the handler answers 504 at the request's
// deadline rather than hanging.
func TestQueryHandler_TimesOutWhenConnectionBusy(t *testing.T) {
	db, _ := setupDB(t)
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, QueryPath, nil)
	rec := httptest.NewRecorder()

	start := time.Now()
	(&QueryHandler{DB: db}).ServeHTTP(rec, req)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504; body=%s", rec.Code, rec.Body.String())
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("handler took %s to honor a 100ms deadline", elapsed)
	}
}

// TestViewerHandler_RendersWhenConnectionBusy checks the viewer still
// returns a page (with the error shown) instead of hanging.
func TestViewerHandler_RendersWhenConnectionBusy(t *testing.T) {
	db, _ := setupDB(t)
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, ViewerPath, nil)
	rec := httptest.NewRecorder()

	start := time.Now()
	(&ViewerHandler{DB: db}).ServeHTTP(rec, req)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("viewer took %s to honor a 100ms deadline", elapsed)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 with inline error", rec.Code)
	}
}

// TestViewerHandler_DefaultWindow checks that with no since/until the
// viewer shows only DefaultViewerWindow, and since=all shows the rest.
func TestViewerHandler_DefaultWindow(t *testing.T) {
	db, w := setupDB(t)
	now := time.Now()
	for _, r := range []struct {
		msg string
		at  time.Time
	}{
		{"recent-record", now},
		{"older-record", now.Add(-2 * DefaultViewerWindow)},
	} {
		if err := w.Submit(store.Record{TimeNs: r.at.UnixNano(), IngestNs: now.UnixNano(),
			Severity: store.SeverityInfo, Service: "svc", Instance: "h", Message: r.msg}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for w.Stats().Written < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	get := func(target string) string {
		rec := httptest.NewRecorder()
		(&ViewerHandler{DB: db}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d", target, rec.Code)
		}
		return rec.Body.String()
	}

	body := get(ViewerPath)
	if !strings.Contains(body, "recent-record") || strings.Contains(body, "older-record") {
		t.Fatalf("default view should show only the recent record")
	}
	if !strings.Contains(body, "(default)") {
		t.Fatalf("default view should show the default-window chip")
	}

	body = get(ViewerPath + "?since=all")
	if !strings.Contains(body, "recent-record") || !strings.Contains(body, "older-record") {
		t.Fatalf("since=all should show both records")
	}
}

// TestViewerHandler_PagesOlder walks the "Older" links and checks the
// pages are disjoint, ordered, and end without an Older link.
func TestViewerHandler_PagesOlder(t *testing.T) {
	db, w := setupDB(t)
	now := time.Now()
	const n = 5
	for i := range n {
		if err := w.Submit(store.Record{TimeNs: now.Add(-time.Duration(i) * time.Minute).UnixNano(),
			IngestNs: now.UnixNano(), Severity: store.SeverityInfo, Service: "svc", Instance: "h",
			Message: fmt.Sprintf("rec-%d", i)}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for w.Stats().Written < n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	older := regexp.MustCompile(`<a href="(/logs\?[^"]*)">Older`)
	recRe := regexp.MustCompile(`rec-\d`)
	var seen []string
	target := ViewerPath + "?since=all&limit=2"
	for page := 0; target != ""; page++ {
		if page > n {
			t.Fatal("too many pages")
		}
		rec := httptest.NewRecorder()
		(&ViewerHandler{DB: db}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
		body := rec.Body.String()
		// Each message renders twice (row + detail panel).
		seen = append(seen, slices.Compact(recRe.FindAllString(body, -1))...)
		target = ""
		if m := older.FindStringSubmatch(body); m != nil {
			target = html.UnescapeString(m[1])
		}
	}
	want := []string{"rec-0", "rec-1", "rec-2", "rec-3", "rec-4"}
	if !slices.Equal(seen, want) {
		t.Fatalf("paged through %v, want %v", seen, want)
	}
}

// TestQueryHandler_Search checks q= reaches the store's FTS filter.
func TestQueryHandler_Search(t *testing.T) {
	db, w := setupDB(t)
	now := time.Now()
	for i, m := range []string{"disk full", "network down"} {
		if err := w.Submit(store.Record{TimeNs: now.UnixNano() + int64(i), IngestNs: now.UnixNano(),
			Severity: store.SeverityInfo, Service: "svc", Instance: "h", Message: m}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for w.Stats().Written < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	rec := httptest.NewRecorder()
	(&QueryHandler{DB: db}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, QueryPath+"?q=disk", nil))
	var res store.QueryResult
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body.String())
	}
	if res.CountReturned != 1 || res.Records[0].Message != "disk full" {
		t.Fatalf("q=disk returned %+v", res.Records)
	}
}

func TestQueryString_EscapesValues(t *testing.T) {
	until := time.Date(2026, 11, 15, 19, 0, 0, 123, time.FixedZone("x", -5*3600))
	got := queryString(viewerData{Until: until, FilterSearch: "a&b c"}, "", "")
	v, err := url.ParseQuery(strings.TrimPrefix(got, "?"))
	if err != nil {
		t.Fatalf("ParseQuery(%q): %v", got, err)
	}
	parsed, err := time.Parse(time.RFC3339, v.Get("until"))
	if err != nil || !parsed.Equal(until) {
		t.Fatalf("until round-trip: got %v (%v), want %v", parsed, err, until)
	}
	if v.Get("q") != "a&b c" {
		t.Fatalf("q round-trip: got %q", v.Get("q"))
	}
}

// TestViewerHandler_FormSubmitWithBlanks reproduces the viewer's own
// form submission with every field left at its default ("All
// services", "All levels", empty search) and checks records still
// come back, both in the viewer and the JSON API.
func TestViewerHandler_FormSubmitWithBlanks(t *testing.T) {
	db, w := setupDB(t)
	seed(t, w, "svc", store.SeverityInfo, 3)

	form := "?service=&severity=&limit=500&q=&since=&until="
	rec := httptest.NewRecorder()
	(&ViewerHandler{DB: db}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, ViewerPath+form, nil))
	if !strings.Contains(rec.Body.String(), `<div class="status">3 records`) {
		t.Fatalf("viewer form submit with blanks did not return the 3 records")
	}

	rec = httptest.NewRecorder()
	(&QueryHandler{DB: db}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, QueryPath+"?service=", nil))
	var res store.QueryResult
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil || res.CountReturned != 3 {
		t.Fatalf("API service= returned %d records (err %v), want 3", res.CountReturned, err)
	}
}

// TestViewerHandler_SelectsAutoSubmit checks each filter dropdown
// submits on change, so picking a row limit takes effect without
// pressing Search.
func TestViewerHandler_SelectsAutoSubmit(t *testing.T) {
	db, _ := setupDB(t)
	rec := httptest.NewRecorder()
	(&ViewerHandler{DB: db}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, ViewerPath, nil))
	for _, name := range []string{"service", "severity", "limit"} {
		if !strings.Contains(rec.Body.String(), `<select name="`+name+`" onchange="this.form.submit()">`) {
			t.Errorf("select %q does not auto-submit", name)
		}
	}
}

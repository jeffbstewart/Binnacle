package selfcheck

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jeffbstewart/Binnacle/internal/api"
	"github.com/jeffbstewart/Binnacle/internal/ingest"
	"github.com/jeffbstewart/Binnacle/internal/store"
)

const testKey = "test-key"

// setup wires real ingest + query + viewer handlers over a temp store
// and returns base URLs for each port. viewerOverride, if non-nil,
// replaces the viewer handler.
func setup(t *testing.T, viewerOverride http.Handler) (ingestBase, queryBase string) {
	t.Helper()
	db, err := store.Open(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	w := store.NewWriter(db, 64)
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)
	t.Cleanup(func() { cancel(); <-w.Done() })

	imux := http.NewServeMux()
	imux.Handle("POST "+ingest.OTLPPath, ingest.RequireAPIKey(testKey,
		&ingest.OTLPHandler{Writer: w, Converter: ingest.NewConverter()}, nil))
	is := httptest.NewServer(imux)
	t.Cleanup(is.Close)

	var viewer http.Handler = &api.ViewerHandler{DB: db}
	if viewerOverride != nil {
		viewer = viewerOverride
	}
	qmux := http.NewServeMux()
	qmux.Handle("GET "+api.QueryPath, &api.QueryHandler{DB: db})
	qmux.Handle("GET "+api.ViewerPath, viewer)
	qs := httptest.NewTLSServer(qmux)
	t.Cleanup(qs.Close)

	return is.URL, qs.URL
}

func TestRun_Passes(t *testing.T) {
	ib, qb := setup(t, nil)
	err := Run(context.Background(), Config{
		IngestBase: ib, QueryBase: qb, APIKey: testKey,
		Timeout: 10 * time.Second, RequestTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestRun_WrongKeyFailsIngest(t *testing.T) {
	ib, qb := setup(t, nil)
	err := Run(context.Background(), Config{
		IngestBase: ib, QueryBase: qb, APIKey: "wrong",
		Timeout: 5 * time.Second, RequestTimeout: time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "stage ingest") || !strings.Contains(err.Error(), "401") {
		t.Fatalf("want ingest 401 failure, got %v", err)
	}
}

func TestRun_HungViewerReportsTimeout(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	hang := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	ib, qb := setup(t, hang)

	err := Run(context.Background(), Config{
		IngestBase: ib, QueryBase: qb, APIKey: testKey,
		Timeout: 10 * time.Second, RequestTimeout: 300 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "stage viewer") || !isTimeout(err) {
		t.Fatalf("want viewer timeout, got %v", err)
	}
}

func TestRun_NoKeySkips(t *testing.T) {
	if err := Run(context.Background(), Config{}); err == nil {
		t.Fatal("want skip error with empty API key")
	}
}

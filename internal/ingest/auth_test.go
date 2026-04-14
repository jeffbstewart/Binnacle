package ingest

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// okHandler is a sentinel handler we wrap; seeing a 200 means the
// middleware let the request through.
var okHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
})

func TestRequireAPIKey_AcceptsCorrectKey(t *testing.T) {
	srv := httptest.NewServer(RequireAPIKey("secret-key", okHandler))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
	req.Header.Set(APIKeyHeader, "secret-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestRequireAPIKey_RejectsMissingKey(t *testing.T) {
	srv := httptest.NewServer(RequireAPIKey("secret-key", okHandler))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
	// No APIKeyHeader set.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestRequireAPIKey_RejectsWrongKey(t *testing.T) {
	srv := httptest.NewServer(RequireAPIKey("secret-key", okHandler))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
	req.Header.Set(APIKeyHeader, "not-the-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestRequireAPIKey_UnconfiguredReturns503(t *testing.T) {
	// Server started without LOGGING_API_KEY set → expected is "".
	// Every request gets 503, even one with the "right" empty key
	// (constant-time compare against "" would technically match, but
	// the empty-expected short-circuit runs first).
	srv := httptest.NewServer(RequireAPIKey("", okHandler))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
	req.Header.Set(APIKeyHeader, "") // explicit empty
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
}

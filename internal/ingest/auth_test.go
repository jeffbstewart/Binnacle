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
	srv := httptest.NewServer(RequireAPIKey("secret-key", okHandler, nil))
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
	srv := httptest.NewServer(RequireAPIKey("secret-key", okHandler, nil))
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
	srv := httptest.NewServer(RequireAPIKey("secret-key", okHandler, nil))
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

func TestRequireAPIKey_CallsOnFailForWrongKey(t *testing.T) {
	var called int
	var gotAddr, gotUA string
	onFail := func(remoteAddr, serviceName string) {
		called++
		gotAddr = remoteAddr
		gotUA = serviceName
	}

	srv := httptest.NewServer(RequireAPIKey("secret-key", okHandler, onFail))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
	req.Header.Set(APIKeyHeader, "wrong-key")
	req.Header.Set("User-Agent", "test-client/1.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
	if called != 1 {
		t.Fatalf("onFail called %d times, want 1", called)
	}
	if gotAddr == "" {
		t.Error("remoteAddr was empty")
	}
	if gotUA != "test-client/1.0" {
		t.Errorf("serviceName: got %q, want %q", gotUA, "test-client/1.0")
	}
}

func TestRequireAPIKey_DoesNotCallOnFailForCorrectKey(t *testing.T) {
	var called int
	onFail := func(_, _ string) { called++ }

	srv := httptest.NewServer(RequireAPIKey("secret-key", okHandler, onFail))
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
	if called != 0 {
		t.Fatalf("onFail called %d times on correct key, want 0", called)
	}
}

func TestRequireAPIKey_DoesNotCallOnFailForUnconfigured(t *testing.T) {
	var called int
	onFail := func(_, _ string) { called++ }

	// Empty expected key → 503, and onFail should NOT fire (this is
	// a server config problem, not a client auth failure).
	srv := httptest.NewServer(RequireAPIKey("", okHandler, onFail))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
	if called != 0 {
		t.Fatalf("onFail called %d times on unconfigured server, want 0", called)
	}
}

func TestRequireAPIKey_UnconfiguredReturns503(t *testing.T) {
	// Server started without LOGGING_API_KEY set → expected is "".
	// Every request gets 503, even one with the "right" empty key
	// (constant-time compare against "" would technically match, but
	// the empty-expected short-circuit runs first).
	srv := httptest.NewServer(RequireAPIKey("", okHandler, nil))
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

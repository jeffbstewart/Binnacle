package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
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

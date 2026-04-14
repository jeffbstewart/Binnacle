package ingest

import (
	"crypto/subtle"
	"net/http"
)

// APIKeyHeader is the HTTP header name that carries the Phase 1
// shared write-path API key. Kept as a constant so tests and clients
// agree with the server.
const APIKeyHeader = "X-Logging-Api-Key"

// RequireAPIKey wraps next so only requests presenting the correct
// APIKeyHeader value reach it. The expected key is captured at
// middleware construction time; changing it requires restarting the
// server (Phase 5 will replace this with a rotatable key table).
//
// If expected is empty the middleware refuses all requests with 503
// Service Unavailable — the server was started without the env var
// set. This is intentional: running an ingest port with no auth
// wide open is worse than being unreachable.
//
// Comparison uses subtle.ConstantTimeCompare so a buggy or malicious
// client can't mount a timing attack to learn the expected key.
func RequireAPIKey(expected string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if expected == "" {
			http.Error(w, "write path not configured", http.StatusServiceUnavailable)
			return
		}
		got := r.Header.Get(APIKeyHeader)
		if subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

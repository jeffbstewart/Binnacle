package ingest

import (
	"crypto/subtle"
	"net/http"
)

// APIKeyHeader is the HTTP header name that carries the Phase 1
// shared write-path API key. Kept as a constant so tests and clients
// agree with the server.
const APIKeyHeader = "X-Logging-Api-Key"

// AuthFailureFunc is called on every auth rejection so the caller
// can log or meter the event. remoteAddr is r.RemoteAddr (IP:port);
// serviceName is whatever the client put in the User-Agent header
// (empty if absent). The callee owns rate-limiting.
type AuthFailureFunc func(remoteAddr, serviceName string)

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
//
// onFail is called on every auth rejection; pass nil to skip.
func RequireAPIKey(expected string, next http.Handler, onFail AuthFailureFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if expected == "" {
			http.Error(w, "write path not configured", http.StatusServiceUnavailable)
			return
		}
		got := r.Header.Get(APIKeyHeader)
		if subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
			if onFail != nil {
				onFail(r.RemoteAddr, r.UserAgent())
			}
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

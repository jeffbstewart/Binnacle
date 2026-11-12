// Package selflog records selected Binnacle lifecycle and security
// events into the store itself so they appear in query results
// alongside client-emitted logs. Only explicitly chosen events are
// recorded — this is NOT a general-purpose slog tee.
//
// Usage: call Init once after the writer goroutine is running, then
// call the exported event functions (Startup, ShutdownRequested, etc.)
// at the relevant points in main. Each function also emits the event
// to slog for stderr so the two destinations stay in sync.
package selflog

import (
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jeffbstewart/Binnacle/internal/store"
)

const serviceName = "binnacle"

var (
	writer   *store.Writer
	hostname string
	ver      string
)

// Init captures the writer and version string. Must be called once
// before any event function. Safe to call before the HTTP servers
// start — the writer goroutine must already be running.
func Init(w *store.Writer, version string) {
	writer = w
	ver = version
	h, err := os.Hostname()
	if err != nil {
		h = "unknown"
	}
	hostname = h
}

// emit submits one record at the given severity. Errors are silently
// dropped — self-logging must never break the service.
func emit(sev store.Severity, message string) {
	if writer == nil {
		return
	}
	now := time.Now()
	_ = writer.Submit(store.Record{
		TimeNs:   now.UnixNano(),
		IngestNs: now.UnixNano(),
		Severity: sev,
		Service:  serviceName,
		Instance: hostname,
		Version:  ver,
		Message:  message,
	})
}

// --- lifecycle events ---

// Startup records that the service has finished initialization and is
// about to open its listen ports.
func Startup() {
	slog.Info("binnacle ready")
	emit(store.SeverityInfo, "binnacle started, version "+ver)
}

// ShutdownRequested records that a SIGTERM/SIGINT was received.
func ShutdownRequested() {
	slog.Info("shutdown signal received, stopping")
	emit(store.SeverityInfo, "shutdown requested")
}

// ShutdownComplete records that HTTP ports are closed and the writer
// is about to drain. This is the last self-log record that will land
// in the store before the DB closes.
func ShutdownComplete(stats store.WriterStats) {
	slog.Info("writer drained",
		"submitted", stats.Submitted,
		"written", stats.Written,
		"dropped", stats.Dropped,
		"too_old", stats.TooOld,
		"queue_len", stats.QueueLen)
	emit(store.SeverityInfo, "shutdown complete")
}

// --- security events (rate-limited) ---

var (
	authFailMu       sync.Mutex
	authFailLastTime time.Time
)

// AuthFailure records a failed authentication attempt, rate-limited
// to at most one stored record per minute. Every failure is still
// logged to slog (stderr) at WARN level for real-time tailing; the
// rate limit only applies to the stored record so a brute-force
// probe can't fill the partition.
func AuthFailure(remoteAddr, serviceName string) {
	slog.Warn("auth failure",
		"remote_addr", remoteAddr,
		"service", serviceName)

	authFailMu.Lock()
	defer authFailMu.Unlock()
	if time.Since(authFailLastTime) < time.Minute {
		return
	}
	authFailLastTime = time.Now()

	emit(store.SeverityWarn, "auth failure from "+remoteAddr+" (service: "+serviceName+")")
}

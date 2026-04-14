// Binnacle is a unified log collection, storage, and query service for a
// home NAS. See README.md for the design proposal.
//
// main.go wires the three long-lived pieces together:
//
//   - store.Writer, the single goroutine that drains the ingest queue
//     into SQLite (plus the schema migrator + daily partition cache
//     sitting behind it).
//   - ingest.OTLPHandler, mounted behind ingest.RequireAPIKey on the
//     OTLP-HTTP port so OpenTelemetry SDKs can POST batches.
//   - api.QueryHandler, mounted on the query port alongside a health
//     endpoint so agents and humans can read back what was written.
//
// OTLP-gRPC (port 4317) is reserved but unbound in Phase 1; it lands
// in Phase 2.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jeffbstewart/Binnacle/internal/api"
	"github.com/jeffbstewart/Binnacle/internal/ingest"
	"github.com/jeffbstewart/Binnacle/internal/store"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

type config struct {
	dataDir       string
	retentionDays int
	otlpGRPCPort  int
	otlpHTTPPort  int
	queryPort     int
	logLevel      string
	apiKeyEnv     string
	healthcheck   bool
}

func parseFlags() (config, error) {
	fs := flag.NewFlagSet("binnacle", flag.ContinueOnError)

	var c config
	fs.StringVar(&c.dataDir, "data-dir", "/data", "directory for the SQLite log store")
	fs.IntVar(&c.retentionDays, "retention-days", 7, "how many days of logs to keep before partition drop")
	fs.IntVar(&c.otlpGRPCPort, "otlp-grpc-port", 4317, "port for OTLP gRPC ingest")
	fs.IntVar(&c.otlpHTTPPort, "otlp-http-port", 4318, "port for OTLP HTTP ingest")
	fs.IntVar(&c.queryPort, "query-port", 8088, "port for the query API and UI")
	fs.StringVar(&c.logLevel, "log-level", "info", "log level: debug, info, warn, error")
	fs.StringVar(&c.apiKeyEnv, "api-key-env", "LOGGING_API_KEY",
		"name of the env var holding the shared write-path API key")
	fs.BoolVar(&c.healthcheck, "healthcheck", false,
		"probe the local /api/logs/health endpoint and exit 0 (ok) or 1 (fail); used as the Docker HEALTHCHECK")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return c, err
	}

	if c.retentionDays < 1 {
		return c, fmt.Errorf("--retention-days must be >= 1, got %d", c.retentionDays)
	}
	return c, nil
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}

	// --healthcheck short-circuits to a probe of the locally-bound query
	// port and exits. Docker HEALTHCHECK calls this.
	if cfg.healthcheck {
		os.Exit(runHealthcheck(cfg.queryPort))
	}

	logger := newLogger(cfg.logLevel)
	slog.SetDefault(logger)

	apiKey := os.Getenv(cfg.apiKeyEnv)
	if apiKey == "" {
		slog.Warn("no write-path API key set; ingest endpoints will reject all requests",
			"env_var", cfg.apiKeyEnv)
	}

	slog.Info("binnacle starting",
		"version", version,
		"data_dir", cfg.dataDir,
		"retention_days", cfg.retentionDays,
		"otlp_grpc_port", cfg.otlpGRPCPort,
		"otlp_http_port", cfg.otlpHTTPPort,
		"query_port", cfg.queryPort)

	// sigCtx fires on SIGINT/SIGTERM. It is the trigger for shutdown,
	// not the writer's lifetime — we want the writer to stay alive a
	// little longer than the HTTP servers so any in-flight request
	// that already got past Submit() can finish persisting.
	sigCtx, stopSignals := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	// Open the SQLite store: creates the data directory if missing,
	// enables WAL mode, and applies any pending schema migrations.
	db, err := store.Open(cfg.dataDir)
	if err != nil {
		slog.Error("open store failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := db.Close(); err != nil {
			slog.Error("store close error", "error", err)
		}
	}()

	// Writer has its own context so shutdown can order explicitly:
	// (1) stop accepting new HTTP requests,
	// (2) cancel writerCtx and wait for the queue to drain,
	// (3) close the DB.
	writerCtx, stopWriter := context.WithCancel(context.Background())
	writer := store.NewWriter(db, 1024)
	go writer.Run(writerCtx)

	otlp := &ingest.OTLPHandler{Writer: writer, Converter: ingest.NewConverter()}
	query := &api.QueryHandler{DB: db}

	ingestSrv := buildIngestServer(cfg.otlpHTTPPort, apiKey, otlp)
	querySrv := buildQueryServer(cfg.queryPort, query)

	// If either server fails to bind (port in use, permission denied)
	// we want the whole process to exit, not limp along half-serving.
	// fatalErr is closed in that case; main treats it the same as a
	// signal.
	fatalErr := make(chan struct{})
	var fatalOnce sync.Once
	fatal := func() { fatalOnce.Do(func() { close(fatalErr) }) }

	go runServer(ingestSrv, "ingest", fatal)
	go runServer(querySrv, "query", fatal)

	select {
	case <-sigCtx.Done():
		slog.Info("shutdown signal received, stopping")
	case <-fatalErr:
		slog.Error("fatal server error, stopping")
	}

	// Stage 1: refuse new HTTP requests. Shutdown returns once in-flight
	// handlers return, which for ingest includes the Submit() call — so
	// anything that was going to land in the queue is already there.
	shutdownHTTPServers(ingestSrv, querySrv)

	// Stage 2: tell the writer to drain and wait for it. Run() exits
	// its select on ctx.Done, then drains any queued records before
	// closing Done().
	stopWriter()
	<-writer.Done()
	s := writer.Stats()
	slog.Info("writer drained",
		"submitted", s.Submitted,
		"written", s.Written,
		"dropped", s.Dropped,
		"too_old", s.TooOld,
		"queue_len", s.QueueLen)

	// Stage 3: defer runs db.Close() on return.
	slog.Info("binnacle stopped")
}

// buildIngestServer mounts the OTLP HTTP handler at OTLPPath behind
// the shared-key middleware. Non-OTLP paths get 404.
func buildIngestServer(port int, apiKey string, otlp http.Handler) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("POST "+ingest.OTLPPath, ingest.RequireAPIKey(apiKey, otlp))
	return &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// buildQueryServer mounts the health check and the structured-query
// handler. No auth — the read path is LAN-only by design.
func buildQueryServer(port int, query http.Handler) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/logs/health", healthHandler)
	mux.Handle("GET "+api.QueryPath, query)

	// TODO(phase2): mount /api/logs/schema, /summary, /errors,
	//               /correlation/{id}, /tail, /stats, and the HTML UI.

	return &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// runServer blocks on srv.ListenAndServe and calls fatal on any error
// other than the "server was asked to shut down" sentinel. name is
// only used for logging.
func runServer(srv *http.Server, name string, fatal func()) {
	slog.Info(name+" server listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error(name+" server failed", "error", err)
		fatal()
	}
}

// shutdownHTTPServers calls Shutdown on both servers in parallel with
// a shared 10s budget. Errors are logged but don't stop us from
// moving on to the writer drain — a stuck HTTP handler is not a
// reason to lose unwritten records.
func shutdownHTTPServers(servers ...*http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, srv := range servers {
		wg.Add(1)
		go func(s *http.Server) {
			defer wg.Done()
			if err := s.Shutdown(ctx); err != nil {
				slog.Error("http server shutdown error", "addr", s.Addr, "error", err)
			}
		}(srv)
	}
	wg.Wait()
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"version": version,
	})
}

// runHealthcheck probes /api/logs/health on localhost and returns a
// POSIX-style exit code. The binary invokes itself this way via the
// Dockerfile HEALTHCHECK directive.
//
// It intentionally uses 127.0.0.1 instead of localhost so it doesn't
// depend on /etc/hosts — distroless doesn't ship one by default.
func runHealthcheck(port int) int {
	url := fmt.Sprintf("http://127.0.0.1:%d/api/logs/health", port)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck failed:", err)
		return 1
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck HTTP %d: %s\n", resp.StatusCode, string(body))
		return 1
	}
	_, _ = os.Stdout.Write(body)
	return 0
}

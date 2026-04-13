// Binnacle is a unified log collection, storage, and query service for a
// home NAS. See README.md for the design proposal.
//
// This is the current scaffold: CLI flag parsing, structured logging,
// graceful shutdown on SIGINT/SIGTERM, and a health endpoint. OTLP ingest,
// SQLite storage, and the query API are TODOs slotted in where they'll
// eventually land.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
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

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// TODO(phase1): start OTLP gRPC ingest on cfg.otlpGRPCPort.
	// TODO(phase1): start OTLP HTTP ingest on cfg.otlpHTTPPort.
	// TODO(phase1): open SQLite, manage daily partitions, run retention loop.

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/logs/health", healthHandler)

	// TODO(phase1): mount /api/logs/schema, /summary, /errors, /correlation/{id},
	//               /query, /tail, /stats, and the HTML UI on this mux.

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.queryPort),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("query server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("query server failed", "error", err)
			cancel()
		}
	}()

	<-ctx.Done()
	slog.Info("shutdown signal received, stopping")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("query server shutdown error", "error", err)
	}

	slog.Info("binnacle stopped")
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"version": version,
	})
}

// Package selfcheck runs an end-to-end probe of Binnacle's own log
// plumbing at startup: it POSTs one OTLP record to the local ingest
// port, then reads it back through the query API and the HTML viewer,
// reporting each stage's outcome and latency on stderr via slog.
//
// It exists so an operator looking at `docker logs` can tell at a
// glance whether ingest → writer → SQLite → query → viewer works, and
// if not, which stage failed or hung. Every HTTP call carries its own
// timeout so a wedged stage shows up as an explicit "timed out" line
// rather than silence.
package selfcheck

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	collectorpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/jeffbstewart/Binnacle/internal/api"
	"github.com/jeffbstewart/Binnacle/internal/ingest"
)

// ServiceName is the service.name the probe record is tagged with, so
// it can be filtered out (or in) in the viewer.
const ServiceName = "binnacle-selfcheck"

// Config describes where to send the probe and how long to wait.
type Config struct {
	// IngestBase is the OTLP-HTTP base URL, e.g. "http://127.0.0.1:4318".
	IngestBase string
	// QueryBase is the query-port base URL, e.g. "https://127.0.0.1:8088".
	QueryBase string
	// APIKey is the shared write-path key sent in ingest.APIKeyHeader.
	APIKey string
	// Timeout bounds the whole check.
	Timeout time.Duration
	// RequestTimeout bounds each individual HTTP request. A request
	// that exceeds it is reported as a hang and fails the check.
	RequestTimeout time.Duration
	// Client is used for all requests; nil means a default client that
	// skips TLS verification (the query port uses a self-signed cert
	// and the probe only ever talks to loopback).
	Client *http.Client
}

// Run executes the probe, logging each stage, and returns nil on
// success or an error naming the stage that failed.
func Run(ctx context.Context, cfg Config) error {
	if cfg.APIKey == "" {
		slog.Warn("self-check skipped: no write-path API key configured")
		return errors.New("self-check skipped: no API key")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // loopback only
		}}
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	c := &checker{cfg: cfg, client: client, nonce: newNonce(), start: time.Now()}
	slog.Info("self-check starting", "nonce", c.nonce,
		"ingest", cfg.IngestBase, "query", cfg.QueryBase)

	stages := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"ingest", c.ingest},
		{"query", c.query},
		{"viewer", c.viewer},
	}
	for _, s := range stages {
		t0 := time.Now()
		if err := s.fn(ctx); err != nil {
			slog.Error("self-check FAILED", "stage", s.name,
				"elapsed", time.Since(t0).String(), "nonce", c.nonce, "error", err)
			return fmt.Errorf("self-check stage %s: %w", s.name, err)
		}
		slog.Info("self-check stage ok", "stage", s.name, "elapsed", time.Since(t0).String())
	}
	slog.Info("self-check passed", "nonce", c.nonce, "total", time.Since(c.start).String())
	return nil
}

type checker struct {
	cfg    Config
	client *http.Client
	nonce  string
	start  time.Time
}

func (c *checker) message() string { return "startup self-check " + c.nonce }

// ingest POSTs the probe record. Transport errors (typically
// "connection refused" while the listener is still binding) are
// retried until ctx expires; any HTTP response is final.
func (c *checker) ingest(ctx context.Context) error {
	body, err := proto.Marshal(c.batch())
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	for attempt := 1; ; attempt++ {
		status, respBody, err := c.do(ctx, http.MethodPost, c.cfg.IngestBase+ingest.OTLPPath, body)
		if err == nil {
			if status != http.StatusOK {
				return fmt.Errorf("HTTP %d: %s", status, truncate(respBody))
			}
			var resp collectorpb.ExportLogsServiceResponse
			if err := proto.Unmarshal(respBody, &resp); err != nil {
				return fmt.Errorf("decode response: %w", err)
			}
			if ps := resp.GetPartialSuccess(); ps.GetRejectedLogRecords() > 0 {
				return fmt.Errorf("server rejected %d record(s): %s",
					ps.GetRejectedLogRecords(), ps.GetErrorMessage())
			}
			return nil
		}
		if isTimeout(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("gave up after %d attempt(s): %w", attempt, err)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// query polls the JSON query API until the probe record appears. The
// writer is asynchronous, so "not yet visible" is retried; a request
// that times out is not.
func (c *checker) query(ctx context.Context) error {
	u := c.cfg.QueryBase + api.QueryPath + "?" + c.filter().Encode()
	for attempt := 1; ; attempt++ {
		status, body, err := c.do(ctx, http.MethodGet, u, nil)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("HTTP %d: %s", status, truncate(body))
		}
		var res struct {
			Records []struct {
				Message string `json:"message"`
			} `json:"records"`
		}
		if err := json.Unmarshal(body, &res); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
		for _, r := range res.Records {
			if r.Message == c.message() {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("record not visible after %d attempt(s)", attempt)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// viewer fetches the HTML log viewer filtered to the probe and checks
// the record renders. By this point the query stage has proven the
// record is stored, so a single attempt suffices.
func (c *checker) viewer(ctx context.Context) error {
	status, body, err := c.do(ctx, http.MethodGet,
		c.cfg.QueryBase+api.ViewerPath+"?"+c.filter().Encode(), nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", status, truncate(body))
	}
	if !bytes.Contains(body, []byte(c.nonce)) {
		return fmt.Errorf("rendered page (%d bytes) does not contain the probe record", len(body))
	}
	return nil
}

func (c *checker) filter() url.Values {
	return url.Values{
		"service": {ServiceName},
		// RFC3339 has second precision; back off a second so the
		// record's own timestamp is inside the window.
		"since": {c.start.Add(-time.Second).UTC().Format(time.RFC3339)},
		"limit": {"10"},
	}
}

// errRequestTimeout marks a single request that exceeded
// RequestTimeout — the "hang" signal this package exists to surface.
var errRequestTimeout = errors.New("request timed out")

func isTimeout(err error) bool { return errors.Is(err, errRequestTimeout) }

// do performs one request bounded by RequestTimeout and returns the
// status and (capped) body.
func (c *checker) do(ctx context.Context, method, u string, body []byte) (int, []byte, error) {
	rctx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()

	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(rctx, method, u, rdr)
	if err != nil {
		return 0, nil, err
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set(ingest.APIKeyHeader, c.cfg.APIKey)
	}
	req.Header.Set("User-Agent", ServiceName)

	resp, err := c.client.Do(req)
	if err == nil {
		defer resp.Body.Close()
		var data []byte
		data, err = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err == nil {
			return resp.StatusCode, data, nil
		}
	}
	// Distinguish our per-request deadline from the overall one so
	// the log says "this request hung" rather than a generic error.
	if errors.Is(rctx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
		return 0, nil, fmt.Errorf("%w after %s: %s %s",
			errRequestTimeout, c.cfg.RequestTimeout, method, stripQuery(u))
	}
	return 0, nil, err
}

func (c *checker) batch() *collectorpb.ExportLogsServiceRequest {
	str := func(s string) *commonpb.AnyValue {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
	}
	return &collectorpb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{{Key: "service.name", Value: str(ServiceName)}},
			},
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					TimeUnixNano:   uint64(time.Now().UnixNano()),
					SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
					SeverityText:   "INFO",
					Body:           str(c.message()),
				}},
			}},
		}},
	}
}

func newNonce() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func truncate(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}

func stripQuery(u string) string {
	if i := strings.IndexByte(u, '?'); i >= 0 {
		return u[:i]
	}
	return u
}

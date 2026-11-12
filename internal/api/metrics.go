package api

import (
	"database/sql"
	"fmt"
	"io"
	"net/http"

	"github.com/jeffbstewart/Binnacle/internal/ingest"
	"github.com/jeffbstewart/Binnacle/internal/store"
)

// MetricsPath is the canonical scrape endpoint. Exposed as a constant
// so handler registration, tests, and the Prometheus config agree.
const MetricsPath = "/metrics"

// promContentType is the Prometheus text-format Content-Type for
// v0.0.4 (the current stable). Newer "application/openmetrics-text"
// exists but is not negotiated here — Prometheus accepts either and
// every downstream tool (Grafana, node_exporter, VictoriaMetrics)
// still speaks the v0.0.4 flavor.
const promContentType = "text/plain; version=0.0.4; charset=utf-8"

// MetricsHandler exposes Binnacle's internal counters in Prometheus
// text format. Each scrape snapshots every Stats() struct once — a
// scrape is a point-in-time read, not a reconciliation — and hits the
// partitions control table for the partition count.
//
// Rationale for hand-rolling the text format instead of pulling in
// prometheus/client_golang:
//
//   - Our counters already live in atomic.Uint64 on the producers.
//     Wrapping them in a prom.Counter gains us nothing operationally
//     and costs one more dependency plus the boilerplate to register
//     every metric with a registry.
//   - The text format is 14 lines of code to render. When we
//     eventually need histograms or label vectors, that's the moment
//     to reach for client_golang — not before.
type MetricsHandler struct {
	DB      *sql.DB
	Writer  *store.Writer
	Ingest  *ingest.OTLPHandler // Converter is reached via Ingest.Converter
	Version string
}

// ServeHTTP implements http.Handler.
func (h *MetricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", promContentType)
	h.render(w)
}

// render writes the full metric snapshot. Split out so tests can
// assert on content without spinning an HTTP server.
func (h *MetricsHandler) render(w io.Writer) {
	ws := h.Writer.Stats()
	counter(w, "binnacle_writer_submitted_total",
		"Total records submitted to the writer queue.", ws.Submitted)
	counter(w, "binnacle_writer_written_total",
		"Total records persisted to SQLite.", ws.Written)
	counter(w, "binnacle_writer_dropped_total",
		"Records dropped because the writer queue was full.", ws.Dropped)
	counter(w, "binnacle_writer_too_old_total",
		"Records rejected at Submit because client timestamp was older than MaxRecordAge.", ws.TooOld)
	gauge(w, "binnacle_writer_queue_length",
		"Current length of the writer queue.", uint64(ws.QueueLen))

	is := h.Ingest.Stats()
	counter(w, "binnacle_ingest_records_accepted_total",
		"OTLP log records accepted and enqueued by the writer.", is.RecordsAccepted)
	counter(w, "binnacle_ingest_records_dropped_total",
		"OTLP log records dropped after Submit returned an error (queue full or too old).", is.RecordsDropped)
	counter(w, "binnacle_ingest_rejected_bad_method_total",
		"Ingest requests rejected with 405 Method Not Allowed.", is.RejectedBadMethod)
	counter(w, "binnacle_ingest_rejected_bad_content_type_total",
		"Ingest requests rejected with 415 Unsupported Media Type.", is.RejectedBadCT)
	counter(w, "binnacle_ingest_rejected_bad_format_total",
		"Ingest requests rejected with 400 due to a malformed body.", is.RejectedBadFormat)
	counter(w, "binnacle_ingest_rejected_oversize_total",
		"Ingest requests rejected with 413 Request Entity Too Large.", is.RejectedOversize)

	cs := h.Ingest.Converter.Stats()
	counter(w, "binnacle_converter_dropped_trace_ids_total",
		"OTLP log records whose TraceId was non-empty but not exactly 16 bytes.", cs.DroppedTraceIDs)
	counter(w, "binnacle_converter_dropped_span_ids_total",
		"OTLP log records whose SpanId was non-empty but not exactly 8 bytes.", cs.DroppedSpanIDs)

	// Partition count is a cheap single-row SELECT on an indexed PK.
	// If it fails (e.g., DB going down) we just skip the metric —
	// emitting a broken line would make the whole scrape fail parsing
	// downstream.
	var partitions int64
	if err := h.DB.QueryRow(`SELECT COUNT(*) FROM partitions`).Scan(&partitions); err == nil {
		gauge(w, "binnacle_partitions",
			"Current number of daily partition tables.", uint64(partitions))
	}

	// build_info is a Prometheus convention: always-1 gauge with the
	// interesting facts on labels. Lets dashboards pivot by version
	// without cardinality concerns.
	fmt.Fprintf(w, "# HELP binnacle_build_info Build and version info (always 1; carries labels).\n")
	fmt.Fprintf(w, "# TYPE binnacle_build_info gauge\n")
	fmt.Fprintf(w, "binnacle_build_info{version=%q} 1\n", h.Version)
}

// counter emits a single HELP/TYPE/value triplet for a counter. The
// Prometheus convention is for counter names to end in `_total`.
func counter(w io.Writer, name, help string, value uint64) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s counter\n", name)
	fmt.Fprintf(w, "%s %d\n", name, value)
}

// gauge emits a single HELP/TYPE/value triplet for a gauge.
func gauge(w io.Writer, name, help string, value uint64) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s gauge\n", name)
	fmt.Fprintf(w, "%s %d\n", name, value)
}

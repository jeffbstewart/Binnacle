package ingest

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	collectorpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/jeffbstewart/Binnacle/internal/store"
)

// OTLPPath is the standard OTLP HTTP endpoint path for log records.
// Every OTel SDK's OTLP-HTTP exporter POSTs here.
const OTLPPath = "/v1/logs"

// MaxBatchBytes is the per-request body cap. Matches OTLP's
// recommended size and design doc §R4 / §A4 (oversized-record DoS).
const MaxBatchBytes = 4 * 1024 * 1024

// wireFormat enumerates the two OTLP-HTTP wire encodings we accept.
// Decoding and response-encoding both pivot on this, so extracting
// it to a type avoids stringly-typed branching.
type wireFormat int

const (
	wireProtobuf wireFormat = iota
	wireJSON
)

func (f wireFormat) contentType() string {
	if f == wireJSON {
		return "application/json"
	}
	return "application/x-protobuf"
}

// OTLPHandler is the POST /v1/logs handler: decodes an OTLP batch
// (protobuf or JSON), converts records, submits them to the writer,
// and returns an OTLP response describing any partial failures.
//
// The handler is a thin shell around ingest.Converter + store.Writer
// so it stays testable: a test can point it at an in-memory Writer
// and verify wire-level behavior without SQL or HTTP plumbing.
type OTLPHandler struct {
	Writer    *store.Writer
	Converter *Converter

	// Now is injectable for deterministic ingest timestamps in tests.
	// Defaults to time.Now when nil.
	Now func() time.Time

	// Counters for /metrics. See Stats().
	rejectedBadFormat atomic.Uint64
	rejectedBadMethod atomic.Uint64
	rejectedBadCT     atomic.Uint64
	rejectedOversize  atomic.Uint64
	recordsAccepted   atomic.Uint64
	recordsDropped    atomic.Uint64
}

// OTLPHandlerStats is a read-only snapshot of handler counters.
type OTLPHandlerStats struct {
	RejectedBadFormat uint64
	RejectedBadMethod uint64
	RejectedBadCT     uint64
	RejectedOversize  uint64
	RecordsAccepted   uint64
	RecordsDropped    uint64
}

// Stats returns a counter snapshot.
func (h *OTLPHandler) Stats() OTLPHandlerStats {
	return OTLPHandlerStats{
		RejectedBadFormat: h.rejectedBadFormat.Load(),
		RejectedBadMethod: h.rejectedBadMethod.Load(),
		RejectedBadCT:     h.rejectedBadCT.Load(),
		RejectedOversize:  h.rejectedOversize.Load(),
		RecordsAccepted:   h.recordsAccepted.Load(),
		RecordsDropped:    h.recordsDropped.Load(),
	}
}

// ServeHTTP implements http.Handler. Each stage below returns early
// on rejection; the counters and response are owned by the helper
// that made the decision. ServeHTTP itself is just the recipe.
func (h *OTLPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.requirePost(w, r) {
		return
	}
	format, ok := h.parseContentType(w, r)
	if !ok {
		return
	}
	body, ok := h.readBody(w, r)
	if !ok {
		return
	}
	req, ok := h.decodeBatch(w, body, format)
	if !ok {
		return
	}

	ingestTs := h.now()
	records := h.Converter.Convert(ingestTs, req.GetResourceLogs())
	rejected := h.submitAll(records)

	h.writeResponse(w, format, rejected)
}

// --- request-parsing stages ---

// requirePost rejects anything that isn't POST with 405 Method Not
// Allowed. Returns true if the handler should continue.
func (h *OTLPHandler) requirePost(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodPost {
		return true
	}
	h.rejectedBadMethod.Add(1)
	w.Header().Set("Allow", http.MethodPost)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

// parseContentType maps the Content-Type header to a wireFormat.
// Unsupported types get a 415.
func (h *OTLPHandler) parseContentType(w http.ResponseWriter, r *http.Request) (wireFormat, bool) {
	switch stripCharset(r.Header.Get("Content-Type")) {
	case "application/x-protobuf", "application/protobuf":
		return wireProtobuf, true
	case "application/json":
		return wireJSON, true
	default:
		h.rejectedBadCT.Add(1)
		http.Error(w,
			"unsupported Content-Type (want application/x-protobuf or application/json)",
			http.StatusUnsupportedMediaType)
		return 0, false
	}
}

// readBody reads the request body with a one-byte-over-cap limit so
// we can distinguish exactly-at-cap from over-cap and return 413.
func (h *OTLPHandler) readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxBatchBytes+1))
	if err != nil {
		h.rejectedBadFormat.Add(1)
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return nil, false
	}
	if len(body) > MaxBatchBytes {
		h.rejectedOversize.Add(1)
		http.Error(w, "request entity too large", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	return body, true
}

// decodeBatch unmarshals the OTLP batch per its wire format.
func (h *OTLPHandler) decodeBatch(w http.ResponseWriter, body []byte, format wireFormat) (*collectorpb.ExportLogsServiceRequest, bool) {
	var req collectorpb.ExportLogsServiceRequest
	var err error
	switch format {
	case wireJSON:
		err = protojson.Unmarshal(body, &req)
	case wireProtobuf:
		err = proto.Unmarshal(body, &req)
	}
	if err != nil {
		h.rejectedBadFormat.Add(1)
		http.Error(w, "decode "+format.contentType()+": "+err.Error(), http.StatusBadRequest)
		return nil, false
	}
	return &req, true
}

// --- persistence stage ---

// submitAll hands every record to the writer, counting per-record
// rejections and unexpected errors. Returns the total rejected for
// PartialSuccess reporting.
func (h *OTLPHandler) submitAll(records []store.Record) int64 {
	var rejected int64
	for _, rec := range records {
		if err := h.Writer.Submit(rec); err != nil {
			rejected++
			if !errors.Is(err, store.ErrQueueFull) && !errors.Is(err, store.ErrRecordTooOld) {
				slog.Warn("unexpected writer error", "error", err)
			}
		}
	}
	h.recordsAccepted.Add(uint64(int64(len(records)) - rejected))
	h.recordsDropped.Add(uint64(rejected))
	return rejected
}

// --- response stage ---

// writeResponse emits an OTLP ExportLogsServiceResponse in the same
// wire format as the request. PartialSuccess is set if any records
// were rejected — OTel SDKs read that field to decide whether to
// back off or drop to a local buffer.
func (h *OTLPHandler) writeResponse(w http.ResponseWriter, format wireFormat, rejected int64) {
	resp := &collectorpb.ExportLogsServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &collectorpb.ExportLogsPartialSuccess{
			RejectedLogRecords: rejected,
			ErrorMessage:       "writer queue full or record older than MaxRecordAge",
		}
	}

	var data []byte
	switch format {
	case wireJSON:
		data, _ = protojson.Marshal(resp)
	case wireProtobuf:
		data, _ = proto.Marshal(resp)
	}
	w.Header().Set("Content-Type", format.contentType())
	_, _ = w.Write(data)
}

// --- small helpers ---

// now returns the handler's configured clock or time.Now.
func (h *OTLPHandler) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

// stripCharset turns "application/json; charset=utf-8" into
// "application/json" (lowercased, trimmed). Good enough for the
// two Content-Type strings we accept.
func stripCharset(ct string) string {
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	return strings.ToLower(strings.TrimSpace(ct))
}

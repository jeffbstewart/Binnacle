// Package ingest converts OpenTelemetry OTLP wire-format log batches
// into Binnacle's internal store.Record shape and writes them via a
// store.Writer.
package ingest

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/jeffbstewart/Binnacle/internal/store"
)

// Semantic-convention attribute keys that we lift out of the flat
// OTel attribute bag and promote to first-class Record fields.
// Everything else stays in Record.Attrs.
const (
	attrServiceName     = "service.name"
	attrServiceVersion  = "service.version"
	attrServiceInstance = "service.instance.id"
	attrCodeNamespace   = "code.namespace"

	// Exception attribute keys, OTel semantic conventions.
	attrExceptionType       = "exception.type"
	attrExceptionMessage    = "exception.message"
	attrExceptionStackTrace = "exception.stacktrace"
)

// Converter flattens OTLP ResourceLogs into store.Record slices and
// tracks operational counters along the way. One Converter is
// typically created per OTLP-HTTP handler at startup; its counters
// get exposed via /metrics in Phase 2 and surface drop anomalies
// (malformed client IDs, out-of-spec payloads) without the operator
// having to grep logs.
//
// Safe for concurrent use — the counters are atomic and the
// conversion itself is purely functional apart from counter writes.
type Converter struct {
	// droppedTraceIDs counts LogRecords whose TraceId field was
	// non-empty but not exactly TraceIDSize bytes. A healthy client
	// never hits this; a steady trickle signals an instrumentation
	// bug in an emitter.
	droppedTraceIDs atomic.Uint64

	// droppedSpanIDs counts LogRecords whose SpanId field was
	// non-empty but not exactly SpanIDSize bytes.
	droppedSpanIDs atomic.Uint64
}

// NewConverter constructs a Converter with its counters at zero.
func NewConverter() *Converter { return &Converter{} }

// ConverterStats is a read-only snapshot of the counters.
type ConverterStats struct {
	DroppedTraceIDs uint64
	DroppedSpanIDs  uint64
}

// Stats returns the current counter values.
func (c *Converter) Stats() ConverterStats {
	return ConverterStats{
		DroppedTraceIDs: c.droppedTraceIDs.Load(),
		DroppedSpanIDs:  c.droppedSpanIDs.Load(),
	}
}

// Convert flattens an OTLP batch's ResourceLogs tree into a slice of
// store.Record, stamping ingestTs on each. Resource-level attributes
// (including service.name and friends) are promoted to Record fields
// where semantic conventions apply; the rest merge into Record.Attrs
// per log record.
//
// Conversion is total — no error return. A record with missing
// required fields is still produced (the store layer's MaxRecordAge
// / non-null constraints will reject it at Submit if it's truly
// malformed). This keeps a single bad log record from failing an
// entire batch.
func (c *Converter) Convert(ingestTs time.Time, resourceLogs []*logspb.ResourceLogs) []store.Record {
	var records []store.Record
	for _, rl := range resourceLogs {
		res := extractResource(rl.GetResource())
		for _, sl := range rl.GetScopeLogs() {
			scope := extractScope(sl.GetScope())
			for _, lr := range sl.GetLogRecords() {
				records = append(records, c.convertOne(lr, res, scope, ingestTs))
			}
		}
	}
	return records
}

// resourceInfo is the extracted-and-promoted view of a Resource.
// `remaining` is everything that wasn't lifted into a first-class
// field; it gets merged into each record's attrs so filtering and
// display work uniformly.
type resourceInfo struct {
	serviceName     string
	serviceVersion  string
	serviceInstance string
	remaining       map[string]any
}

func extractResource(r *resourcepb.Resource) resourceInfo {
	info := resourceInfo{remaining: map[string]any{}}
	if r == nil {
		return info
	}
	for _, kv := range r.GetAttributes() {
		switch kv.GetKey() {
		case attrServiceName:
			info.serviceName = anyValueAsString(kv.GetValue())
		case attrServiceVersion:
			info.serviceVersion = anyValueAsString(kv.GetValue())
		case attrServiceInstance:
			info.serviceInstance = anyValueAsString(kv.GetValue())
		default:
			info.remaining[kv.GetKey()] = anyValueToAny(kv.GetValue())
		}
	}
	return info
}

// extractScope returns the instrumentation-scope name, which we use
// as Record.Logger when the record itself doesn't carry a
// code.namespace attribute.
func extractScope(s *commonpb.InstrumentationScope) string {
	if s == nil {
		return ""
	}
	return s.GetName()
}

func (c *Converter) convertOne(lr *logspb.LogRecord, res resourceInfo, scope string, ingestTs time.Time) store.Record {
	// OTel defines both time_unix_nano (client-observed event time)
	// and observed_time_unix_nano (SDK receive time). Prefer event
	// time; fall back to observed; fall back to ingest.
	timeNs := int64(lr.GetTimeUnixNano())
	if timeNs == 0 {
		timeNs = int64(lr.GetObservedTimeUnixNano())
	}
	if timeNs == 0 {
		timeNs = ingestTs.UnixNano()
	}

	// Start attributes with any resource-level leftovers. Per-record
	// attributes overlay on top, so a record can shadow a resource
	// attribute if it really wants to.
	attrs := make(map[string]any, len(res.remaining)+len(lr.GetAttributes()))
	for k, v := range res.remaining {
		attrs[k] = v
	}

	// Sift record attributes: pull exception.* into its own bundle,
	// allow code.namespace to override the instrumentation scope as
	// the logger name, leave everything else in attrs.
	logger := scope
	var exception *store.Exception
	for _, kv := range lr.GetAttributes() {
		switch kv.GetKey() {
		case attrExceptionType:
			if exception == nil {
				exception = &store.Exception{}
			}
			exception.Type = anyValueAsString(kv.GetValue())
		case attrExceptionMessage:
			if exception == nil {
				exception = &store.Exception{}
			}
			exception.Message = anyValueAsString(kv.GetValue())
		case attrExceptionStackTrace:
			if exception == nil {
				exception = &store.Exception{}
			}
			exception.StackTrace = anyValueAsString(kv.GetValue())
		case attrCodeNamespace:
			// Per-record logger overrides the scope name. Still
			// echoed in attrs for filtering.
			logger = anyValueAsString(kv.GetValue())
			attrs[kv.GetKey()] = anyValueToAny(kv.GetValue())
		default:
			attrs[kv.GetKey()] = anyValueToAny(kv.GetValue())
		}
	}
	if len(attrs) == 0 {
		attrs = nil
	}

	// TraceID / SpanID arrive as raw bytes. NewTraceID / NewSpanID
	// validate the length; a wrong-size field is dropped (the
	// record stays usable without a trace ID) and counted so that
	// persistent client-side bugs surface in /metrics instead of
	// disappearing into a slog line no one reads.
	//
	// Empty input (len == 0) is the normal "no trace context" case
	// — NewTraceID/NewSpanID return nil, nil and we don't count it.
	traceID, terr := store.NewTraceID(lr.GetTraceId())
	if terr != nil {
		c.droppedTraceIDs.Add(1)
	}
	spanID, serr := store.NewSpanID(lr.GetSpanId())
	if serr != nil {
		c.droppedSpanIDs.Add(1)
	}

	return store.Record{
		TimeNs:    timeNs,
		IngestNs:  ingestTs.UnixNano(),
		Severity:  store.Severity(int32(lr.GetSeverityNumber())),
		Service:   res.serviceName,
		Instance:  res.serviceInstance,
		Version:   res.serviceVersion,
		Logger:    logger,
		TraceID:   traceID,
		SpanID:    spanID,
		Message:   anyValueAsString(lr.GetBody()),
		Attrs:     attrs,
		Exception: exception,
	}
}

// anyValueAsString renders an OTel AnyValue as its best-effort string
// form — used when we need to store a single human-readable value
// (service name, logger name, message body, etc.).
func anyValueAsString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
	case *commonpb.AnyValue_BytesValue:
		return fmt.Sprintf("%x", x.BytesValue)
	case *commonpb.AnyValue_ArrayValue, *commonpb.AnyValue_KvlistValue:
		// Nested shapes are rare for fields we funnel through this
		// function. Fall back to %v so the info isn't lost entirely.
		return fmt.Sprintf("%v", v.GetValue())
	default:
		return ""
	}
}

// anyValueToAny converts an AnyValue into a Go value suitable for
// JSON marshaling into Record.Attrs. Preserves native types (int64,
// float64, bool, etc.) so downstream queries can filter on them
// accurately.
func anyValueToAny(v *commonpb.AnyValue) any {
	if v == nil {
		return nil
	}
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BoolValue:
		return x.BoolValue
	case *commonpb.AnyValue_IntValue:
		return x.IntValue
	case *commonpb.AnyValue_DoubleValue:
		return x.DoubleValue
	case *commonpb.AnyValue_BytesValue:
		return x.BytesValue
	case *commonpb.AnyValue_ArrayValue:
		out := make([]any, 0, len(x.ArrayValue.GetValues()))
		for _, e := range x.ArrayValue.GetValues() {
			out = append(out, anyValueToAny(e))
		}
		return out
	case *commonpb.AnyValue_KvlistValue:
		out := make(map[string]any, len(x.KvlistValue.GetValues()))
		for _, kv := range x.KvlistValue.GetValues() {
			out[kv.GetKey()] = anyValueToAny(kv.GetValue())
		}
		return out
	default:
		return nil
	}
}

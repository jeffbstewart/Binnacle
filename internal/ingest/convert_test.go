package ingest

import (
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/jeffbstewart/Binnacle/internal/store"
)

// --- helpers ---

func kv(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key: key,
		Value: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: value},
		},
	}
}

func kvInt(key string, value int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key: key,
		Value: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_IntValue{IntValue: value},
		},
	}
}

func strBody(s string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
}

// --- tests ---

func TestConvert_EmptyBatch(t *testing.T) {
	c := NewConverter()
	got := c.Convert(time.Now(), nil)
	if len(got) != 0 {
		t.Errorf("Convert(nil) = %v, want empty", got)
	}
}

func TestConvert_SingleRecordAllFields(t *testing.T) {
	eventTime := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	ingestTime := time.Date(2026, 4, 13, 12, 0, 0, 500, time.UTC)

	batch := []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			kv("service.name", "mediamanager-server"),
			kv("service.version", "v1.2.3"),
			kv("service.instance.id", "nas-01"),
			kv("host.name", "synology-ds920"),
		}},
		ScopeLogs: []*logspb.ScopeLogs{{
			Scope: &commonpb.InstrumentationScope{Name: "net.stewart.mm.Boot"},
			LogRecords: []*logspb.LogRecord{{
				TimeUnixNano:   uint64(eventTime.UnixNano()),
				SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_ERROR,
				Body:           strBody("startup failed"),
				Attributes: []*commonpb.KeyValue{
					kv("http.method", "GET"),
					kvInt("http.status_code", 500),
					kv("exception.type", "java.lang.NullPointerException"),
					kv("exception.message", "null"),
					kv("exception.stacktrace", "... stack ..."),
				},
				TraceId: []byte{
					0xaa, 0xbb, 0xcc, 0xdd, 0x11, 0x22, 0x33, 0x44,
					0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22,
				},
				SpanId: []byte{1, 2, 3, 4, 5, 6, 7, 8},
			}},
		}},
	}}

	c := NewConverter()
	got := c.Convert(ingestTime, batch)
	if len(got) != 1 {
		t.Fatalf("expected 1 record, got %d", len(got))
	}
	r := got[0]

	if r.TimeNs != eventTime.UnixNano() {
		t.Errorf("TimeNs: got %d, want %d", r.TimeNs, eventTime.UnixNano())
	}
	if r.IngestNs != ingestTime.UnixNano() {
		t.Errorf("IngestNs: got %d, want %d", r.IngestNs, ingestTime.UnixNano())
	}
	if r.Severity != store.SeverityError {
		t.Errorf("Severity: got %d, want %d", r.Severity, store.SeverityError)
	}
	if r.Service != "mediamanager-server" {
		t.Errorf("Service: got %q", r.Service)
	}
	if r.Version != "v1.2.3" {
		t.Errorf("Version: got %q", r.Version)
	}
	if r.Instance != "nas-01" {
		t.Errorf("Instance: got %q", r.Instance)
	}
	if r.Logger != "net.stewart.mm.Boot" {
		t.Errorf("Logger: got %q", r.Logger)
	}
	if r.Message != "startup failed" {
		t.Errorf("Message: got %q", r.Message)
	}
	if r.TraceID.IsZero() {
		t.Errorf("TraceID: should be set")
	}
	if r.SpanID.IsZero() {
		t.Errorf("SpanID: should be set")
	}
	if r.Exception == nil {
		t.Fatalf("Exception: should be non-nil")
	}
	if r.Exception.Type != "java.lang.NullPointerException" {
		t.Errorf("Exception.Type: got %q", r.Exception.Type)
	}
	// Resource remainder merged into attrs.
	if r.Attrs["host.name"] != "synology-ds920" {
		t.Errorf("host.name attr: got %v", r.Attrs["host.name"])
	}
	// Per-record attr preserved with native int type.
	if r.Attrs["http.status_code"] != int64(500) {
		t.Errorf("http.status_code: got %v (%T), want int64(500)", r.Attrs["http.status_code"], r.Attrs["http.status_code"])
	}
	// service.* keys promoted, NOT left in attrs.
	for _, forbidden := range []string{"service.name", "service.version", "service.instance.id"} {
		if _, leaked := r.Attrs[forbidden]; leaked {
			t.Errorf("%s should have been promoted, not kept in attrs", forbidden)
		}
	}
	// Exception attrs consumed, NOT echoed.
	for _, forbidden := range []string{"exception.type", "exception.message", "exception.stacktrace"} {
		if _, leaked := r.Attrs[forbidden]; leaked {
			t.Errorf("%s should have been promoted, not kept in attrs", forbidden)
		}
	}

	// No IDs were malformed — counters should be zero.
	if s := c.Stats(); s.DroppedTraceIDs != 0 || s.DroppedSpanIDs != 0 {
		t.Errorf("unexpected drops: %+v", s)
	}
}

func TestConvert_TimeFallbacks(t *testing.T) {
	ingestTime := time.Date(2026, 4, 13, 12, 0, 0, 500, time.UTC)
	observed := time.Date(2026, 4, 13, 12, 0, 0, 100, time.UTC)
	c := NewConverter()

	// Only observed_time set: use it.
	onlyObserved := []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			LogRecords: []*logspb.LogRecord{{
				ObservedTimeUnixNano: uint64(observed.UnixNano()),
				Body:                 strBody("m"),
			}},
		}},
	}}
	r := c.Convert(ingestTime, onlyObserved)[0]
	if r.TimeNs != observed.UnixNano() {
		t.Errorf("fallback to observed: got %d, want %d", r.TimeNs, observed.UnixNano())
	}

	// Neither time set: use ingest.
	noTime := []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			LogRecords: []*logspb.LogRecord{{Body: strBody("m")}},
		}},
	}}
	r = c.Convert(ingestTime, noTime)[0]
	if r.TimeNs != ingestTime.UnixNano() {
		t.Errorf("fallback to ingest: got %d, want %d", r.TimeNs, ingestTime.UnixNano())
	}
}

func TestConvert_CodeNamespaceOverridesScope(t *testing.T) {
	batch := []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			Scope: &commonpb.InstrumentationScope{Name: "scope.default"},
			LogRecords: []*logspb.LogRecord{{
				Body: strBody("m"),
				Attributes: []*commonpb.KeyValue{
					kv("code.namespace", "net.stewart.specific.Class"),
				},
			}},
		}},
	}}
	r := NewConverter().Convert(time.Now(), batch)[0]
	if r.Logger != "net.stewart.specific.Class" {
		t.Errorf("code.namespace should override scope: got %q", r.Logger)
	}
	if r.Attrs["code.namespace"] != "net.stewart.specific.Class" {
		t.Errorf("code.namespace not preserved in attrs: got %v", r.Attrs["code.namespace"])
	}
}

func TestConvert_FlattensNestedResourceLogs(t *testing.T) {
	batch := []*logspb.ResourceLogs{
		{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				kv("service.name", "svc-A"),
			}},
			ScopeLogs: []*logspb.ScopeLogs{
				{LogRecords: []*logspb.LogRecord{{Body: strBody("a1")}, {Body: strBody("a2")}}},
				{LogRecords: []*logspb.LogRecord{{Body: strBody("a3")}, {Body: strBody("a4")}}},
			},
		},
		{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				kv("service.name", "svc-B"),
			}},
			ScopeLogs: []*logspb.ScopeLogs{
				{LogRecords: []*logspb.LogRecord{{Body: strBody("b1")}, {Body: strBody("b2")}}},
				{LogRecords: []*logspb.LogRecord{{Body: strBody("b3")}, {Body: strBody("b4")}}},
			},
		},
	}
	got := NewConverter().Convert(time.Now(), batch)
	if len(got) != 8 {
		t.Fatalf("expected 8 records, got %d", len(got))
	}
	svcCounts := map[string]int{}
	for _, r := range got {
		svcCounts[r.Service]++
	}
	if svcCounts["svc-A"] != 4 || svcCounts["svc-B"] != 4 {
		t.Errorf("service distribution: got %v, want {svc-A: 4, svc-B: 4}", svcCounts)
	}
}

func TestAnyValue_TypePreservation(t *testing.T) {
	batch := []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			LogRecords: []*logspb.LogRecord{{
				Body: strBody("m"),
				Attributes: []*commonpb.KeyValue{
					kv("k.str", "s"),
					kvInt("k.int", 42),
					{
						Key:   "k.bool",
						Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}},
					},
					{
						Key:   "k.double",
						Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 3.14}},
					},
				},
			}},
		}},
	}}
	r := NewConverter().Convert(time.Now(), batch)[0]
	if v, ok := r.Attrs["k.str"].(string); !ok || v != "s" {
		t.Errorf("k.str: got %v (%T)", r.Attrs["k.str"], r.Attrs["k.str"])
	}
	if v, ok := r.Attrs["k.int"].(int64); !ok || v != 42 {
		t.Errorf("k.int: got %v (%T), want int64(42)", r.Attrs["k.int"], r.Attrs["k.int"])
	}
	if v, ok := r.Attrs["k.bool"].(bool); !ok || !v {
		t.Errorf("k.bool: got %v (%T)", r.Attrs["k.bool"], r.Attrs["k.bool"])
	}
	if v, ok := r.Attrs["k.double"].(float64); !ok || v != 3.14 {
		t.Errorf("k.double: got %v (%T), want 3.14", r.Attrs["k.double"], r.Attrs["k.double"])
	}
}

// --- counter behavior ---

func TestConvert_MalformedTraceIDIsCounted(t *testing.T) {
	// 5-byte trace ID is malformed. The rest of the record should
	// still be converted; the counter bumps by one.
	batch := []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			LogRecords: []*logspb.LogRecord{{
				Body:    strBody("m"),
				TraceId: []byte{1, 2, 3, 4, 5},
			}},
		}},
	}}
	c := NewConverter()
	r := c.Convert(time.Now(), batch)[0]
	if !r.TraceID.IsZero() {
		t.Errorf("malformed TraceID should have been dropped, got %v", r.TraceID)
	}
	if r.Message != "m" {
		t.Errorf("record should still have its body: got %q", r.Message)
	}
	if s := c.Stats(); s.DroppedTraceIDs != 1 {
		t.Errorf("DroppedTraceIDs: got %d, want 1", s.DroppedTraceIDs)
	}
	if s := c.Stats(); s.DroppedSpanIDs != 0 {
		t.Errorf("DroppedSpanIDs: got %d, want 0", s.DroppedSpanIDs)
	}
}

func TestConvert_MalformedSpanIDIsCounted(t *testing.T) {
	batch := []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			LogRecords: []*logspb.LogRecord{{
				Body:   strBody("m"),
				SpanId: []byte{1, 2, 3},
			}},
		}},
	}}
	c := NewConverter()
	r := c.Convert(time.Now(), batch)[0]
	if !r.SpanID.IsZero() {
		t.Errorf("malformed SpanID should have been dropped")
	}
	if s := c.Stats(); s.DroppedSpanIDs != 1 {
		t.Errorf("DroppedSpanIDs: got %d, want 1", s.DroppedSpanIDs)
	}
}

func TestConvert_EmptyTraceIDIsNotCounted(t *testing.T) {
	// Zero-length trace ID is the normal "no trace context" case,
	// not a malformed ID. Must not bump the counter.
	batch := []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			LogRecords: []*logspb.LogRecord{{
				Body:    strBody("m"),
				TraceId: nil,
				SpanId:  nil,
			}},
		}},
	}}
	c := NewConverter()
	_ = c.Convert(time.Now(), batch)
	if s := c.Stats(); s.DroppedTraceIDs != 0 || s.DroppedSpanIDs != 0 {
		t.Errorf("empty IDs should not count as drops: %+v", s)
	}
}

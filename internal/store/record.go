package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// Severity is Binnacle's internal severity representation. Numerically
// identical to OTel's SeverityNumber (1–24) so the ingest layer can
// convert with a single `store.Severity(otlp.SeverityNumber)` cast at
// zero runtime cost — but defined in this package so the storage
// layer does not depend on OTel's proto types.
//
// The labeled constants correspond to the floor of each OTel range;
// SeverityLabel() rounds any value to the appropriate label.
type Severity int32

const (
	SeverityUnspecified Severity = 0
	SeverityTrace       Severity = 1
	SeverityDebug       Severity = 5
	SeverityInfo        Severity = 9
	SeverityWarn        Severity = 13
	SeverityError       Severity = 17
	SeverityFatal       Severity = 21
)

// String returns the coarse-grained label (TRACE / DEBUG / INFO / WARN
// / ERROR / FATAL / UNSPECIFIED) for a severity value, matching the
// buckets in README.md.
func (s Severity) String() string {
	switch {
	case s >= SeverityFatal:
		return "FATAL"
	case s >= SeverityError:
		return "ERROR"
	case s >= SeverityWarn:
		return "WARN"
	case s >= SeverityInfo:
		return "INFO"
	case s >= SeverityDebug:
		return "DEBUG"
	case s >= SeverityTrace:
		return "TRACE"
	default:
		return "UNSPECIFIED"
	}
}

// ParseSeverity turns a label back into a representative Severity
// value (the floor of its range). Accepts common casing variants;
// unknown labels return SeverityUnspecified.
func ParseSeverity(s string) Severity {
	switch s {
	case "TRACE", "trace":
		return SeverityTrace
	case "DEBUG", "debug":
		return SeverityDebug
	case "INFO", "info":
		return SeverityInfo
	case "WARN", "warn", "WARNING", "warning":
		return SeverityWarn
	case "ERROR", "error":
		return SeverityError
	case "FATAL", "fatal":
		return SeverityFatal
	default:
		return SeverityUnspecified
	}
}

// MarshalJSON emits the severity as a string label ("ERROR") rather
// than as the raw number. Consumers — humans and agents both — want
// to see words, not magic integers. The raw number is still preserved
// in the database so queries can use severity ranges.
func (s Severity) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

// UnmarshalJSON accepts either a JSON string label ("ERROR") or a
// JSON number (17) so query-param deserialization can be forgiving.
func (s *Severity) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		*s = SeverityUnspecified
		return nil
	}
	if trimmed[0] == '"' {
		var label string
		if err := json.Unmarshal(trimmed, &label); err != nil {
			return err
		}
		*s = ParseSeverity(label)
		return nil
	}
	// Numeric form.
	n, err := strconv.ParseInt(string(trimmed), 10, 32)
	if err != nil {
		return fmt.Errorf("severity: not a label or number: %q", string(trimmed))
	}
	*s = Severity(int32(n))
	return nil
}

// Record is Binnacle's internal, storage-ready shape for one log
// entry. Wire formats (OTLP protobuf, OTLP JSON) convert in and out of
// this type; storage never sees an OTel-specific struct.
//
// Time fields are `int64` nanoseconds-since-epoch because:
//
//   - OTel wire format is `fixed64 time_unix_nano`; ingest avoids an
//     allocation per record by not going through time.Time.
//   - SQLite stores INTEGER natively in 1-8 bytes; our index is on
//     int64, no conversion at query time.
//   - Memory footprint at partition scale matters (8 bytes vs 24 for
//     time.Time).
//
// The user-facing query response type (QueriedRecord) does use
// time.Time so JSON output is ISO-8601, not a pile of nanoseconds.
type Record struct {
	TimeNs    int64    // client-reported timestamp
	IngestNs  int64    // set on arrival by the collector
	Severity  Severity // typed enum
	Service   string   // service.name from Resource attributes
	Instance  string   // service.instance.id
	Version   string   // service.version
	Logger    string   // InstrumentationScope.name, or code.namespace attr
	TraceID   TraceID  // 16 bytes, or nil when unset
	SpanID    SpanID   // 8 bytes, or nil when unset
	Message   string   // LogRecord.body rendered as a string
	Attrs     map[string]any
	Exception *Exception // non-nil if exception.* attrs were present
}

// Exception captures the three standard OTel exception.* attributes
// as a single stored blob so queries can surface them as a unit.
type Exception struct {
	Type       string `json:"type,omitempty"`
	Message    string `json:"message,omitempty"`
	StackTrace string `json:"stacktrace,omitempty"`
}

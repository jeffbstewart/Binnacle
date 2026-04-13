package store

import (
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// TraceID is a 16-byte OpenTelemetry trace identifier. Internal
// representation is a []byte to match both the OTLP wire format
// (`bytes trace_id = ...`) and SQLite's BLOB column type, avoiding
// conversion on the hot read/write paths.
//
// External representations:
//
//   - JSON: lowercase hex string, or an empty string if the trace ID
//     is not set. The containing struct fields use `omitempty` so an
//     empty trace ID simply doesn't appear in query responses.
//   - SQL:  BLOB, or NULL when unset. TraceID implements Valuer and
//     Scanner so database/sql handles it directly — no nullable-
//     wrapper gymnastics at the call site.
//
// Construction is validated. Callers build a TraceID via NewTraceID,
// through UnmarshalJSON, or through Scan; each enforces the 16-byte
// length. A bare assignment like `var t TraceID = []byte{1}` compiles
// but is outside the validation funnel and considered a programmer
// error.
type TraceID []byte

// TraceIDSize is the byte length of a valid TraceID, per the W3C
// trace-context and OpenTelemetry specs.
const TraceIDSize = 16

// NewTraceID validates b and returns a defensive copy. Empty input
// returns nil (the "not set" sentinel). Length-mismatched input
// returns an error.
func NewTraceID(b []byte) (TraceID, error) {
	if len(b) == 0 {
		return nil, nil
	}
	if len(b) != TraceIDSize {
		return nil, fmt.Errorf("trace_id: expected %d bytes, got %d", TraceIDSize, len(b))
	}
	out := make(TraceID, TraceIDSize)
	copy(out, b)
	return out, nil
}

// IsZero reports whether the TraceID is unset.
func (t TraceID) IsZero() bool { return len(t) == 0 }

// String returns the lowercase-hex representation, or "" when unset.
// Suitable for human-readable logs.
func (t TraceID) String() string {
	if t.IsZero() {
		return ""
	}
	return hex.EncodeToString(t)
}

// MarshalJSON emits `""` when unset (combined with omitempty, the
// field vanishes) or a quoted lowercase-hex string otherwise.
func (t TraceID) MarshalJSON() ([]byte, error) {
	if t.IsZero() {
		return []byte(`""`), nil
	}
	return []byte(`"` + hex.EncodeToString(t) + `"`), nil
}

// UnmarshalJSON accepts a JSON string ("aabbcc…") or null. Empty
// strings decode to the zero TraceID; otherwise the string must be
// exactly TraceIDSize*2 hex characters.
func (t *TraceID) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*t = nil
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	return t.setFromHex(s)
}

func (t *TraceID) setFromHex(s string) error {
	if s == "" {
		*t = nil
		return nil
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return fmt.Errorf("trace_id: %w", err)
	}
	if len(b) != TraceIDSize {
		return fmt.Errorf("trace_id: expected %d bytes decoded, got %d", TraceIDSize, len(b))
	}
	*t = b
	return nil
}

// Scan implements sql.Scanner. Accepts nil → zero value, or a
// []byte / string of exactly TraceIDSize bytes.
func (t *TraceID) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*t = nil
		return nil
	case []byte:
		if len(v) == 0 {
			*t = nil
			return nil
		}
		if len(v) != TraceIDSize {
			return fmt.Errorf("trace_id: scan expected %d bytes, got %d", TraceIDSize, len(v))
		}
		out := make(TraceID, TraceIDSize)
		copy(out, v)
		*t = out
		return nil
	case string:
		return t.setFromHex(v)
	default:
		return fmt.Errorf("trace_id: unsupported scan source type %T", src)
	}
}

// Value implements driver.Valuer. Zero value becomes SQL NULL; any
// set value becomes a BLOB of exactly TraceIDSize bytes.
func (t TraceID) Value() (driver.Value, error) {
	if t.IsZero() {
		return nil, nil
	}
	return []byte(t), nil
}

// ---------- SpanID ----------

// SpanID is an 8-byte OpenTelemetry span identifier. Same
// implementation story as TraceID — see that type's doc for the
// rationale.
type SpanID []byte

// SpanIDSize is the byte length of a valid SpanID per the W3C
// trace-context and OpenTelemetry specs.
const SpanIDSize = 8

// NewSpanID validates b and returns a defensive copy.
func NewSpanID(b []byte) (SpanID, error) {
	if len(b) == 0 {
		return nil, nil
	}
	if len(b) != SpanIDSize {
		return nil, fmt.Errorf("span_id: expected %d bytes, got %d", SpanIDSize, len(b))
	}
	out := make(SpanID, SpanIDSize)
	copy(out, b)
	return out, nil
}

// IsZero reports whether the SpanID is unset.
func (s SpanID) IsZero() bool { return len(s) == 0 }

// String returns the lowercase-hex representation, or "" when unset.
func (s SpanID) String() string {
	if s.IsZero() {
		return ""
	}
	return hex.EncodeToString(s)
}

// MarshalJSON emits `""` when unset or a quoted lowercase-hex string.
func (s SpanID) MarshalJSON() ([]byte, error) {
	if s.IsZero() {
		return []byte(`""`), nil
	}
	return []byte(`"` + hex.EncodeToString(s) + `"`), nil
}

// UnmarshalJSON accepts a JSON string or null. Must be SpanIDSize*2
// hex characters when non-empty.
func (s *SpanID) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*s = nil
		return nil
	}
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	return s.setFromHex(str)
}

func (s *SpanID) setFromHex(str string) error {
	if str == "" {
		*s = nil
		return nil
	}
	b, err := hex.DecodeString(str)
	if err != nil {
		return fmt.Errorf("span_id: %w", err)
	}
	if len(b) != SpanIDSize {
		return fmt.Errorf("span_id: expected %d bytes decoded, got %d", SpanIDSize, len(b))
	}
	*s = b
	return nil
}

// Scan implements sql.Scanner.
func (s *SpanID) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*s = nil
		return nil
	case []byte:
		if len(v) == 0 {
			*s = nil
			return nil
		}
		if len(v) != SpanIDSize {
			return fmt.Errorf("span_id: scan expected %d bytes, got %d", SpanIDSize, len(v))
		}
		out := make(SpanID, SpanIDSize)
		copy(out, v)
		*s = out
		return nil
	case string:
		return s.setFromHex(v)
	default:
		return fmt.Errorf("span_id: unsupported scan source type %T", src)
	}
}

// Value implements driver.Valuer.
func (s SpanID) Value() (driver.Value, error) {
	if s.IsZero() {
		return nil, nil
	}
	return []byte(s), nil
}

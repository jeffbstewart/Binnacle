package store

import (
	"encoding/json"
	"testing"
)

var (
	sample16 = []byte{
		0xaa, 0xbb, 0xcc, 0xdd, 0x11, 0x22, 0x33, 0x44,
		0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22,
	}
	sample16Hex = "aabbccdd112233445566778899001122"

	sample8    = []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	sample8Hex = "0102030405060708"
)

func TestNewTraceID_Validation(t *testing.T) {
	if got, err := NewTraceID(nil); err != nil || got != nil {
		t.Errorf("NewTraceID(nil): got (%v, %v), want (nil, nil)", got, err)
	}
	if got, err := NewTraceID([]byte{}); err != nil || got != nil {
		t.Errorf("NewTraceID(empty): got (%v, %v), want (nil, nil)", got, err)
	}
	if got, err := NewTraceID(sample16); err != nil || len(got) != TraceIDSize {
		t.Errorf("NewTraceID(16 bytes): got (%v, %v)", got, err)
	}
	if _, err := NewTraceID([]byte{1, 2, 3}); err == nil {
		t.Errorf("NewTraceID(3 bytes) should error")
	}
	// Defensive copy: mutating the input does not mutate the ID.
	in := append([]byte(nil), sample16...)
	out, _ := NewTraceID(in)
	in[0] = 0xff
	if out[0] == 0xff {
		t.Errorf("NewTraceID did not copy; shared backing array with caller")
	}
}

func TestTraceID_JSONRoundTrip(t *testing.T) {
	tid, err := NewTraceID(sample16)
	if err != nil {
		t.Fatalf("NewTraceID: %v", err)
	}
	data, err := json.Marshal(tid)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `"` + sample16Hex + `"`
	if string(data) != want {
		t.Fatalf("Marshal: got %s, want %s", string(data), want)
	}

	var got TraceID
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.String() != sample16Hex {
		t.Fatalf("round-trip mismatch: got %s, want %s", got.String(), sample16Hex)
	}
}

func TestTraceID_ZeroMarshalsEmpty(t *testing.T) {
	var tid TraceID
	data, err := json.Marshal(tid)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(data) != `""` {
		t.Fatalf("zero TraceID marshal: got %s, want \"\"", string(data))
	}
}

func TestTraceID_UnmarshalErrors(t *testing.T) {
	var tid TraceID
	if err := json.Unmarshal([]byte(`"zz"`), &tid); err == nil {
		t.Errorf("non-hex input should error")
	}
	if err := json.Unmarshal([]byte(`"aabbcc"`), &tid); err == nil {
		t.Errorf("wrong-length hex should error")
	}
}

func TestTraceID_Scan(t *testing.T) {
	var tid TraceID

	if err := tid.Scan(nil); err != nil || !tid.IsZero() {
		t.Errorf("Scan(nil): want zero, got %v (err %v)", tid, err)
	}
	if err := tid.Scan([]byte{}); err != nil || !tid.IsZero() {
		t.Errorf("Scan(empty bytes): want zero, got %v (err %v)", tid, err)
	}
	if err := tid.Scan(sample16); err != nil || tid.String() != sample16Hex {
		t.Errorf("Scan(16 bytes): got %s (err %v)", tid.String(), err)
	}
	if err := tid.Scan([]byte{1, 2, 3}); err == nil {
		t.Errorf("Scan(3 bytes) should error")
	}
	if err := tid.Scan(42); err == nil {
		t.Errorf("Scan(int) should error")
	}
}

func TestTraceID_Value(t *testing.T) {
	var tid TraceID
	v, err := tid.Value()
	if err != nil || v != nil {
		t.Errorf("zero Value: got (%v, %v), want (nil, nil)", v, err)
	}

	tid, _ = NewTraceID(sample16)
	v, err = tid.Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	b, ok := v.([]byte)
	if !ok {
		t.Fatalf("Value: expected []byte, got %T", v)
	}
	if len(b) != TraceIDSize {
		t.Errorf("Value: expected %d bytes, got %d", TraceIDSize, len(b))
	}
}

// SpanID shares the implementation shape; one lighter round-trip
// test confirms the 8-byte type behaves analogously.
func TestSpanID_Basics(t *testing.T) {
	sid, err := NewSpanID(sample8)
	if err != nil || sid.String() != sample8Hex {
		t.Fatalf("NewSpanID: got %s, err %v", sid.String(), err)
	}
	if _, err := NewSpanID([]byte{1, 2, 3}); err == nil {
		t.Errorf("NewSpanID(3 bytes) should error")
	}
	data, err := json.Marshal(sid)
	if err != nil || string(data) != `"`+sample8Hex+`"` {
		t.Errorf("SpanID Marshal: got %s, err %v", string(data), err)
	}
	var round SpanID
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if round.String() != sid.String() {
		t.Errorf("round-trip mismatch")
	}
}

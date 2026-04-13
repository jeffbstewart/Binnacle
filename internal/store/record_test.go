package store

import (
	"encoding/json"
	"testing"
)

func TestSeverityString(t *testing.T) {
	cases := []struct {
		in   Severity
		want string
	}{
		{SeverityUnspecified, "UNSPECIFIED"},
		{SeverityTrace, "TRACE"},
		{SeverityDebug, "DEBUG"},
		{SeverityInfo, "INFO"},
		{SeverityWarn, "WARN"},
		{SeverityError, "ERROR"},
		{SeverityFatal, "FATAL"},
		{Severity(3), "TRACE"},  // floor of TRACE range
		{Severity(12), "INFO"},  // top of INFO range
		{Severity(16), "WARN"},  // top of WARN range
		{Severity(20), "ERROR"}, // top of ERROR range
		{Severity(24), "FATAL"},
	}
	for _, c := range cases {
		if got := c.in.String(); got != c.want {
			t.Errorf("Severity(%d).String() = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestParseSeverity(t *testing.T) {
	cases := []struct {
		in   string
		want Severity
	}{
		{"ERROR", SeverityError},
		{"error", SeverityError},
		{"WARN", SeverityWarn},
		{"warning", SeverityWarn},
		{"", SeverityUnspecified},
		{"invalid", SeverityUnspecified},
	}
	for _, c := range cases {
		if got := ParseSeverity(c.in); got != c.want {
			t.Errorf("ParseSeverity(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestSeverityJSON(t *testing.T) {
	// Marshal emits the label string.
	b, err := json.Marshal(SeverityError)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != `"ERROR"` {
		t.Fatalf("marshal got %s, want \"ERROR\"", string(b))
	}

	// Unmarshal accepts label strings.
	var s Severity
	if err := json.Unmarshal([]byte(`"WARN"`), &s); err != nil {
		t.Fatalf("unmarshal label: %v", err)
	}
	if s != SeverityWarn {
		t.Fatalf("unmarshal label got %d, want %d", s, SeverityWarn)
	}

	// Unmarshal also accepts raw numbers.
	if err := json.Unmarshal([]byte(`17`), &s); err != nil {
		t.Fatalf("unmarshal number: %v", err)
	}
	if s != SeverityError {
		t.Fatalf("unmarshal number got %d, want %d", s, SeverityError)
	}

	// Round-trip through a struct tag.
	type holder struct {
		Sev Severity `json:"sev"`
	}
	h := holder{Sev: SeverityInfo}
	b, err = json.Marshal(h)
	if err != nil {
		t.Fatalf("marshal holder: %v", err)
	}
	if string(b) != `{"sev":"INFO"}` {
		t.Fatalf("holder marshal got %s", string(b))
	}
	var got holder
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal holder: %v", err)
	}
	if got.Sev != SeverityInfo {
		t.Fatalf("holder round-trip got %d, want %d", got.Sev, SeverityInfo)
	}
}

func TestSeverityOTelNumericCompatibility(t *testing.T) {
	// OTel's SeverityNumber is a typed int32. Our Severity is also
	// int32 under the hood, so an ingest-time cast (without conversion
	// cost) should preserve the numeric value. This test locks in that
	// promise.
	for _, n := range []int32{0, 1, 5, 9, 13, 17, 21, 24} {
		s := Severity(n)
		if int32(s) != n {
			t.Errorf("Severity(%d) != %d after cast", n, int32(s))
		}
	}
}

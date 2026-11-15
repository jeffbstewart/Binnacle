package store

import (
	"testing"
	"time"
)

func TestFtsMatchExpr(t *testing.T) {
	for in, want := range map[string]string{
		"":                ``,
		"   ":             ``,
		"disk":            `"disk"`,
		"disk  full":      `"disk" "full"`,
		`say "hi"`:        `"say" """hi"""`,
		"NEAR(a b) OR c*": `"NEAR(a" "b)" "OR" "c*"`,
	} {
		if got := ftsMatchExpr(in); got != want {
			t.Errorf("ftsMatchExpr(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestQuery_Search(t *testing.T) {
	db, w := openWithWriter(t)
	now := time.Now()
	msgs := []string{
		"disk full on /dev/sda",
		"network timeout talking to registry",
		`client sent "weird" input`,
	}
	for i, m := range msgs {
		if err := w.Submit(Record{TimeNs: now.UnixNano() + int64(i), IngestNs: now.UnixNano(),
			Severity: SeverityInfo, Service: "svc", Instance: "h", Message: m}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	mustWrite(t, w, uint64(len(msgs)))

	for search, want := range map[string]int{
		"disk":         1,
		"DISK sda":     1, // terms ANDed, case-insensitive
		"disk network": 0,
		"dev/sda":      1, // tokenized into the phrase "dev sda"
		`"weird`:       1, // stray quote is not a syntax error
		"NEAR( OR":     0, // FTS5 operators are inert
		"":             3,
	} {
		res, err := Query(t.Context(), db, QueryFilter{Search: search})
		if err != nil {
			t.Fatalf("Search %q: %v", search, err)
		}
		if res.CountReturned != want {
			t.Errorf("Search %q: %d records, want %d", search, res.CountReturned, want)
		}
	}
}

func TestVacuumIfFragmented(t *testing.T) {
	db, w := openWithWriter(t)
	now := time.Now()
	// Fill a partition with enough rows to free a meaningful number of
	// pages when it is dropped.
	big := make([]byte, 2000)
	for i := range big {
		big[i] = 'x'
	}
	const n = 500
	for i := range n {
		if err := w.Submit(Record{TimeNs: now.UnixNano() + int64(i), IngestNs: now.UnixNano(),
			Severity: SeverityInfo, Service: "svc", Instance: "h", Message: string(big)}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		if i%50 == 49 {
			mustWrite(t, w, uint64(i+1))
		}
	}
	mustWrite(t, w, n)
	if err := DropPartition(t.Context(), db, dayKey(now)); err != nil {
		t.Fatalf("DropPartition: %v", err)
	}

	ran, before, _, err := VacuumIfFragmented(t.Context(), db, 0.99, 0)
	if err != nil || ran {
		t.Fatalf("below threshold: ran=%v err=%v (free fraction %.2f)", ran, err, before.FreeFraction())
	}

	ran, before, after, err := VacuumIfFragmented(t.Context(), db, 0.10, 0)
	if err != nil || !ran {
		t.Fatalf("above threshold: ran=%v err=%v (free fraction %.2f)", ran, err, before.FreeFraction())
	}
	if after.FreePages != 0 || after.Bytes() >= before.Bytes() {
		t.Fatalf("vacuum did not reclaim: before=%+v after=%+v", before, after)
	}
}

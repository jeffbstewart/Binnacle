package store

import (
	"slices"
	"testing"
	"time"
)

func TestExpiredPartitionDays(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	days := []string{"2026-05-01", "2026-11-07", "2026-11-08", "2026-11-15", "garbage"}

	got := ExpiredPartitionDays(days, 7*24*time.Hour, now)
	// Cutoff is 2026-11-08T12:00Z. 11-07 ends at 11-08T00:00 (all
	// older than cutoff) so it goes; 11-08 still holds in-window
	// records, so it stays.
	want := []string{"2026-05-01", "2026-11-07"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEnforceRetention_DropsOnlyExpired(t *testing.T) {
	db, _ := openWithWriter(t)
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	for _, d := range []string{"2026-05-01", "2026-11-07", "2026-11-14", "2026-11-15"} {
		day, _ := time.Parse("2006-01-02", d)
		if err := ensurePartition(t.Context(), db, day); err != nil {
			t.Fatalf("ensurePartition %s: %v", d, err)
		}
		if _, err := db.Exec(`INSERT INTO partition_services (day, service) VALUES (?, ?)`, d, "svc-"+d); err != nil {
			t.Fatalf("seed partition_services: %v", err)
		}
	}

	dropped, err := EnforceRetention(t.Context(), db, 7*24*time.Hour, now)
	if err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}
	if want := []string{"2026-05-01", "2026-11-07"}; !slices.Equal(dropped, want) {
		t.Fatalf("dropped %v, want %v", dropped, want)
	}

	days, _ := ListPartitionDays(t.Context(), db)
	if want := []string{"2026-11-14", "2026-11-15"}; !slices.Equal(days, want) {
		t.Fatalf("remaining partitions %v, want %v", days, want)
	}
	svcs, _ := ListDistinctServices(t.Context(), db)
	if want := []string{"svc-2026-11-14", "svc-2026-11-15"}; !slices.Equal(svcs, want) {
		t.Fatalf("remaining services %v, want %v", svcs, want)
	}

	// A second sweep has nothing to do.
	dropped, err = EnforceRetention(t.Context(), db, 7*24*time.Hour, now)
	if err != nil || len(dropped) != 0 {
		t.Fatalf("second sweep: dropped %v, err %v", dropped, err)
	}
}

func TestWriter_IndexesServices(t *testing.T) {
	db, w := openWithWriter(t)
	now := time.Now()
	for i, svc := range []string{"beta", "alpha", "beta", "alpha", "gamma"} {
		if err := w.Submit(Record{TimeNs: now.UnixNano() + int64(i), IngestNs: now.UnixNano(),
			Severity: SeverityInfo, Service: svc, Instance: "h", Message: "m"}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	mustWrite(t, w, 5)

	svcs, err := ListDistinctServices(t.Context(), db)
	if err != nil {
		t.Fatalf("ListDistinctServices: %v", err)
	}
	if want := []string{"alpha", "beta", "gamma"}; !slices.Equal(svcs, want) {
		t.Fatalf("services %v, want %v", svcs, want)
	}
}

// TestBackfillServiceIndex simulates a partition created before V002:
// log rows exist but partition_services is empty and the flag is 0.
func TestBackfillServiceIndex(t *testing.T) {
	db, w := openWithWriter(t)
	now := time.Now()
	for i, svc := range []string{"one", "two"} {
		if err := w.Submit(Record{TimeNs: now.UnixNano() + int64(i), IngestNs: now.UnixNano(),
			Severity: SeverityInfo, Service: svc, Instance: "h", Message: "m"}); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	mustWrite(t, w, 2)
	if _, err := db.Exec(`DELETE FROM partition_services`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE partitions SET services_indexed = 0`); err != nil {
		t.Fatal(err)
	}

	n, err := BackfillServiceIndex(t.Context(), db)
	if err != nil || n != 1 {
		t.Fatalf("BackfillServiceIndex: n=%d err=%v, want 1 partition", n, err)
	}
	svcs, _ := ListDistinctServices(t.Context(), db)
	if want := []string{"one", "two"}; !slices.Equal(svcs, want) {
		t.Fatalf("services %v, want %v", svcs, want)
	}
	if n, err := BackfillServiceIndex(t.Context(), db); err != nil || n != 0 {
		t.Fatalf("second backfill: n=%d err=%v, want 0", n, err)
	}
}

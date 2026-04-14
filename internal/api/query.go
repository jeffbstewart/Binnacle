// Package api houses the agent- and human-facing HTTP handlers
// mounted on Binnacle's query port. The read path is unauthenticated
// by design (see README § Security Model); the write path lives in
// package ingest and is gated separately.
package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/jeffbstewart/Binnacle/internal/store"
)

// QueryPath is the canonical path for the primary structured query
// endpoint. Kept as a constant so handler registration, tests, and
// documentation all agree.
const QueryPath = "/api/logs/query"

// defaultLimit mirrors store.Query's default. Declared here as well
// so the handler's docstring can cite a single number.
const defaultLimit = 100

// QueryHandler turns URL query parameters into a store.QueryFilter,
// runs the query, and emits JSON. All parsing errors map to 400 so
// clients (agents, humans, the future Angular UI) get structured
// feedback on what they got wrong.
//
// Supported parameters:
//
//	service=<name>         exact match; repeatable (OR filter)
//	severity=<label|num>   INFO / WARN / ERROR / ... or a raw number
//	since=<RFC3339>        time_ns >= Since.UnixNano()
//	until=<RFC3339>        time_ns <  Until.UnixNano()
//	limit=<int>            default 100, hard-capped at 1000 in store
type QueryHandler struct {
	DB *sql.DB
}

// ServeHTTP implements http.Handler.
func (h *QueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	filter, err := parseQueryFilter(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := store.Query(h.DB, filter)
	if err != nil {
		http.Error(w, "query: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		// The response is already partly written at this point, so
		// there's not much we can do other than note it for logs.
		return
	}
}

// parseQueryFilter converts URL query parameters into a QueryFilter.
// Returns a user-friendly error for bad input (time not RFC3339,
// limit not an int, etc.) so the handler can surface it with 400.
func parseQueryFilter(r *http.Request) (store.QueryFilter, error) {
	q := r.URL.Query()

	filter := store.QueryFilter{
		Services: q["service"],
	}

	if s := q.Get("severity"); s != "" {
		sev := store.ParseSeverity(s)
		if sev == store.SeverityUnspecified {
			// Not a known label — try a raw number. Agents may prefer
			// either; we accept both.
			n, err := strconv.Atoi(s)
			if err != nil {
				return filter, fmt.Errorf("severity: %q is not a known label or integer", s)
			}
			sev = store.Severity(int32(n))
		}
		filter.MinSeverity = sev
	}

	if s := q.Get("since"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return filter, fmt.Errorf("since: %w (want RFC3339, e.g. 2026-04-13T12:00:00Z)", err)
		}
		filter.Since = t
	}
	if s := q.Get("until"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return filter, fmt.Errorf("until: %w (want RFC3339, e.g. 2026-04-13T12:00:00Z)", err)
		}
		filter.Until = t
	}

	if s := q.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			return filter, fmt.Errorf("limit: %w", err)
		}
		if n < 0 {
			return filter, fmt.Errorf("limit: must be >= 0, got %d", n)
		}
		filter.Limit = n
	}

	return filter, nil
}

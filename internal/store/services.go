package store

import (
	"database/sql"
	"fmt"
	"time"
)

// ListDistinctServices returns the unique service names across all
// partitions, sorted alphabetically. Used to populate the UI filter
// dropdown. The query is fast — one UNION per day with DISTINCT, at
// most 7 subqueries at default retention.
func ListDistinctServices(db *sql.DB) ([]string, error) {
	days, err := ListPartitionDays(db)
	if err != nil {
		return nil, err
	}
	if len(days) == 0 {
		return nil, nil
	}

	var parts []string
	for _, day := range days {
		t, err := time.Parse("2006-01-02", day)
		if err != nil {
			continue
		}
		parts = append(parts, fmt.Sprintf(
			`SELECT DISTINCT service FROM %s`, PartitionName(t),
		))
	}

	query := `SELECT DISTINCT service FROM (` +
		joinStrings(parts, " UNION ALL ") +
		`) ORDER BY service`

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var services []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		services = append(services, s)
	}
	return services, rows.Err()
}

func joinStrings(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for _, p := range parts[1:] {
		result += sep + p
	}
	return result
}

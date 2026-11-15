package store

import (
	"context"
	"database/sql"
)

// ListDistinctServices returns the unique service names across all
// retained partitions, sorted alphabetically, for the UI filter
// dropdown. It reads the small partition_services index (V002), not
// the log tables. Partitions from before V002 contribute once the
// maintenance loop has backfilled them.
func ListDistinctServices(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT DISTINCT service FROM partition_services ORDER BY service`)
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

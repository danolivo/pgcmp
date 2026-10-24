package main

import (
	"context"
	"fmt"
)

// row is one table row as seen by the merger: the integer key and the
// server-side text of ROW(non-key columns).
type row struct {
	key  int64
	data string
}

// stream runs the query on this side and pushes rows into out in key order.
// It always closes out, so the merger can simply range until both channels
// are exhausted.
func (s *side) stream(ctx context.Context, sql string, out chan<- row) error {
	defer close(out)

	rows, err := s.tx.Query(ctx, sql)
	if err != nil {
		return fmt.Errorf("%s: query: %w", s.name, err)
	}
	defer rows.Close()

	var prev int64
	first := true
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.key, &r.data); err != nil {
			return fmt.Errorf("%s: scan: %w", s.name, err)
		}
		// Defensive: ORDER BY on a primary key guarantees a strictly
		// increasing sequence; anything else means we would merge garbage.
		if !first && r.key <= prev {
			return fmt.Errorf("%s: rows are not strictly ordered by key (%d after %d)", s.name, r.key, prev)
		}
		prev, first = r.key, false

		select {
		case out <- r:
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", s.name, ctx.Err())
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%s: read: %w", s.name, err)
	}
	return nil
}

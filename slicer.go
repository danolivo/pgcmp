package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The slicer cuts the key space of the reference table into blocks of a
// fixed number of rows, in one pass over the key index, in the snapshot of
// the coordinator transaction (issue #1).
//
// A plain cursor over the key, read in key order, skips N-1 rows at a time
// and fetches the N-th: that key is the upper bound of a block.
//
//	DECLARE pgcmp_slice NO SCROLL CURSOR FOR
//	SELECT "k1", ..., "kn" FROM t [WHERE (filter)] ORDER BY t."k1", ..., t."kn";
//	MOVE FORWARD N-1 IN pgcmp_slice; FETCH FORWARD 1 FROM pgcmp_slice;  -- per bound
//
// MOVE reads and discards: the executor advances the portal and sends
// nothing. The target list holds the bare key columns, so nothing is
// computed for the skipped rows; the one exception is a key type whose
// text does not read back (regproc), cast to its sibling (regprocedure).
//
// MOVE/FETCH pairs go in batches, several statements in one message of the
// simple query protocol: one round trip per batch, and every result comes
// back as text, the output of the column type. A batch comes back whole,
// so the first one holds a single pair (the first bound arrives after one
// block) and the size doubles up to maxBatch.
//
// A block is (previous bound, bound], on the whole key; the first is open
// below, the last open above, so test-side rows outside the reference key
// range are compared too. k bounds give k+1 blocks.
type slicer struct {
	table    string      // qualified name of the reference table
	key      []keyColumn // as the reference server sees it
	filter   string
	rows     int // N: rows in one block
	maxBatch int // MOVE/FETCH pairs in one round trip, at most
}

const sliceCursor = "pgcmp_slice"

// cursorQuery is the query of the cursor.
func (s *slicer) cursorQuery() string {
	cols := make([]string, len(s.key))
	order := make([]string, len(s.key))
	for i, k := range s.key {
		col := pgx.Identifier{k.name}.Sanitize()
		cols[i] = col
		if k.bound != k.baseType {
			cols[i] = col + "::" + k.bound
		}
		// Qualified, so that ORDER BY cannot take an output column of the
		// same name (k::regprocedure is named k): that would sort the whole
		// result and stop the scan from streaming.
		order[i] = s.table + "." + col
	}
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s FROM %s", strings.Join(cols, ", "), s.table)
	if f := strings.TrimSpace(s.filter); f != "" {
		fmt.Fprintf(&b, " WHERE (%s)", f)
	}
	fmt.Fprintf(&b, " ORDER BY %s", strings.Join(order, ", "))
	return b.String()
}

func (s *slicer) declare() string {
	return "DECLARE " + sliceCursor + " NO SCROLL CURSOR FOR " + s.cursorQuery()
}

// batch is the text of n MOVE/FETCH pairs. With N = 1 there is nothing to
// skip: MOVE FORWARD 0 would re-read the current row.
func (s *slicer) batch(n int) string {
	var b strings.Builder
	for range n {
		if s.rows > 1 {
			fmt.Fprintf(&b, "MOVE FORWARD %d IN %s; ", s.rows-1, sliceCursor)
		}
		fmt.Fprintf(&b, "FETCH FORWARD 1 FROM %s; ", sliceCursor)
	}
	return b.String()
}

// takeBounds reads the results of a batch: the bounds it found, and
// whether the cursor has reached the end. The end is a MOVE that moved
// fewer than N-1 rows, or an empty FETCH: with kN + N-1 rows the last MOVE
// is still full, and only the FETCH after it finds nothing. Results after
// the end are ignored.
func (s *slicer) takeBounds(res []*pgconn.Result) (bounds [][]string, end bool, err error) {
	for i := 0; i < len(res); {
		if s.rows > 1 {
			tag := res[i].CommandTag
			if !strings.HasPrefix(tag.String(), "MOVE") {
				return nil, false, fmt.Errorf("slicer: expected MOVE, got %q", tag)
			}
			if tag.RowsAffected() < int64(s.rows-1) {
				return bounds, true, nil
			}
			i++
			if i == len(res) {
				return nil, false, fmt.Errorf("slicer: a MOVE without its FETCH")
			}
		}
		fetch := res[i]
		i++
		if tag := fetch.CommandTag; !strings.HasPrefix(tag.String(), "FETCH") {
			return nil, false, fmt.Errorf("slicer: expected FETCH, got %q", tag)
		}
		if len(fetch.Rows) == 0 {
			return bounds, true, nil
		}
		row := fetch.Rows[0]
		if len(row) != len(s.key) {
			return nil, false, fmt.Errorf("slicer: FETCH returned %d columns, the key has %d", len(row), len(s.key))
		}
		b := make([]string, len(row))
		for j, v := range row {
			if v == nil {
				return nil, false, fmt.Errorf("slicer: NULL in key column %q", s.key[j].name)
			}
			b[j] = string(v)
		}
		bounds = append(bounds, b)
	}
	return bounds, false, nil
}

// blockAfter is the block that ends at bound hi, after the block that
// ended at lo; nil lo or hi is open.
func blockAfter(lo, hi []string) keyRange {
	var r keyRange
	if lo != nil {
		r.lo = keyBound{vals: lo}
	}
	if hi != nil {
		r.hi = keyBound{vals: hi, inclusive: true}
	}
	return r
}

// feed cuts the key space through the coordinator transaction of ref and
// sends the blocks in key order. It closes the cursor before it returns,
// so that the coordinator can join the workers (keySplit.feedHoldsCoordinator).
func (s *slicer) feed(ref *side) rangeFeed {
	return func(ctx context.Context, out chan<- keyRange) error {
		start := time.Now()
		conn := ref.conn.PgConn()
		if _, err := conn.Exec(ctx, s.declare()).ReadAll(); err != nil {
			return fmt.Errorf("%s: slicer: %w", ref.name, err)
		}
		send := func(r keyRange) error {
			select {
			case out <- r:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		var prev []string
		bounds := 0
		for n, end := 1, false; !end; n = min(n*2, s.maxBatch) {
			res, err := conn.Exec(ctx, s.batch(n)).ReadAll()
			if err != nil {
				return fmt.Errorf("%s: slicer: %w", ref.name, err)
			}
			var bs [][]string
			if bs, end, err = s.takeBounds(res); err != nil {
				return fmt.Errorf("%s: %w", ref.name, err)
			}
			for _, b := range bs {
				if err := send(blockAfter(prev, b)); err != nil {
					return err
				}
				prev = b
				bounds++
			}
		}
		if err := send(blockAfter(prev, nil)); err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, "CLOSE "+sliceCursor).ReadAll(); err != nil {
			return fmt.Errorf("%s: slicer: %w", ref.name, err)
		}
		log.Printf("slicer: %d bounds, %d blocks in %s", bounds, bounds+1, time.Since(start).Round(time.Millisecond))
		return nil
	}
}

// Rows in one block when -rows is not given: as many as fit into -mem by
// the average width of a row, within these limits; without any estimate
// of the width, defaultSliceRows.
const (
	minSliceRows     = 1000
	maxSliceRows     = 10_000_000
	defaultSliceRows = 100_000
)

// rowsForWidth is the number of rows of the given width (bytes) that fit
// into mem, clamped to [minSliceRows, maxSliceRows]. A block row costs its
// text plus rowOverhead.
func rowsForWidth(mem byteSize, width float64) int {
	n := float64(mem) / (width + rowOverhead)
	return int(min(max(n, minSliceRows), maxSliceRows))
}

// sliceRows decides N: rows if given (-rows); otherwise an estimate from
// mem (-mem) and the average width of a row of the table on side s: the
// sum of pg_stats.avg_width over cols, or, without statistics, the table
// size per row. It returns N and how it was chosen, for the log.
func sliceRows(ctx context.Context, s *side, d *description, cols []string, rows int, mem byteSize) (int, string, error) {
	if rows > 0 {
		return rows, "from -rows", nil
	}
	ref := s
	var width *float64
	var tuples float64
	var size int64
	err := ref.tx.QueryRow(ctx, `
		SELECT (SELECT sum(s.avg_width)::float8 FROM pg_catalog.pg_stats s
		        WHERE s.schemaname = $1 AND s.tablename = $2 AND NOT s.inherited
		          AND s.attname = ANY ($3)),
		       c.reltuples::float8, pg_catalog.pg_relation_size(c.oid)
		FROM pg_catalog.pg_class c WHERE c.oid = $4`,
		d.schema, d.name, cols, d.oid).Scan(&width, &tuples, &size)
	if err != nil {
		return 0, "", fmt.Errorf("%s: row width of %s: %w", ref.name, d.qualifiedName(), err)
	}
	switch {
	case width != nil:
		return rowsForWidth(mem, *width), fmt.Sprintf("estimated from -mem %s and the average row width, %.0f bytes", mem, *width), nil
	case tuples > 0:
		w := float64(size) / tuples
		return rowsForWidth(mem, w), fmt.Sprintf("estimated from -mem %s and the table size per row, %.0f bytes", mem, w), nil
	}
	return defaultSliceRows, "by default: the table has never been analysed", nil
}

// newSlicer prepares the slicer of a table on side s. cols are the
// columns whose width estimates N when rows is 0; maxBatch limits the
// MOVE/FETCH pairs of one round trip.
func newSlicer(ctx context.Context, s *side, d *description, cols []string, filter string,
	rows int, mem byteSize, maxBatch int) (*slicer, string, error) {
	key, err := keyOf(d)
	if err != nil {
		return nil, "", err
	}
	n, how, err := sliceRows(ctx, s, d, cols, rows, mem)
	if err != nil {
		return nil, "", err
	}
	return &slicer{
		table:    d.qualifiedName(),
		key:      key,
		filter:   filter,
		rows:     n,
		maxBatch: maxBatch,
	}, how, nil
}

// checkPlan explains the cursor and returns a warning if the slicer would
// not walk the key index, or "". EXPLAIN of the DECLARE plans the query as
// the cursor will run it (cursor_tuple_fraction included).
func (s *slicer) checkPlan(ctx context.Context, ref *side) (string, error) {
	plan, err := ref.explain(ctx, s.declare())
	if err != nil {
		return "", err
	}
	if shape, seq := planShape(plan); seq {
		return fmt.Sprintf("warning: %s: the slicer will read %s with a sequential scan and sort it in full (%s) "+
			"instead of walking the primary key index", ref.name, s.table, shape), nil
	}
	return "", nil
}

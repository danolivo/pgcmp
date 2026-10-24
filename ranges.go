package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Key ranges: how the key space of the table is cut into pieces that
// parallel workers compare independently. Strategies (strategy.go) compare
// one range at a time and do not know where the ranges came from.
//
// The ends of a range are key values as text. The servers parse them in the
// key column types and do all the comparisons; the client never decides
// which of two keys is smaller. An end may constrain only the first few key
// columns: a histogram bound is a value of the first key column alone.

// keyBound is one end of a key range.
type keyBound struct {
	vals      []string // text of the first len(vals) key columns; nil: open end
	inclusive bool     // whether keys equal to vals belong to the range
}

// keyRange is an interval of the key. The zero value is the whole key
// space.
type keyRange struct {
	lo, hi keyBound
}

func (r keyRange) unbounded() bool { return r.lo.vals == nil && r.hi.vals == nil }

// conditions returns the SQL conditions that restrict the key to r, none
// for the whole key space. cols are the quoted key columns in key order;
// lit renders the text v of key column i as an SQL expression.
func (r keyRange) conditions(cols []string, lit func(i int, v string) string) []string {
	var c []string
	if r.lo.vals != nil {
		op := ">"
		if r.lo.inclusive {
			op = ">="
		}
		c = append(c, keyCondition(cols, op, r.lo.vals, lit))
	}
	if r.hi.vals != nil {
		op := "<"
		if r.hi.inclusive {
			op = "<="
		}
		c = append(c, keyCondition(cols, op, r.hi.vals, lit))
	}
	return c
}

// keyCondition compares the first len(vals) key columns with vals. Several
// columns use a row comparison, which a btree index on the key serves
// directly.
func keyCondition(cols []string, op string, vals []string, lit func(i int, v string) string) string {
	if len(vals) == 1 {
		return cols[0] + " " + op + " " + lit(0, vals[0])
	}
	l := make([]string, len(vals))
	for i, v := range vals {
		l[i] = lit(i, v)
	}
	return "(" + strings.Join(cols[:len(vals)], ", ") + ") " + op + " (" + strings.Join(l, ", ") + ")"
}

// quoteLiteral quotes s as an SQL string literal; standard_conforming_strings
// is on in every session (sessionSetup).
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// keySplit is how the key space is cut for one run: either a list of
// ranges known before the run, or a feed that cuts the key space while the
// workers compare.
type keySplit struct {
	ranges []keyRange // known in advance; nil with a feed
	feed   rangeFeed  // cuts during the run; nil with a list
	slicer *slicer    // the slicer behind feed, if it is one
}

// inCoordinator reports whether the run compares one range right in the
// coordinator sessions, with no workers and no queue. Only a single range
// known in advance does; anything else goes through runParallel, where the
// coordinator pair is one of the workers.
func (s keySplit) inCoordinator() bool { return s.feed == nil && len(s.ranges) == 1 }

// feedHoldsCoordinator reports whether the ranges come from a feed that
// reads through the coordinator transaction of the reference side (the
// slicer keeps its cursor open there; a stored block map is read there).
// One connection runs one query at a time, so the coordinator pair joins
// the workers only once such a feed has returned.
func (s keySplit) feedHoldsCoordinator() bool { return s.feed != nil }

// workers is the number of workers, the coordinator pair included: no more
// than there are ranges, when that is known.
func (s keySplit) workers(jobs int) int {
	if s.feed != nil {
		return max(jobs, 1)
	}
	return min(jobs, len(s.ranges))
}

// source feeds the ranges to the workers.
func (s keySplit) source() rangeFeed {
	if s.feed != nil {
		return s.feed
	}
	return rangeList(s.ranges)
}

// planRanges are the ranges whose queries are checked with EXPLAIN before
// the run: all of them when they are known, or else the whole key space,
// which has the same plan shape as any range without its bounds.
func (s keySplit) planRanges() []keyRange {
	if s.feed != nil {
		return []keyRange{{}}
	}
	return s.ranges
}

// Ways to cut the key space, the values of -split. The value is the way
// tried first; if it cannot be used, the next one is taken.
const (
	splitHistogram = "histogram" // the histogram; the slicer if it cannot be used
	splitFixed     = "fixed"     // the slicer: blocks of a fixed number of rows
	splitNone      = "none"      // one range, no parallelism
)

// minBinsPerWorker: the histogram is used only if it has at least this many
// bins per worker. With fewer, uneven bins leave workers idle at the end.
const minBinsPerWorker = 4

// splitKeys cuts the key space for opt.jobs workers, by the priority of
// opt.split:
//
//   - histogram: with one worker, one range, the whole key space; otherwise
//     the histogram of the first key column on the reference side (see
//     histogramRanges), if it can be used (usableHistogram), and the slicer
//     if not;
//   - fixed: the slicer, with any number of workers;
//   - none: one range.
func splitKeys(ctx context.Context, opt *options, ref *side, d *description, p *plan) (keySplit, error) {
	if opt.mapName != "" {
		return mapSplit(ctx, opt, ref, d)
	}
	switch {
	case opt.split == splitNone, opt.split == splitHistogram && opt.jobs <= 1:
		return keySplit{ranges: []keyRange{{}}}, nil
	case opt.split == splitFixed:
		return sliceSplit(ctx, opt, ref, d, p, "")
	}
	rs, why := usableHistogram(ctx, opt, ref, d, p)
	if why != "" {
		return sliceSplit(ctx, opt, ref, d, p, why)
	}
	log.Printf("parallel: %d workers, %d key ranges from the histogram of %s",
		min(opt.jobs, len(rs)), len(rs), p.key[0].name)
	return keySplit{ranges: rs}, nil
}

// usableHistogram returns the ranges of the histogram of the first key
// column, or why the histogram cannot be used: there is none (the table
// has not been analysed), it cannot be read, -filter is set (the histogram
// describes the whole table, not the rows that match), or it has fewer
// than minBinsPerWorker bins per worker.
func usableHistogram(ctx context.Context, opt *options, ref *side, d *description, p *plan) ([]keyRange, string) {
	k := p.key[0]
	if strings.TrimSpace(opt.filter) != "" {
		return nil, "-filter is set, and the histogram describes the whole table"
	}
	bounds, err := ref.histogram(ctx, d, k.name, k.base[0])
	switch {
	case err != nil:
		return nil, err.Error()
	case len(bounds) < 2:
		return nil, fmt.Sprintf("no histogram for %s.%s on the reference side", d.qualifiedName(), k.name)
	}
	rs := histogramRanges(bounds)
	if len(rs) < minBinsPerWorker*opt.jobs {
		return nil, fmt.Sprintf("the histogram of %s has %d bins, fewer than %d per worker",
			k.name, len(rs), minBinsPerWorker)
	}
	return rs, ""
}

// mapSplit cuts the key space by the blocks of a map stored on the
// reference side (pgcmp slice), read through the coordinator transaction.
func mapSplit(ctx context.Context, opt *options, ref *side, d *description) (keySplit, error) {
	if opt.splitSet {
		log.Printf("warning: -split is ignored: the blocks come from -map %s", opt.mapName)
	}
	m, table, err := ref.mapMetaOf(ctx, ref.tx, opt.mapName)
	if err != nil {
		return keySplit{}, err
	}
	key, err := keyOf(d)
	if err != nil {
		return keySplit{}, err
	}
	if err := m.compatible(key, opt.filter); err != nil {
		return keySplit{}, fmt.Errorf("-map %s: %w", table, err)
	}
	if m.Table != d.qualifiedName() {
		log.Printf("warning: -map %s was cut on %s, the reference table is %s", table, m.Table, d.qualifiedName())
	}
	workers := max(opt.jobs, 1)
	log.Printf("parallel: %d workers, %d blocks of %d rows from the block map %s, cut %s ago",
		workers, m.Blocks, m.Rows, table, time.Since(m.Built).Round(time.Second))
	return keySplit{feed: mapFeed(ref, table, m.Blocks)}, nil
}

// sliceSplit cuts the key space with the slicer; why, if not empty, says
// why the histogram was passed over.
func sliceSplit(ctx context.Context, opt *options, ref *side, d *description, p *plan, why string) (keySplit, error) {
	workers := max(opt.jobs, 1)
	// The width of a block row: the key and the compared columns.
	cols := make([]string, 0, len(p.key)+len(p.cols))
	for _, k := range p.key {
		cols = append(cols, k.name)
	}
	for _, c := range p.cols {
		cols = append(cols, c.name)
	}
	s, how, err := newSlicer(ctx, ref, d, cols, opt.filter, opt.blockRows, opt.blockMem, 4*workers)
	if err != nil {
		return keySplit{}, err
	}
	if why != "" {
		why = "; " + why
	}
	log.Printf("parallel: %d workers, blocks of %d rows (%s) from an index scan of (%s)%s",
		workers, s.rows, how, keyList(d.pk), why)
	return keySplit{feed: s.feed(ref), slicer: s}, nil
}

// histogramRanges turns histogram bounds b0 < b1 < ... < bk of the first
// key column into k ranges: (-inf, b1), [b1, b2), ..., [b(k-1), +inf). The
// outer ranges are open, so rows outside a stale histogram are still
// covered; the others are half-open, with no gaps and no overlaps. Fewer
// than three bounds give one range, the whole key space.
func histogramRanges(b []string) []keyRange {
	if len(b) < 3 {
		return []keyRange{{}}
	}
	rs := make([]keyRange, 0, len(b)-1)
	for i := 1; i < len(b); i++ {
		var r keyRange
		if i > 1 {
			r.lo = keyBound{vals: []string{b[i-1]}, inclusive: true}
		}
		if i < len(b)-1 {
			r.hi = keyBound{vals: []string{b[i]}}
		}
		rs = append(rs, r)
	}
	return rs
}

// histogram returns the histogram bounds of a column as text, or nil if the
// table has not been analysed. typ is the column type.
//
// histogram_bounds is anyarray, which cannot be cast, subscripted or
// unnested; its text is parsed as an array of typ. The server also drops
// repeated bounds: a column that is not unique (the first column of a
// composite key) may have a value frequent enough to span several bins, and
// each repeat would make an empty range.
//
// The query runs under a savepoint: a type that cannot read its own text
// back fails here, and the caller can go on without parallelism.
func (s *side) histogram(ctx context.Context, d *description, col, typ string) ([]string, error) {
	sp, err := s.tx.Begin(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := sp.Query(ctx, fmt.Sprintf(`
		SELECT b::text
		FROM (SELECT b, o, lag(b) OVER (ORDER BY o) AS prev
		      FROM pg_catalog.pg_stats s,
		           unnest(s.histogram_bounds::text::%s[]) WITH ORDINALITY AS u(b, o)
		      WHERE s.schemaname = $1 AND s.tablename = $2 AND s.attname = $3
		        AND NOT s.inherited) h
		WHERE prev IS NULL OR b > prev
		ORDER BY o`, typ),
		d.schema, d.name, col)
	var b []string
	if err == nil {
		b, err = pgx.CollectRows(rows, pgx.RowTo[string])
	}
	if err != nil {
		_ = sp.Rollback(ctx)
		return nil, fmt.Errorf("%s: histogram of %s.%s: %w", s.name, d.qualifiedName(), col, err)
	}
	return b, sp.Commit(ctx)
}

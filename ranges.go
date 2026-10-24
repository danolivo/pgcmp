package main

import (
	"context"
	"fmt"
	"log"
	"strings"

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

// splitKeys cuts the key space for opt.jobs workers. A sequential run gets
// one range, the whole key space. Otherwise the ranges come from the
// histogram of the first key column on the reference side (see
// histogramRanges); without a usable histogram the run is sequential.
func splitKeys(ctx context.Context, opt *options, ref *side, d *description, p *plan) keySplit {
	whole := keySplit{ranges: []keyRange{{}}}
	if opt.jobs <= 1 {
		return whole
	}
	k := p.key[0]
	bounds, err := ref.histogram(ctx, d, k.name, k.base[0])
	switch {
	case err != nil:
		log.Printf("warning: %v; running without parallelism", err)
		return whole
	case len(bounds) < 2:
		log.Printf("warning: no histogram for %s.%s on the reference side (run ANALYZE); running without parallelism",
			d.qualifiedName(), k.name)
		return whole
	}
	rs := histogramRanges(bounds)
	log.Printf("parallel: %d workers, %d key ranges from the histogram of %s",
		min(opt.jobs, len(rs)), len(rs), k.name)
	return keySplit{ranges: rs}
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

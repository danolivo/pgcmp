package main

import (
	"context"
	"io"
	"log"
	"path/filepath"
	"strconv"
)

// mergeStrategy compares tables with a single integer key: both sides
// stream in key order and the streams are merged on the client. With -j > 1
// the key space is split by the histogram of the reference side.
type mergeStrategy struct {
	opt    *options
	tables [2]string
	p      *plan
	ranges []keyRange
}

func newMergeStrategy(ctx context.Context, opt *options, ref *side, d1, d2 *description, p *plan) (*mergeStrategy, error) {
	m := &mergeStrategy{
		opt:    opt,
		tables: [2]string{d1.qualifiedName(), d2.qualifiedName()},
		p:      p,
		ranges: []keyRange{{}},
	}
	if opt.jobs > 1 {
		pk := p.key[0].name
		bounds, err := ref.histogram(ctx, d1, pk)
		if err != nil {
			return nil, err
		}
		if len(bounds) < 2 {
			log.Printf("warning: no histogram for %s.%s on the reference side (run ANALYZE); running without parallelism",
				d1.qualifiedName(), pk)
		} else {
			m.ranges = rangesFromBounds(bounds)
			log.Printf("parallel: %d workers, %d key ranges from the histogram",
				min(opt.jobs, len(m.ranges)), len(m.ranges))
		}
	}
	return m, nil
}

func (m *mergeStrategy) name() string { return "merge by integer key" }

func (m *mergeStrategy) rangeQuery(side int, r keyRange) string {
	return buildQuery(m.tables[side], m.p, side, m.opt.filter, r)
}

func (m *mergeStrategy) query(side int) string { return m.rangeQuery(side, keyRange{}) }

func (m *mergeStrategy) planQueries(side int) []string {
	qs := make([]string, len(m.ranges))
	for i, r := range m.ranges {
		qs[i] = m.rangeQuery(side, r)
	}
	return qs
}

func (m *mergeStrategy) run(ctx context.Context, ref, tst *side, w io.Writer) (stats, error) {
	if len(m.ranges) == 1 {
		return runRange(ctx, ref, tst, m.rangeQuery(0, m.ranges[0]), m.rangeQuery(1, m.ranges[0]), w)
	}
	return runParallel(ctx, m.opt, ref, tst, len(m.ranges),
		func(ctx context.Context, ref, tst *side, i int, w io.Writer) (stats, error) {
			return runRange(ctx, ref, tst, m.rangeQuery(0, m.ranges[i]), m.rangeQuery(1, m.ranges[i]), w)
		}, filepath.Dir(m.opt.out), w)
}

// merge consumes two key-ordered streams and writes one report line per
// mismatch (see writeDiff). It returns when both channels are closed.
func merge(in1, in2 <-chan row, w io.Writer) (stats, error) {
	var st stats
	recv1 := func() (row, bool) {
		r, ok := <-in1
		if ok {
			st.rows1++
		}
		return r, ok
	}
	recv2 := func() (row, bool) {
		r, ok := <-in2
		if ok {
			st.rows2++
		}
		return r, ok
	}
	report := func(key int64, left, right string) error {
		return writeDiff(w, strconv.FormatInt(key, 10), left, right)
	}

	r1, ok1 := recv1()
	r2, ok2 := recv2()
	for ok1 || ok2 {
		var err error
		switch {
		case ok1 && ok2 && r1.key == r2.key:
			if r1.data != r2.data {
				st.differ++
				err = report(r1.key, r1.data, r2.data)
			}
			r1, ok1 = recv1()
			r2, ok2 = recv2()
		case ok1 && (!ok2 || r1.key < r2.key):
			st.onlyLeft++
			err = report(r1.key, r1.data, "NULL")
			r1, ok1 = recv1()
		default:
			st.onlyRight++
			err = report(r2.key, "NULL", r2.data)
			r2, ok2 = recv2()
		}
		if err != nil {
			return st, err
		}
	}
	return st, nil
}

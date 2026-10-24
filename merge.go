package main

import (
	"context"
	"io"
	"strconv"

	"golang.org/x/sync/errgroup"
)

// mergeStrategy compares tables with a single integer key: both sides
// stream in key order and the streams are merged on the client.
type mergeStrategy struct {
	opt    *options
	tables [2]string
	p      *plan
}

func newMergeStrategy(opt *options, d1, d2 *description, p *plan) *mergeStrategy {
	return &mergeStrategy{
		opt:    opt,
		tables: [2]string{d1.qualifiedName(), d2.qualifiedName()},
		p:      p,
	}
}

func (m *mergeStrategy) name() string { return "merge by integer key" }

func (m *mergeStrategy) rangeQuery(side int, r keyRange) string {
	return buildQuery(m.tables[side], m.p, side, m.opt.filter, r)
}

func (m *mergeStrategy) compareRange(ctx context.Context, ref, tst *side, r keyRange, w io.Writer) (stats, error) {
	return runRange(ctx, ref, tst, m.rangeQuery(0, r), m.rangeQuery(1, r), w)
}

// runRange streams both sides concurrently and merges them.
//
// errgroup cancels gctx as soon as any of the three goroutines fails;
// readers blocked on a full channel then wake up and exit. Wait returns the
// first error, which is the real cause.
func runRange(ctx context.Context, ref, tst *side, q1, q2 string, w io.Writer) (stats, error) {
	g, gctx := errgroup.WithContext(ctx)
	ch1 := make(chan row, bufferRows)
	ch2 := make(chan row, bufferRows)
	var st stats

	g.Go(func() error { return ref.stream(gctx, q1, ch1) })
	g.Go(func() error { return tst.stream(gctx, q2, ch2) })
	g.Go(func() error {
		var err error
		st, err = merge(ch1, ch2, w)
		return err
	})
	err := g.Wait()
	return st, err
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

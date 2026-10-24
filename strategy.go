package main

import (
	"context"
	"fmt"
	"io"
)

// strategy matches the rows of the two sides by primary key. compare()
// picks one right after the key has been negotiated (newStrategy); the rest
// of the program does not know which one runs.
//
//   - mergeStrategy: one int2/int4/int8 key column. Both sides stream in key
//     order and are merged on the client; parallel ranges come from the
//     histogram.
//   - hashStrategy: any other key. Blocks of the reference side are hashed
//     by the key text; each block is matched against the same key range of
//     the test side. The client never orders keys. Parallel ranges come
//     from the histogram of the first key column.
type strategy interface {
	// name is shown in the log.
	name() string
	// query is the query of one side (0 = reference, 1 = test) over the
	// whole key space, for the log.
	query(side int) string
	// planQueries are the queries of one side whose plans are checked
	// before the run.
	planQueries(side int) []string
	// run compares the two sides and writes one report line per mismatch.
	run(ctx context.Context, ref, tst *side, w io.Writer) (stats, error)
}

func newStrategy(ctx context.Context, opt *options, ref *side, d1, d2 *description, p *plan) (strategy, error) {
	if p.intKey {
		return newMergeStrategy(ctx, opt, ref, d1, d2, p)
	}
	return newHashStrategy(ctx, opt, ref, d1, d2, p), nil
}

type stats struct {
	rows1, rows2 uint64 // rows read from the reference / test side
	differ       uint64 // same key, different row text
	onlyLeft     uint64 // key present only on the reference side
	onlyRight    uint64 // key present only on the test side
	blocks       uint64 // hash strategy: blocks compared
}

func (s stats) diffs() uint64 { return s.differ + s.onlyLeft + s.onlyRight }

func (s *stats) add(o stats) {
	s.rows1 += o.rows1
	s.rows2 += o.rows2
	s.differ += o.differ
	s.onlyLeft += o.onlyLeft
	s.onlyRight += o.onlyRight
	s.blocks += o.blocks
}

// writeDiff writes one report line: <key> <reference row> <test row>, with
// NULL standing for a row that is absent on one side.
//
// A space is a safe separator: record_out double-quotes any field that
// contains whitespace, so a row's text never has a bare space in it. A row
// always starts with '(', so it cannot be confused with the NULL marker.
// The key is either an integer or, in the hash strategy, the record_out
// text of the key columns, so the same holds for it.
func writeDiff(w io.Writer, key, left, right string) error {
	if _, err := fmt.Fprintf(w, "%s %s %s\n", key, left, right); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}

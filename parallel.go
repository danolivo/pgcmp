package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/sync/errgroup"
)

// isTooManyConnections reports whether a connection attempt was refused
// because the server is out of connection slots (SQLSTATE 53300).
func isTooManyConnections(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "53300"
}

func (s *side) exportSnapshot(ctx context.Context) (string, error) {
	var id string
	if err := s.tx.QueryRow(ctx, "SELECT pg_catalog.pg_export_snapshot()").Scan(&id); err != nil {
		return "", fmt.Errorf("%s: export snapshot: %w", s.name, err)
	}
	return id, nil
}

// rangeFeed sends key ranges to out, in key order. It must return as soon
// as ctx is done. The caller closes out after it returns.
//
// The number of ranges need not be known in advance: a feed may cut the
// key space while the workers are already comparing.
type rangeFeed func(ctx context.Context, out chan<- keyRange) error

// rangeList feeds ranges that are known in advance.
func rangeList(rs []keyRange) rangeFeed {
	return func(ctx context.Context, out chan<- keyRange) error {
		for _, r := range rs {
			select {
			case out <- r:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
}

// rangeFunc compares key range r in the given pair of sessions and writes
// its report lines to w.
type rangeFunc func(ctx context.Context, ref, tst *side, r keyRange, w io.Writer) (stats, error)

// runParallel compares the ranges of feed with up to workers pairs of
// sessions; how one range is compared is up to the strategy (compare).
//
// Each side exports the snapshot of its coordinator transaction, and every
// worker imports it, so all sessions of one side see exactly the same data.
// A worker is a pair of connections (reference + test) that takes ranges
// one by one. The report comes out in range order (see orderedReport), so
// the report of a range-ordered strategy stays ordered.
func runParallel(ctx context.Context, opt *options, ref, tst *side, feed rangeFeed, workers int,
	compare rangeFunc, tmpDir string, w io.Writer) (stats, error) {

	snap1, err := ref.exportSnapshot(ctx)
	if err != nil {
		return stats{}, err
	}
	snap2, err := tst.exportSnapshot(ctx)
	if err != nil {
		return stats{}, err
	}

	// Open all worker connections up front. If a server runs out of
	// connection slots, go on with the workers that did connect: fewer
	// workers is slower but still correct.
	type pair struct{ ref, tst *side }
	var pairs []pair
	defer func() {
		for _, p := range pairs {
			p.ref.close()
			p.tst.close()
		}
	}()
	for len(pairs) < workers {
		a, err := openSide(ctx, "reference", opt.dsn1, snap1)
		if err == nil {
			var b *side
			if b, err = openSide(ctx, "test", opt.dsn2, snap2); err == nil {
				pairs = append(pairs, pair{a, b})
				continue
			}
			a.close()
		}
		if len(pairs) > 0 && isTooManyConnections(err) {
			log.Printf("warning: only %d of %d workers could connect: %v", len(pairs), workers, err)
			break
		}
		return stats{}, err
	}

	rep := newOrderedReport(tmpDir, w)
	defer rep.cleanup()
	err = forEachRange(ctx, feed, len(pairs), func(ctx context.Context, worker, seq int, r keyRange) error {
		p := pairs[worker]
		return rep.add(seq, func(w io.Writer) (stats, error) {
			return compare(ctx, p.ref, p.tst, r, w)
		})
	})
	if err != nil {
		return stats{}, err
	}
	return rep.finish()
}

// forEachRange runs work on every range of feed with n workers. Ranges are
// numbered in the order the feed sends them, from 0; worker is the number
// of the worker, from 0 to n-1, so that work can pick its sessions.
//
// The first error cancels the context of the feed and of all workers and is
// returned; the feed may run ahead of the workers by a few ranges.
func forEachRange(ctx context.Context, feed rangeFeed, n int,
	work func(ctx context.Context, worker, seq int, r keyRange) error) error {

	if n < 1 {
		return errors.New("internal error: no workers")
	}
	type job struct {
		seq int
		r   keyRange
	}
	g, gctx := errgroup.WithContext(ctx)
	ranges := make(chan keyRange, 4*n)
	jobs := make(chan job)

	g.Go(func() error {
		defer close(ranges)
		return feed(gctx, ranges)
	})
	// Numbering happens in one goroutine, in the order of the feed; workers
	// receive concurrently and could not agree on an order themselves.
	g.Go(func() error {
		defer close(jobs)
		seq := 0
		for r := range ranges {
			select {
			case jobs <- job{seq, r}:
				seq++
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})
	for worker := range n {
		g.Go(func() error {
			for j := range jobs {
				if err := work(gctx, worker, j.seq, j.r); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return g.Wait()
}

// orderedReport puts together the reports of ranges compared out of order.
//
// Each range writes its report to a temporary file in dir. As soon as all
// ranges before it are done, the file is appended to w and removed, so
// only ranges that finished ahead of a slower predecessor wait on disk,
// however many ranges there are.
type orderedReport struct {
	dir string

	mu    sync.Mutex
	w     io.Writer
	next  int            // first range not yet appended to w
	ready map[int]string // finished ranges waiting for a predecessor: their files
	total stats
}

func newOrderedReport(dir string, w io.Writer) *orderedReport {
	return &orderedReport{dir: dir, w: w, ready: map[int]string{}}
}

// add runs compare for range seq with a temporary file as its report, then
// appends to w every report that is now in order. Each seq must be added
// once; add may be called from several goroutines.
func (o *orderedReport) add(seq int, compare func(w io.Writer) (stats, error)) error {
	f, err := os.CreateTemp(o.dir, ".pgcmp-*.part")
	if err != nil {
		return fmt.Errorf("create temporary report: %w", err)
	}
	bw := bufio.NewWriter(f)
	st, err := compare(bw)
	if err == nil {
		err = bw.Flush()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(f.Name())
		return err
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	o.total.add(st)
	o.ready[seq] = f.Name()
	for {
		path, ok := o.ready[o.next]
		if !ok {
			return nil
		}
		delete(o.ready, o.next)
		o.next++
		if err := appendFile(o.w, path); err != nil {
			return err
		}
	}
}

// finish returns the total of all ranges once every one has been added.
func (o *orderedReport) finish() (stats, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.ready) > 0 {
		return o.total, fmt.Errorf("internal error: %d range reports wait for range %d, which never came", len(o.ready), o.next)
	}
	return o.total, nil
}

// cleanup removes the files of reports that never made it to w (after an
// error).
func (o *orderedReport) cleanup() {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, path := range o.ready {
		os.Remove(path)
	}
	clear(o.ready)
}

// appendFile copies the file to w and removes it.
func appendFile(w io.Writer, path string) error {
	defer os.Remove(path)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("read temporary report: %w", err)
	}
	defer f.Close()
	if _, err := io.Copy(w, f); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}

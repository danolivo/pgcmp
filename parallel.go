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

// runParallel compares the ranges of split with split.workers(opt.jobs)
// workers; how one range is compared is up to the strategy (compare).
//
// A worker is a pair of sessions (reference + test) that takes ranges one
// by one. The coordinator pair, ref and tst, is one of the workers, so a
// run with j workers needs j connections per server. It starts right away
// when the ranges are a list; with a feed it starts once the feed has
// returned, because the feed may read through the coordinator transaction
// (see keySplit.feedHoldsCoordinator).
//
// The other workers import the snapshots of the coordinator transactions,
// so all sessions of one side see exactly the same data. The report comes
// out in range order (see orderedReport), so the report of a range-ordered
// strategy stays ordered.
func runParallel(ctx context.Context, opt *options, ref, tst *side, split keySplit,
	compare rangeFunc, tmpDir string, w io.Writer) (stats, error) {

	type pair struct{ ref, tst *side }
	var pairs []pair // opened here: all workers but the coordinator
	defer func() {
		for _, p := range pairs {
			p.ref.close()
			p.tst.close()
		}
	}()
	workers := split.workers(opt.jobs)
	if workers > 1 {
		snap1, err := ref.exportSnapshot(ctx)
		if err != nil {
			return stats{}, err
		}
		snap2, err := tst.exportSnapshot(ctx)
		if err != nil {
			return stats{}, err
		}
		// Open the connections up front. If a server runs out of
		// connection slots, go on with the workers that did connect, the
		// coordinator at least: fewer workers is slower but still correct.
		for len(pairs) < workers-1 {
			a, err := openSide(ctx, "reference", opt.dsn1, snap1)
			if err == nil {
				var b *side
				if b, err = openSide(ctx, "test", opt.dsn2, snap2); err == nil {
					pairs = append(pairs, pair{a, b})
					continue
				}
				a.close()
			}
			if isTooManyConnections(err) {
				log.Printf("warning: only %d of %d workers could connect: %v", len(pairs)+1, workers, err)
				break
			}
			return stats{}, err
		}
	}
	all := append(pairs[:len(pairs):len(pairs)], pair{ref, tst}) // the coordinator is the last worker

	rep := newOrderedReport(tmpDir, w)
	defer rep.cleanup()
	err := forEachRange(ctx, split.source(), len(all), split.feedHoldsCoordinator(),
		func(ctx context.Context, worker, seq int, r keyRange) error {
			p := all[worker]
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
// The feed never waits for the workers: ranges it sends queue up without
// limit, so it runs at its own pace and finishes as early as it can. A
// range costs only its bounds in memory. With late, the last worker starts
// only after the feed has returned, in the feed's own goroutine: it may
// share the feed's sessions. Even a single late worker cannot deadlock,
// since the feed never waits for it.
//
// The first error cancels the context of the feed and of all workers and is
// returned.
func forEachRange(ctx context.Context, feed rangeFeed, n int, late bool,
	work func(ctx context.Context, worker, seq int, r keyRange) error) error {

	if n < 1 {
		return errors.New("internal error: no workers")
	}
	type job struct {
		seq int
		r   keyRange
	}
	g, gctx := errgroup.WithContext(ctx)
	ranges := make(chan keyRange)
	jobs := make(chan job)

	loop := func(worker int) error {
		for j := range jobs {
			if err := work(gctx, worker, j.seq, j.r); err != nil {
				return err
			}
		}
		return nil
	}
	g.Go(func() error {
		err := feed(gctx, ranges)
		close(ranges)
		if err != nil || !late {
			return err
		}
		return loop(n - 1)
	})
	// The queue: always ready to take the next range from the feed, and
	// hands ranges out to the workers in feed order, numbering them. One
	// goroutine numbers them: workers receive concurrently and could not
	// agree on an order themselves.
	g.Go(func() error {
		defer close(jobs)
		var queue []keyRange
		seq := 0
		in := ranges
		for in != nil || len(queue) > 0 {
			var out chan<- job // nil, so never ready, while the queue is empty
			var head job
			if len(queue) > 0 {
				out, head = jobs, job{seq, queue[0]}
			}
			select {
			case r, ok := <-in:
				if !ok {
					in = nil // the feed is done; hand out the rest
					continue
				}
				queue = append(queue, r)
			case out <- head:
				queue[0] = keyRange{} // drop the bounds of a range handed out
				queue = queue[1:]
				seq++
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})
	early := n
	if late {
		early = n - 1
	}
	for worker := range early {
		g.Go(func() error { return loop(worker) })
	}
	return g.Wait()
}

// orderedReport puts together the reports of ranges compared out of order.
//
// A range that finds differences writes them to a temporary file in dir;
// a range that matches writes nothing and leaves no file. As soon as all
// ranges before a range are done, its file is appended to w and removed,
// so only ranges with differences that finished ahead of a slower
// predecessor wait on disk.
type orderedReport struct {
	dir string

	mu    sync.Mutex
	w     io.Writer
	next  int            // first range not yet appended to w
	ready map[int]string // finished ranges waiting for a predecessor: their files, "" if none
	total stats
}

func newOrderedReport(dir string, w io.Writer) *orderedReport {
	return &orderedReport{dir: dir, w: w, ready: map[int]string{}}
}

// add runs compare for range seq with a temporary file as its report, then
// appends to w every report that is now in order. Each seq must be added
// once; add may be called from several goroutines.
func (o *orderedReport) add(seq int, compare func(w io.Writer) (stats, error)) error {
	lf := &lazyFile{dir: o.dir}
	st, err := compare(lf)
	path, err := lf.finish(err)
	if err != nil {
		return err
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	o.total.add(st)
	o.ready[seq] = path
	for {
		path, ok := o.ready[o.next]
		if !ok {
			return nil
		}
		delete(o.ready, o.next)
		o.next++
		if path == "" {
			continue
		}
		if err := appendFile(o.w, path); err != nil {
			return err
		}
	}
}

// lazyFile is the report of one range. It becomes a temporary file only
// when the first line is written, so a range that matched costs no file.
type lazyFile struct {
	dir string
	f   *os.File
	bw  *bufio.Writer
}

func (l *lazyFile) Write(p []byte) (int, error) {
	if l.f == nil {
		f, err := os.CreateTemp(l.dir, ".pgcmp-*.part")
		if err != nil {
			return 0, fmt.Errorf("create temporary report: %w", err)
		}
		l.f, l.bw = f, bufio.NewWriter(f)
	}
	return l.bw.Write(p)
}

// finish flushes and closes the file and returns its path, or "" if
// nothing was written. err is the error of the comparison: with it, or
// with an error of its own, the file is removed and the error returned.
func (l *lazyFile) finish(err error) (string, error) {
	if l.f == nil {
		return "", err
	}
	if err == nil {
		err = l.bw.Flush()
	}
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(l.f.Name())
		return "", err
	}
	return l.f.Name(), nil
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
		if path != "" {
			os.Remove(path)
		}
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

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

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

// rangeFunc compares key range r in the given pair of sessions and writes
// its report lines to w.
type rangeFunc func(ctx context.Context, ref, tst *side, r keyRange, w io.Writer) (stats, error)

// runParallel compares the key ranges with up to opt.jobs workers; how one
// range is compared is up to the strategy (compare).
//
// Each side exports the snapshot of its coordinator transaction, and every
// worker imports it, so all sessions of one side see exactly the same data.
// A worker is a pair of connections (reference + test) that takes ranges
// from a queue one by one. Each range writes its report to a temporary
// file; at the end the files are appended to w in range order, so the
// report of a range-ordered strategy stays ordered.
func runParallel(ctx context.Context, opt *options, ref, tst *side, ranges []keyRange,
	compare rangeFunc, tmpDir string, w io.Writer) (stats, error) {
	n := len(ranges)

	snap1, err := ref.exportSnapshot(ctx)
	if err != nil {
		return stats{}, err
	}
	snap2, err := tst.exportSnapshot(ctx)
	if err != nil {
		return stats{}, err
	}

	type result struct {
		st   stats
		path string
	}
	results := make([]result, n)
	defer func() {
		for _, r := range results {
			if r.path != "" {
				os.Remove(r.path)
			}
		}
	}()

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
	for len(pairs) < min(opt.jobs, n) {
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
			log.Printf("warning: only %d of %d workers could connect: %v", len(pairs), min(opt.jobs, n), err)
			break
		}
		return stats{}, err
	}

	g, gctx := errgroup.WithContext(ctx)

	queue := make(chan int)
	g.Go(func() error {
		defer close(queue)
		for i := range n {
			select {
			case queue <- i:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})

	for _, p := range pairs {
		a, b := p.ref, p.tst
		g.Go(func() error {
			// Each index comes out of the queue once, so results[i] has a
			// single writer; it is read only after g.Wait.
			for i := range queue {
				f, err := os.CreateTemp(tmpDir, ".pgcmp-*.part")
				if err != nil {
					return fmt.Errorf("create temporary report: %w", err)
				}
				results[i].path = f.Name()
				bw := bufio.NewWriter(f)
				st, err := compare(gctx, a, b, ranges[i], bw)
				if err == nil {
					err = bw.Flush()
				}
				if cerr := f.Close(); err == nil {
					err = cerr
				}
				if err != nil {
					return err
				}
				results[i].st = st
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return stats{}, err
	}

	var total stats
	for _, r := range results {
		total.add(r.st)
		f, err := os.Open(r.path)
		if err != nil {
			return total, fmt.Errorf("read temporary report: %w", err)
		}
		_, err = io.Copy(w, f)
		f.Close()
		if err != nil {
			return total, fmt.Errorf("write report: %w", err)
		}
	}
	return total, nil
}

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// seqRange marks a range with its position in the feed.
func seqRange(i int) keyRange { return keyRange{lo: keyBound{vals: []string{strconv.Itoa(i)}}} }

func countingFeed(n int) rangeFeed {
	return func(ctx context.Context, out chan<- keyRange) error {
		for i := range n {
			select {
			case out <- seqRange(i):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
}

func assertEmptyDir(t *testing.T, dir string) {
	t.Helper()
	left, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) > 0 {
		t.Fatalf("%d temporary files left in %s", len(left), dir)
	}
}

// Ranges finish in random order, but the report comes out in feed order,
// and every range is compared exactly once. With late, the last worker
// takes nothing before the feed has returned.
func TestForEachRangeOrder(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(fmt.Sprintf("late=%v", late), func(t *testing.T) { testForEachRangeOrder(t, late) })
	}
}

func testForEachRangeOrder(t *testing.T, late bool) {
	const ranges, workers = 300, 8
	dir := t.TempDir()
	var out bytes.Buffer
	rep := newOrderedReport(dir, &out)
	var seen [ranges]atomic.Int32
	var busy [workers]atomic.Int32
	var fed atomic.Bool
	feed := func(ctx context.Context, out chan<- keyRange) error {
		err := countingFeed(ranges)(ctx, out)
		fed.Store(true)
		return err
	}

	err := forEachRange(context.Background(), feed, workers, late,
		func(ctx context.Context, worker, seq int, r keyRange) error {
			if busy[worker].Add(1) != 1 {
				return fmt.Errorf("worker %d runs two ranges at once", worker)
			}
			defer busy[worker].Add(-1)
			if late && worker == workers-1 && !fed.Load() {
				return fmt.Errorf("late worker took range %d before the feed returned", seq)
			}
			if r.lo.vals[0] != strconv.Itoa(seq) {
				return fmt.Errorf("range %v numbered %d", r.lo.vals, seq)
			}
			seen[seq].Add(1)
			return rep.add(seq, func(w io.Writer) (stats, error) {
				time.Sleep(time.Duration(rand.IntN(300)) * time.Microsecond)
				_, err := fmt.Fprintf(w, "%d\n", seq)
				return stats{rows1: 1, differ: uint64(seq % 2)}, err
			})
		})
	if err != nil {
		t.Fatal(err)
	}
	st, err := rep.finish()
	if err != nil {
		t.Fatal(err)
	}
	var want strings.Builder
	for i := range ranges {
		fmt.Fprintf(&want, "%d\n", i)
		if n := seen[i].Load(); n != 1 {
			t.Fatalf("range %d compared %d times", i, n)
		}
	}
	if out.String() != want.String() {
		t.Fatalf("report out of order:\n%s", out.String())
	}
	if st.rows1 != ranges || st.differ != ranges/2 {
		t.Fatalf("stats = %+v", st)
	}
	assertEmptyDir(t, dir)
}

// One worker that may start only after the feed: the feed must never wait
// for it, however many ranges it sends (the slicer at -j 1).
func TestForEachRangeSingleLateWorker(t *testing.T) {
	const ranges = 20000
	var fed atomic.Bool
	feed := func(ctx context.Context, out chan<- keyRange) error {
		err := countingFeed(ranges)(ctx, out)
		fed.Store(true)
		return err
	}
	next := 0
	done := make(chan error, 1)
	go func() {
		done <- forEachRange(context.Background(), feed, 1, true, func(_ context.Context, _, seq int, _ keyRange) error {
			if !fed.Load() {
				return errors.New("worker started before the feed returned")
			}
			if seq != next {
				return fmt.Errorf("range %d after %d", seq, next-1)
			}
			next++
			return nil
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: the feed waits for the only worker, which waits for the feed")
	}
	if next != ranges {
		t.Fatalf("%d ranges compared, want %d", next, ranges)
	}
}

// A failing range stops a feed that would never end on its own, and its
// error is the one returned.
func TestForEachRangeError(t *testing.T) {
	boom := errors.New("boom")
	endless := func(ctx context.Context, out chan<- keyRange) error {
		for i := 0; ; i++ {
			select {
			case out <- seqRange(i):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	done := make(chan error)
	go func() {
		done <- forEachRange(context.Background(), endless, 4, false, func(ctx context.Context, _, seq int, _ keyRange) error {
			switch {
			case seq < 5:
				return nil
			case seq == 5:
				return boom
			}
			<-ctx.Done() // a long range, cancelled by the failure
			return ctx.Err()
		})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, boom) {
			t.Fatalf("got %v, want boom", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("forEachRange did not return")
	}
}

func TestForEachRangeFeedError(t *testing.T) {
	boom := errors.New("histogram went away")
	feed := func(ctx context.Context, out chan<- keyRange) error {
		if err := countingFeed(3)(ctx, out); err != nil {
			return err
		}
		return boom
	}
	// The late worker must not run after a failed feed.
	var lateRan atomic.Bool
	err := forEachRange(context.Background(), feed, 2, true, func(_ context.Context, worker, _ int, _ keyRange) error {
		if worker == 1 {
			lateRan.Store(true)
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want the feed's error", err)
	}
	if lateRan.Load() {
		t.Fatal("the late worker ran after the feed failed")
	}
}

// Parent cancellation (Ctrl-C) stops everything.
func TestForEachRangeCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var started atomic.Int32
	err := forEachRange(ctx, countingFeed(1000), 3, false, func(ctx context.Context, _, _ int, _ keyRange) error {
		if started.Add(1) == 3 { // all workers are busy
			cancel()
		}
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

// Matched ranges waiting for a slow predecessor leave nothing on disk;
// only a range with differences keeps a file until its turn.
func TestOrderedReportMatchedRangesNoFiles(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	rep := newOrderedReport(dir, &out)
	matched := func(io.Writer) (stats, error) { return stats{rows1: 10, rows2: 10}, nil }
	for seq := 1; seq <= 100; seq++ {
		if err := rep.add(seq, matched); err != nil {
			t.Fatal(err)
		}
	}
	assertEmptyDir(t, dir)
	if err := rep.add(101, func(w io.Writer) (stats, error) {
		_, err := io.WriteString(w, "101 (a) (b)\n")
		return stats{differ: 1}, err
	}); err != nil {
		t.Fatal(err)
	}
	if left, _ := os.ReadDir(dir); len(left) != 1 {
		t.Fatalf("%d files waiting, want 1 (the range with differences)", len(left))
	}
	if err := rep.add(0, matched); err != nil { // the slow one
		t.Fatal(err)
	}
	st, err := rep.finish()
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != "101 (a) (b)\n" || st.rows1 != 1010 || st.differ != 1 {
		t.Fatalf("report %q, stats %+v", out.String(), st)
	}
	assertEmptyDir(t, dir)
}

// After an error, nothing is left on disk: neither the report of the range
// that failed nor those that waited for a predecessor.
func TestOrderedReportCleanup(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	rep := newOrderedReport(dir, &out)
	write := func(s string) func(io.Writer) (stats, error) {
		return func(w io.Writer) (stats, error) { _, err := io.WriteString(w, s); return stats{}, err }
	}
	for _, seq := range []int{2, 1} {
		if err := rep.add(seq, write(strconv.Itoa(seq))); err != nil {
			t.Fatal(err)
		}
	}
	boom := errors.New("boom")
	if err := rep.add(0, func(io.Writer) (stats, error) { return stats{}, boom }); !errors.Is(err, boom) {
		t.Fatalf("got %v, want boom", err)
	}
	if out.Len() != 0 {
		t.Fatalf("ranges 1 and 2 written before range 0: %q", out.String())
	}
	if _, err := rep.finish(); err == nil {
		t.Fatal("finish with ranges waiting for range 0: no error")
	}
	rep.cleanup()
	assertEmptyDir(t, dir)
}

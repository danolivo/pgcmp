// pgcmp compares one table on two PostgreSQL instances by streaming both
// sides in primary-key order. A single integer key is merged on the client;
// any other key is matched block by block through a hash table, with the
// key ranges evaluated by the servers (see strategy.go).
//
// Row contents are compared as the server's own text representation of
// ROW(col1, col2, ...) — i.e. what psql prints for a composite value — so
// no type conversion happens on the client side. Both sessions are
// configured with identical output-affecting settings (TimeZone,
// DateStyle, ...) before the comparison starts.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	exitMatch = 0
	exitDiff  = 1
	exitError = 2
)

// Rows each reader may run ahead of the merger. A full channel blocks the
// reader, which stops reading the socket, so client memory stays bounded
// no matter how far the two tables diverge.
const bufferRows = 4096

type options struct {
	dsn1, dsn2 string
	table      string
	columns    []string
	filter     string
	out        string
	jobs       int
	blockMem   byteSize // hash strategy: memory for one block of reference rows
	blockRows  int      // hash strategy: rows in one block; 0 = limited by blockMem only
}

// byteSize is a -mem value: a number with an optional PostgreSQL-style
// unit (kB, MB, GB, TB; powers of 1024). A bare number means MB.
type byteSize int64

var sizeUnits = []struct {
	suffix string
	shift  uint
}{{"kB", 10}, {"MB", 20}, {"GB", 30}, {"TB", 40}}

func (b *byteSize) Set(s string) error {
	s = strings.TrimSpace(s)
	shift := uint(20)
	for _, u := range sizeUnits {
		if strings.HasSuffix(s, u.suffix) {
			s, shift = strings.TrimSpace(strings.TrimSuffix(s, u.suffix)), u.shift
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 1 || n > (1<<62)>>shift {
		return fmt.Errorf("invalid size %q: want a positive number with an optional unit kB, MB, GB or TB", s)
	}
	*b = byteSize(n << shift)
	return nil
}

func (b byteSize) String() string {
	for i := len(sizeUnits) - 1; i >= 0; i-- {
		if u := sizeUnits[i]; b >= 1<<u.shift && int64(b)%(1<<u.shift) == 0 {
			return strconv.FormatInt(int64(b)>>u.shift, 10) + u.suffix
		}
	}
	return strconv.FormatInt(int64(b), 10) + "B"
}

func main() {
	os.Exit(run())
}

func run() int {
	log.SetFlags(0)
	log.SetPrefix("pgcmp: ")

	var opt options
	var cols string
	flag.StringVar(&opt.dsn1, "dsn1", "", "connection string of the reference instance")
	flag.StringVar(&opt.dsn2, "dsn2", "", "connection string of the instance under test")
	flag.StringVar(&opt.table, "table", "", "table name, optionally schema-qualified")
	flag.StringVar(&cols, "columns", "", "comma-separated columns to compare (default: columns present on both sides)")
	flag.StringVar(&opt.filter, "filter", "", "WHERE expression applied on both sides")
	flag.StringVar(&opt.out, "out", "pgcmp.out", "report file")
	flag.IntVar(&opt.jobs, "j", 1, "parallel workers; key ranges come from the reference histogram of the (first) key column")
	opt.blockMem = 64 << 20
	flag.Var(&opt.blockMem, "mem", "memory for one block of reference rows, e.g. 64MB or 512kB; a bare number is MB (keys other than a single integer column)")
	flag.IntVar(&opt.blockRows, "rows", 0, "at most this many reference rows in one block, 0 = no limit; with -mem, whichever is reached first closes the block (keys other than a single integer column)")
	flag.Parse()

	// Parsing stops at the first non-flag argument; everything after it
	// (including any further -flags) lands in flag.Args() unparsed.
	if flag.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "pgcmp: unexpected arguments: %q\n", flag.Args())
		flag.Usage()
		return exitError
	}

	if opt.dsn1 == "" || opt.dsn2 == "" || opt.table == "" || opt.jobs < 1 || opt.blockRows < 0 {
		fmt.Fprintln(os.Stderr, "usage: pgcmp -dsn1 <dsn> -dsn2 <dsn> -table <name> [-columns a,b,c] [-filter <expr>] [-j N] [-mem size] [-rows N] [-out <file>]")
		return exitError
	}
	for _, c := range strings.Split(cols, ",") {
		if c = strings.TrimSpace(c); c != "" {
			opt.columns = append(opt.columns, c)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := compare(ctx, &opt)
	if err != nil {
		log.Printf("error: %v", err)
		return exitError
	}

	log.Printf("reference: %d rows, test: %d rows", st.rows1, st.rows2)
	if st.diffs() == 0 {
		log.Printf("match")
		return exitMatch
	}
	log.Printf("mismatch: %d differ, %d only in reference, %d only in test (see %s)",
		st.differ, st.onlyLeft, st.onlyRight, opt.out)
	return exitDiff
}

func compare(ctx context.Context, opt *options) (stats, error) {
	started := time.Now()

	// 1. Connect both sides and align session settings.
	ref, err := openSide(ctx, "reference", opt.dsn1, "")
	if err != nil {
		return stats{}, err
	}
	defer ref.close()
	tst, err := openSide(ctx, "test", opt.dsn2, "")
	if err != nil {
		return stats{}, err
	}
	defer tst.close()
	if err := checkEncodings(ref.encoding, tst.encoding); err != nil {
		return stats{}, err
	}
	if w := versionWarning(ref.version, tst.version); w != "" {
		log.Print(w)
	}

	// 2. Negotiate the table structure.
	d1, err := ref.describe(ctx, opt.table)
	if err != nil {
		return stats{}, err
	}
	d2, err := tst.describe(ctx, opt.table)
	if err != nil {
		return stats{}, err
	}
	// The reference server decides on common types for mismatching columns.
	p, err := negotiate(d1, d2, opt.columns, func(c1, c2 column) (string, error) {
		return ref.commonType(ctx, c1, c2)
	})
	if err != nil {
		return stats{}, err
	}
	names := make([]string, len(p.cols))
	for i, c := range p.cols {
		names[i] = c.name
	}
	keyTypes := make([]string, len(d1.pk))
	for i, c := range d1.pk {
		keyTypes[i] = c.typeName
	}
	log.Printf("key: (%s) [%s]; columns: %s", keyList(d1.pk), strings.Join(keyTypes, ", "), strings.Join(names, ", "))
	if len(p.ignored) > 0 {
		log.Printf("ignored (present on one side only): %s", strings.Join(p.ignored, ", "))
	}

	// 3. Choose how rows are matched by key, and cut the key space into
	// ranges for the workers. Everything below goes through the strategy
	// and does not depend on either choice.
	cmp := newStrategy(opt, d1, d2, p)
	ranges := keyRanges(ctx, opt, ref, d1, p)
	log.Printf("method: %s", cmp.name())
	q1, q2 := cmp.rangeQuery(0, keyRange{}), cmp.rangeQuery(1, keyRange{})
	log.Printf("reference query: %s", q1)
	if q2 != q1 {
		log.Printf("test query: %s", q2)
	}

	// 4. Check that each side will read the rows of every range in key
	// order through the primary key index rather than scan and sort the
	// table.
	for i, s := range []*side{ref, tst} {
		table := []*description{d1, d2}[i].qualifiedName()
		queries := make([]string, len(ranges))
		for j, r := range ranges {
			queries[j] = cmp.rangeQuery(i, r)
		}
		w, err := s.checkPlans(ctx, table, queries)
		if err != nil {
			return stats{}, err
		}
		if w != "" {
			log.Print(w)
		}
	}

	// 5. Open the report.
	f, err := os.Create(opt.out)
	if err != nil {
		return stats{}, fmt.Errorf("create report: %w", err)
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)

	// 6. Compare: one range in the coordinator sessions, or several in
	// parallel workers.
	var st stats
	if len(ranges) == 1 {
		st, err = cmp.compareRange(ctx, ref, tst, ranges[0], w)
	} else {
		st, err = runParallel(ctx, opt, ref, tst, ranges, cmp.compareRange, filepath.Dir(opt.out), w)
	}
	if err != nil {
		return st, err
	}
	if st.blocks > 0 {
		log.Printf("blocks: %d", st.blocks)
	}

	if st.diffs() == 0 {
		if _, err := w.WriteString("match\n"); err != nil {
			return st, fmt.Errorf("write report: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		return st, fmt.Errorf("write report: %w", err)
	}
	if err := f.Sync(); err != nil {
		return st, fmt.Errorf("sync report: %w", err)
	}
	log.Printf("done in %s", time.Since(started).Round(time.Millisecond))
	return st, nil
}

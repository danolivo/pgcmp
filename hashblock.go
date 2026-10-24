package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5"
)

// hashStrategy compares tables whose key is not a single integer column:
// any type, any number of columns.
//
// The client never decides which of two keys is smaller; both servers do.
//
//  1. The reference side runs one query ordered by the key and is read as a
//     stream. Rows go into a block — a hash table keyed by the text of the
//     key — until the block reaches its memory limit. The key columns of
//     the last row taken, as text, are the upper bound of the block.
//  2. The test side is asked for the same range: key > previous bound AND
//     key <= this bound, with the bounds sent as text parameters and cast to
//     the column types of the test side. Each row is looked up in the block
//     and reported at once; test rows are never stored, so client memory
//     is bounded by one block however the tables differ.
//  3. Block rows nobody asked for are present only on the reference side.
//
// The first block has no lower bound and the last one no upper bound. The
// reference query stays open between blocks: the client simply stops
// reading it, the socket fills up and the server waits until the next
// block. No cursor and no repeated index descents.
//
// With -j > 1 the key space is first cut into ranges on the first key
// column, by its histogram on the reference side, and every range runs the
// block loop above in its own pair of sessions.
//
// This relies on both servers ordering the key the same way, which the
// plan check verifies indirectly: a key whose ordering does not match its
// index (a different collation) would be read with a sequential scan.
//
// Keys are matched by their text, ROW(k1, k2, ...)::text on both sides,
// cast to the common type where the column types differ.
type hashStrategy struct {
	opt    *options
	tables [2]string
	p      *plan
	filter string
	mem    byteSize
	ranges []textRange
}

// textRange is a half-open interval on the first key column,
// lo <= k1 < hi, with the bounds as text; a nil bound is open.
type textRange struct {
	lo, hi *string
}

func newHashStrategy(ctx context.Context, opt *options, ref *side, d1, d2 *description, p *plan) *hashStrategy {
	h := &hashStrategy{
		opt:    opt,
		tables: [2]string{d1.qualifiedName(), d2.qualifiedName()},
		p:      p,
		filter: strings.TrimSpace(opt.filter),
		mem:    opt.blockMem,
		ranges: []textRange{{}},
	}
	if opt.jobs > 1 {
		k := p.key[0]
		bounds, err := ref.histogramText(ctx, d1, k.name, k.base[0])
		switch {
		case err != nil:
			log.Printf("warning: %v; running without parallelism", err)
		case len(bounds) < 2:
			log.Printf("warning: no histogram for %s.%s on the reference side (run ANALYZE); running without parallelism",
				d1.qualifiedName(), k.name)
		default:
			lo, hi := splitBounds(bounds)
			h.ranges = make([]textRange, len(lo))
			for i := range lo {
				h.ranges[i] = textRange{lo[i], hi[i]}
			}
			log.Printf("parallel: %d workers, %d key ranges from the histogram of %s",
				min(opt.jobs, len(h.ranges)), len(h.ranges), k.name)
		}
	}
	return h
}

func (h *hashStrategy) name() string {
	if h.opt.blockRows > 0 {
		return fmt.Sprintf("hash blocks of up to %d reference rows or %s, whichever is reached first", h.opt.blockRows, h.mem)
	}
	return fmt.Sprintf("hash blocks of up to %s of reference rows", h.mem)
}

// keyExpr is the text of the key on one side, used to match rows.
func (h *hashStrategy) keyExpr(side int) string {
	exprs := make([]string, len(h.p.key))
	for i, k := range h.p.key {
		exprs[i] = k.expr(side)
	}
	return "ROW(" + strings.Join(exprs, ", ") + ")::text"
}

// rangeConds restricts the first key column to range r on one side. The
// bounds are inlined as literals of the column type of that side, so that
// range queries can be planned and checked before the run.
func (h *hashStrategy) rangeConds(side int, r textRange) []string {
	k := h.p.key[0]
	col := pgx.Identifier{k.name}.Sanitize()
	lit := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'::" + k.base[side] }
	var c []string
	if r.lo != nil {
		c = append(c, col+" >= "+lit(*r.lo))
	}
	if r.hi != nil {
		c = append(c, col+" < "+lit(*r.hi))
	}
	return c
}

// refQuery reads range r of the reference side in key order: the key text,
// each key column as text in its bound type (the block bounds), and the
// row text.
func (h *hashStrategy) refQuery(r textRange) string {
	cols := []string{h.keyExpr(0)}
	for _, k := range h.p.key {
		cols = append(cols, pgx.Identifier{k.name}.Sanitize()+"::"+k.base[0]+"::text")
	}
	cols = append(cols, rowExpr(h.p, 0))
	return h.selectFrom(0, cols, h.rangeConds(0, r))
}

// testQuery reads one block of range r on the test side: lo and hi say
// whether the block has a lower (exclusive) and an upper (inclusive) bound.
// The lower bound takes parameters $1..$n, the upper one the next n.
func (h *hashStrategy) testQuery(r textRange, lo, hi bool) string {
	conds := h.rangeConds(1, r)
	next := 1
	if lo {
		conds = append(conds, h.bound(">", next))
		next += len(h.p.key)
	}
	if hi {
		conds = append(conds, h.bound("<=", next))
	}
	return h.selectFrom(1, []string{h.keyExpr(1), rowExpr(h.p, 1)}, conds)
}

// bound compares the key with parameters $first.., each cast to the type
// of its column on the test side. A composite key uses a row comparison,
// which a btree index on the key serves directly.
func (h *hashStrategy) bound(op string, first int) string {
	cols := make([]string, len(h.p.key))
	params := make([]string, len(h.p.key))
	for i, k := range h.p.key {
		cols[i] = pgx.Identifier{k.name}.Sanitize()
		params[i] = fmt.Sprintf("$%d::%s", first+i, k.base[1])
	}
	if len(cols) == 1 {
		return cols[0] + " " + op + " " + params[0]
	}
	return "(" + strings.Join(cols, ", ") + ") " + op + " (" + strings.Join(params, ", ") + ")"
}

func (h *hashStrategy) selectFrom(side int, cols, conds []string) string {
	if h.filter != "" {
		conds = append([]string{"(" + h.filter + ")"}, conds...)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s FROM %s", strings.Join(cols, ", "), h.tables[side])
	if len(conds) > 0 {
		fmt.Fprintf(&b, " WHERE %s", strings.Join(conds, " AND "))
	}
	// Key columns in ORDER BY are qualified with the table: a bare name
	// would refer to an output column first, and the reference query has
	// output columns named after the key columns ("a"::text is named "a"),
	// so the rows would be sorted by the text of the key.
	keys := make([]string, len(h.p.key))
	for i, k := range h.p.key {
		keys[i] = h.tables[side] + "." + pgx.Identifier{k.name}.Sanitize()
	}
	fmt.Fprintf(&b, " ORDER BY %s", strings.Join(keys, ", "))
	return b.String()
}

func (h *hashStrategy) query(side int) string {
	if side == 0 {
		return h.refQuery(textRange{})
	}
	return h.testQuery(textRange{}, false, false)
}

// planQueries: one query per range. Block bounds are parameters and are
// left out; if a range query reads the key index, a block of it does too.
func (h *hashStrategy) planQueries(side int) []string {
	qs := make([]string, len(h.ranges))
	for i, r := range h.ranges {
		if side == 0 {
			qs[i] = h.refQuery(r)
		} else {
			qs[i] = h.testQuery(r, false, false)
		}
	}
	return qs
}

func (h *hashStrategy) run(ctx context.Context, ref, tst *side, w io.Writer) (stats, error) {
	var st stats
	var err error
	if len(h.ranges) == 1 {
		st, err = h.compareRange(ctx, ref, tst, 0, w)
	} else {
		st, err = runParallel(ctx, h.opt, ref, tst, len(h.ranges), h.compareRange, filepath.Dir(h.opt.out), w)
	}
	if err == nil {
		log.Printf("blocks: %d", st.blocks)
	}
	return st, err
}

// compareRange runs the block loop over range i in one pair of sessions.
func (h *hashStrategy) compareRange(ctx context.Context, ref, tst *side, i int, w io.Writer) (stats, error) {
	var st stats
	r := h.ranges[i]
	rows, err := ref.tx.Query(ctx, h.refQuery(r))
	if err != nil {
		return st, fmt.Errorf("%s: query: %w", ref.name, err)
	}
	defer rows.Close()

	// vals: key text, key columns, row text of the current reference row.
	nk := len(h.p.key)
	vals := make([]string, nk+2)
	dest := make([]any, len(vals))
	for i := range vals {
		dest[i] = &vals[i]
	}
	next := func() (bool, error) {
		if rows.Next() {
			if err := rows.Scan(dest...); err != nil {
				return false, fmt.Errorf("%s: scan: %w", ref.name, err)
			}
			return true, nil
		}
		if err := rows.Err(); err != nil {
			return false, fmt.Errorf("%s: read: %w", ref.name, err)
		}
		return false, nil
	}

	b := newBlock(int64(h.mem), h.opt.blockRows)
	var lo, hi []string // block bounds; nil lo: no lower bound
	have, err := next() // vals holds a row not yet put into a block
	if err != nil {
		return st, err
	}
	for {
		st.blocks++
		b.reset()
		for have && !b.full() {
			if err := b.add(vals[0], vals[nk+1]); err != nil {
				return st, fmt.Errorf("%s: %w", ref.name, err)
			}
			hi = append(hi[:0], vals[1:nk+1]...)
			if have, err = next(); err != nil {
				return st, err
			}
		}
		st.rows1 += uint64(len(b.rows))

		// After the last reference row the block stays open to the right:
		// the rest of the range on the test side belongs to it.
		upper := hi
		if !have {
			upper = nil
		}
		if err := h.probe(ctx, tst, b, r, lo, upper, w, &st); err != nil {
			return st, err
		}
		if err := b.finish(w, &st); err != nil {
			return st, err
		}
		if !have {
			return st, nil
		}
		lo = append(lo[:0], hi...)
	}
}

// probe reads the test rows of block (lo, hi] in range r and matches them
// against the block. A nil bound is open.
func (h *hashStrategy) probe(ctx context.Context, tst *side, b *block, r textRange, lo, hi []string, w io.Writer, st *stats) error {
	args := make([]any, 0, len(lo)+len(hi))
	for _, v := range lo {
		args = append(args, v)
	}
	for _, v := range hi {
		args = append(args, v)
	}
	// A Go string parameter goes in text format, so the server parses the
	// bound with the input function of the type it is cast to.
	rows, err := tst.tx.Query(ctx, h.testQuery(r, lo != nil, hi != nil), args...)
	if err != nil {
		return fmt.Errorf("%s: query: %w", tst.name, err)
	}
	defer rows.Close()
	var key, data string
	for rows.Next() {
		if err := rows.Scan(&key, &data); err != nil {
			return fmt.Errorf("%s: scan: %w", tst.name, err)
		}
		st.rows2++
		if err := b.probe(key, data, w, st); err != nil {
			return fmt.Errorf("%s: %w", tst.name, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%s: read: %w", tst.name, err)
	}
	return nil
}

// rowOverhead approximates what one block row costs besides the bytes of
// its strings: the row itself, the map entry and the matched flag.
const rowOverhead = 96

// block is a set of reference rows in key order, with a hash index by key
// text.
//
// Report order inside a block: mismatches and test-only rows in the order
// of the test side (by key), then reference-only rows in key order.
type block struct {
	limit   int64 // bytes; a block always takes at least one row
	maxRows int   // rows; 0 = no limit. Whichever limit is reached first closes the block
	size    int64
	rows    []refRow
	matched []bool
	index   map[string]int
}

type refRow struct{ key, data string }

func newBlock(limit int64, maxRows int) *block {
	return &block{limit: limit, maxRows: maxRows, index: make(map[string]int, min(maxRows, 1<<16))}
}

func (b *block) reset() {
	b.size = 0
	clear(b.rows) // drop references to the strings of the previous block
	b.rows = b.rows[:0]
	b.matched = b.matched[:0]
	clear(b.index)
}

func (b *block) full() bool {
	return len(b.rows) > 0 && (b.size >= b.limit || (b.maxRows > 0 && len(b.rows) >= b.maxRows))
}

func (b *block) add(key, data string) error {
	if _, dup := b.index[key]; dup {
		return fmt.Errorf("key %s occurs twice", key)
	}
	b.index[key] = len(b.rows)
	b.rows = append(b.rows, refRow{key, data})
	b.matched = append(b.matched, false)
	b.size += int64(len(key)+len(data)) + rowOverhead
	return nil
}

// probe matches one test row and reports it if it differs or is absent
// from the block.
func (b *block) probe(key, data string, w io.Writer, st *stats) error {
	i, ok := b.index[key]
	switch {
	case !ok:
		st.onlyRight++
		return writeDiff(w, key, "NULL", data)
	case b.matched[i]:
		return fmt.Errorf("key %s occurs twice", key)
	}
	b.matched[i] = true
	if b.rows[i].data != data {
		st.differ++
		return writeDiff(w, key, b.rows[i].data, data)
	}
	return nil
}

// finish reports the block rows no test row matched.
func (b *block) finish(w io.Writer, st *stats) error {
	for i, r := range b.rows {
		if !b.matched[i] {
			st.onlyLeft++
			if err := writeDiff(w, r.key, r.data, "NULL"); err != nil {
				return err
			}
		}
	}
	return nil
}

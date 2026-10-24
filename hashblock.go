package main

import (
	"context"
	"fmt"
	"io"
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
// The loop runs over one key range (ranges.go): the whole key space in a
// sequential run, or one of the ranges parallel workers take in their own
// pairs of sessions. The first block of a range has the lower end of the
// range as its lower bound, and the last one its upper end.
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
}

func newHashStrategy(opt *options, d1, d2 *description, p *plan) *hashStrategy {
	return &hashStrategy{
		opt:    opt,
		tables: [2]string{d1.qualifiedName(), d2.qualifiedName()},
		p:      p,
		filter: strings.TrimSpace(opt.filter),
		mem:    opt.blockMem,
	}
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

// keyCols are the quoted key columns, in key order.
func (h *hashStrategy) keyCols() []string {
	cols := make([]string, len(h.p.key))
	for i, k := range h.p.key {
		cols[i] = pgx.Identifier{k.name}.Sanitize()
	}
	return cols
}

// rangeConds restricts the key to range r on one side. The ends are
// inlined as literals of the key column types of that side, so that range
// queries can be planned and checked before the run.
func (h *hashStrategy) rangeConds(side int, r keyRange) []string {
	return r.conditions(h.keyCols(), func(i int, v string) string {
		return quoteLiteral(v) + "::" + h.p.key[i].base[side]
	})
}

// refQuery reads range r of the reference side in key order: the key text,
// each key column as text in its bound type (the block bounds), and the
// row text.
func (h *hashStrategy) refQuery(r keyRange) string {
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
func (h *hashStrategy) testQuery(r keyRange, lo, hi bool) string {
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

// bound compares the whole key with parameters $first.., each cast to the
// type of its column on the test side.
func (h *hashStrategy) bound(op string, first int) string {
	return keyCondition(h.keyCols(), op, make([]string, len(h.p.key)), func(i int, _ string) string {
		return fmt.Sprintf("$%d::%s", first+i, h.p.key[i].base[1])
	})
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

// rangeQuery: on the test side, the query of the whole range. Block
// bounds are parameters and are left out; if a range query reads the key
// index, a block of it does too.
func (h *hashStrategy) rangeQuery(side int, r keyRange) string {
	if side == 0 {
		return h.refQuery(r)
	}
	return h.testQuery(r, false, false)
}

// compareRange runs the block loop over range r in one pair of sessions.
func (h *hashStrategy) compareRange(ctx context.Context, ref, tst *side, r keyRange, w io.Writer) (stats, error) {
	var st stats
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
func (h *hashStrategy) probe(ctx context.Context, tst *side, b *block, r keyRange, lo, hi []string, w io.Writer, st *stats) error {
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

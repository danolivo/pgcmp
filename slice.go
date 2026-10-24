package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

// pgcmp slice builds the block map of a table on one server and stores it
// in a table there (issue #1): the same cut of the key space that parallel
// workers get, kept for later runs and for hashing.
//
// Two sessions on the same server: the slicer reads the key in a read-only
// REPEATABLE READ transaction, which fixes the snapshot of the map; the
// writer, outside that transaction, stores the map, since the slicer's own
// transaction cannot write.

type sliceOptions struct {
	dsn      string
	table    string
	mapName  string
	filter   string
	rows     int      // rows in one block; 0 = estimated from blockMem
	blockMem byteSize // memory for one block, to estimate rows
	replace  bool     // replace an existing map
}

func parseSlice(args []string) (*sliceOptions, error) {
	fs := flag.NewFlagSet("pgcmp slice", flag.ContinueOnError)
	var opt sliceOptions
	fs.StringVar(&opt.dsn, "dsn", "", "connection string of the server to build the map on")
	fs.StringVar(&opt.table, "table", "", "table name, optionally schema-qualified")
	fs.StringVar(&opt.mapName, "map", "", "table to store the block map in, optionally schema-qualified")
	fs.StringVar(&opt.filter, "filter", "", "WHERE expression: cut only the rows that match")
	fs.IntVar(&opt.rows, "rows", 0, "rows in one block; 0 = estimated from -mem")
	opt.blockMem = 64 << 20
	fs.Var(&opt.blockMem, "mem", "memory for one block, to estimate -rows, e.g. 64MB or 512kB; a bare number is MB")
	fs.BoolVar(&opt.replace, "replace", false, "replace the map if it exists")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, usageError("slice: unexpected arguments: " + strings.Join(fs.Args(), " "))
	}
	if opt.dsn == "" || opt.table == "" || opt.mapName == "" {
		return nil, usageError("slice: -dsn, -table and -map are required")
	}
	if opt.rows < 0 {
		return nil, usageError("slice: -rows must be at least 0")
	}
	return &opt, nil
}

func runSlice(args []string) int {
	opt, err := parseSlice(args)
	if err != nil {
		return parseFailed(err)
	}
	ctx, stop := signalContext()
	defer stop()
	if err := buildMap(ctx, opt); err != nil {
		log.Printf("error: %v", err)
		return exitError
	}
	return exitMatch
}

// buildMap cuts the table into blocks and stores the map (blockmap.go):
//
//  1. the slicer session reads the key in a read-only REPEATABLE READ
//     transaction: the snapshot of the map;
//  2. the writer session copies the blocks, as they come, into a temporary
//     table; the rows of the last block are counted at the end, through an
//     index scan of the tail in the slicer's snapshot;
//  3. the writer creates the map in one transaction (replacing the old one
//     with -replace), fills it from the temporary table, indexes it and
//     comments it with its meta.
//
// The map appears whole or not at all: on any error before the commit
// there is no map, and the temporary table goes with the session.
func buildMap(ctx context.Context, opt *sliceOptions) error {
	start := time.Now()
	slicerSide, err := openSide(ctx, "slicer", opt.dsn, "")
	if err != nil {
		return err
	}
	defer slicerSide.close()
	d, err := slicerSide.describe(ctx, opt.table)
	if err != nil {
		return err
	}
	writer, err := connect(ctx, "writer", opt.dsn)
	if err != nil {
		return err
	}
	defer writer.close()

	// Check the target before the pass over the index, not after.
	target, err := mapTarget(ctx, writer, opt)
	if err != nil {
		return err
	}

	cols := make([]string, len(d.columns))
	for i, c := range d.columns {
		cols[i] = c.name
	}
	// Nobody waits for the blocks here: big batches.
	sl, how, err := newSlicer(ctx, slicerSide, d, cols, opt.filter, opt.rows, opt.blockMem, 64)
	if err != nil {
		return err
	}
	if w, err := sl.checkPlan(ctx, slicerSide); err != nil {
		return err
	} else if w != "" {
		log.Print(w)
	}
	log.Printf("slice: blocks of %d rows (%s) from an index scan of (%s) into %s", sl.rows, how, keyList(d.pk), target)
	meta := mapMeta{
		Format: mapFormat, Table: d.qualifiedName(), Key: keyMeta(sl.key), Rows: sl.rows,
		Filter: strings.TrimSpace(opt.filter), Server: slicerSide.version, Pgcmp: pgcmpRevision(),
	}
	if err := slicerSide.tx.QueryRow(ctx, "SELECT pg_catalog.pg_current_snapshot()::text").Scan(&meta.Snapshot); err != nil {
		return fmt.Errorf("%s: snapshot: %w", slicerSide.name, err)
	}

	if _, err := writer.conn.Exec(ctx,
		"CREATE TEMP TABLE pgcmp_map (seq bigint, lo text[], hi text[], rows bigint)"); err != nil {
		return fmt.Errorf("%s: %w", writer.name, err)
	}
	if meta.Blocks, meta.Total, err = copyBlocks(ctx, sl, slicerSide, writer); err != nil {
		return err
	}
	slicerSide.close() // done with the snapshot

	meta.Built = time.Now().UTC().Round(time.Second)
	if err := createMap(ctx, writer, target, opt.replace, meta); err != nil {
		return err
	}
	log.Printf("map %s: %d blocks, %d rows, in %s", target, meta.Blocks, meta.Total, time.Since(start).Round(time.Millisecond))
	return nil
}

// mapTarget resolves -map on the writer side: the quoted name of the map
// to create. It refuses a name that exists unless -replace is given, and
// even then anything but a block map; and it checks the right to create a
// table in the schema and a temporary one in the database.
func mapTarget(ctx context.Context, w *side, opt *sliceOptions) (string, error) {
	var parts []string
	if err := w.conn.QueryRow(ctx, "SELECT pg_catalog.parse_ident($1)", opt.mapName).Scan(&parts); err != nil {
		return "", fmt.Errorf("%s: -map %q: %w", w.name, opt.mapName, err)
	}
	switch len(parts) {
	case 1:
		parts = []string{"public", parts[0]}
	case 2:
	default:
		return "", fmt.Errorf("-map %q: want [schema.]table", opt.mapName)
	}
	target := pgx.Identifier(parts).Sanitize()

	var exists bool
	if err := w.conn.QueryRow(ctx, "SELECT pg_catalog.to_regclass($1) IS NOT NULL", target).Scan(&exists); err != nil {
		return "", fmt.Errorf("%s: -map %s: %w", w.name, target, err)
	}
	if exists {
		if !opt.replace {
			return "", fmt.Errorf("%s: %s exists; -replace replaces it", w.name, target)
		}
		// -replace drops a block map, and nothing else.
		if _, _, err := w.mapMetaOf(ctx, w.conn, target); err != nil {
			return "", fmt.Errorf("%w; -replace drops only a block map", err)
		}
	}

	var create, temp bool
	err := w.conn.QueryRow(ctx, `SELECT pg_catalog.has_schema_privilege($1, 'CREATE'),
		pg_catalog.has_database_privilege(pg_catalog.current_database(), 'TEMP')`, parts[0]).Scan(&create, &temp)
	switch {
	case err != nil:
		return "", fmt.Errorf("%s: schema of -map %s: %w", w.name, target, err)
	case !create:
		return "", fmt.Errorf("%s: no CREATE privilege on schema %q for the map", w.name, parts[0])
	case !temp:
		return "", fmt.Errorf("%s: no TEMP privilege on the database for the map", w.name)
	}
	return target, nil
}

// copyBlocks runs the slicer and copies its blocks into the temporary
// table of the writer as they come: N rows each, and the rest in the last
// one, counted once the slicer has closed its cursor. It returns the
// number of blocks and of rows.
func copyBlocks(ctx context.Context, sl *slicer, slicerSide, writer *side) (blocks, total int64, err error) {
	g, gctx := errgroup.WithContext(ctx)
	ch := make(chan keyRange)
	g.Go(func() error {
		defer close(ch)
		return sl.feed(slicerSide)(gctx, ch)
	})
	g.Go(func() error {
		src := &blockSource{ctx: gctx, ch: ch, sl: sl, side: slicerSide}
		n, err := writer.conn.CopyFrom(gctx, pgx.Identifier{"pg_temp", "pgcmp_map"},
			[]string{"seq", "lo", "hi", "rows"}, src)
		if err != nil {
			return fmt.Errorf("%s: copy the map: %w", writer.name, err)
		}
		blocks, total = n, src.total
		return nil
	})
	return blocks, total, g.Wait()
}

// blockSource turns the blocks of the slicer into rows of the map.
type blockSource struct {
	ctx   context.Context
	ch    <-chan keyRange
	sl    *slicer
	side  *side
	seq   int64
	total int64
	row   []any
	err   error
}

func (b *blockSource) Next() bool {
	var r keyRange
	var ok bool
	select {
	case r, ok = <-b.ch:
	case <-b.ctx.Done():
		b.err = b.ctx.Err()
		return false
	}
	if !ok {
		return false // the slicer failed: its error ends the copy
	}
	rows := int64(b.sl.rows)
	if r.hi.vals == nil {
		// The last block. The slicer sends nothing more and closes the
		// channel once it has closed its cursor; then its session is free
		// for the count.
		if _, more := <-b.ch; more {
			b.err = errors.New("slicer: a block after the last one")
			return false
		}
		if rows, b.err = b.sl.countAfter(b.ctx, b.side, r.lo.vals); b.err != nil {
			return false
		}
	}
	b.row = []any{b.seq, r.lo.vals, r.hi.vals, rows}
	b.seq++
	b.total += rows
	return true
}

func (b *blockSource) Values() ([]any, error) { return b.row, nil }
func (b *blockSource) Err() error             { return b.err }

// createMap creates the map from the temporary table in one transaction
// and checks it once committed: whole, or an error.
func createMap(ctx context.Context, w *side, target string, replace bool, meta mapMeta) error {
	comment, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	tx, err := w.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", w.name, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // a no-op after Commit
	stmts := []string{
		"CREATE TABLE " + target + " (seq bigint NOT NULL, lo text[], hi text[], rows bigint NOT NULL, hash bytea)",
		"INSERT INTO " + target + " (seq, lo, hi, rows) SELECT seq, lo, hi, rows FROM pg_temp.pgcmp_map ORDER BY seq",
		"CREATE UNIQUE INDEX ON " + target + " (seq)",
		"COMMENT ON TABLE " + target + " IS " + quoteLiteral(string(comment)),
	}
	if replace {
		stmts = append([]string{"DROP TABLE IF EXISTS " + target}, stmts...)
	}
	for _, q := range stmts {
		if _, err := tx.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: create the map: %w", w.name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%s: create the map: %w", w.name, err)
	}

	var n, last, total int64
	if err := w.conn.QueryRow(ctx, "SELECT count(*), coalesce(max(seq), -1), coalesce(sum(rows), 0)::bigint FROM "+target).
		Scan(&n, &last, &total); err != nil {
		return fmt.Errorf("%s: check the map: %w", w.name, err)
	}
	if n != meta.Blocks || last+1 != n || total != meta.Total {
		return fmt.Errorf("%s: the map %s is incomplete: %d blocks (last %d), %d rows; expected %d blocks, %d rows",
			w.name, target, n, last, total, meta.Blocks, meta.Total)
	}
	return nil
}

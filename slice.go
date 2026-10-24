package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"strings"
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

// errSliceNotImplemented: the commands, sessions and key are in place, the
// cutting itself is not (issue #1).
var errSliceNotImplemented = errors.New("slice: cutting the key space is not implemented yet")

func buildMap(ctx context.Context, opt *sliceOptions) error {
	slicer, err := openSide(ctx, "slicer", opt.dsn, "")
	if err != nil {
		return err
	}
	defer slicer.close()
	d, err := slicer.describe(ctx, opt.table)
	if err != nil {
		return err
	}
	key, err := keyOf(d)
	if err != nil {
		return err
	}
	types := make([]string, len(key))
	for i, k := range key {
		types[i] = k.bound
	}
	log.Printf("key: (%s) [%s]", keyList(d.pk), strings.Join(types, ", "))

	writer, err := connect(ctx, "writer", opt.dsn)
	if err != nil {
		return err
	}
	defer writer.close()
	return errSliceNotImplemented
}

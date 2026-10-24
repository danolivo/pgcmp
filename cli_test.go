package main

import (
	"errors"
	"flag"
	"os"
	"testing"
)

// quietParse runs a parser with stderr silenced: the flag package prints
// its own errors.
func quietParse[T any](t *testing.T, parse func([]string) (T, error), args ...string) (T, error) {
	t.Helper()
	stderr := os.Stderr
	devnull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = devnull
	defer func() { os.Stderr = stderr; devnull.Close() }()
	return parse(args)
}

func TestParseCompare(t *testing.T) {
	opt, err := quietParse(t, parseCompare, "-dsn1", "a", "-dsn2", "b", "-table", "t", "-columns", " x, y ,", "-j", "4")
	if err != nil {
		t.Fatal(err)
	}
	if opt.jobs != 4 || len(opt.columns) != 2 || opt.columns[1] != "y" || opt.blockMem != 64<<20 || opt.out != "pgcmp.out" {
		t.Fatalf("options: %+v", opt)
	}

	var ue usageError
	for _, args := range [][]string{
		{"-dsn1", "a", "-table", "t"},                                // no -dsn2
		{"-dsn1", "a", "-dsn2", "b", "-table", "t", "-j", "0"},       // no workers
		{"-dsn1", "a", "-dsn2", "b", "-table", "t", "extra", "-j=2"}, // stray argument
	} {
		if _, err := quietParse(t, parseCompare, args...); !errors.As(err, &ue) {
			t.Errorf("%q: got %v, want a usage error", args, err)
		}
	}
	if _, err := quietParse(t, parseCompare, "-nosuchflag"); err == nil || errors.As(err, &ue) {
		t.Errorf("unknown flag: got %v, want an error of the flag package", err)
	}
	if _, err := quietParse(t, parseCompare, "-h"); !errors.Is(err, flag.ErrHelp) {
		t.Errorf("-h: got %v", err)
	}
}

func TestParseSlice(t *testing.T) {
	opt, err := quietParse(t, parseSlice, "-dsn", "a", "-table", "t", "-map", "m", "-rows", "1000", "-replace")
	if err != nil {
		t.Fatal(err)
	}
	if opt.rows != 1000 || !opt.replace || opt.mapName != "m" || opt.blockMem != 64<<20 {
		t.Fatalf("options: %+v", opt)
	}
	var ue usageError
	for _, args := range [][]string{
		{"-dsn", "a", "-table", "t"}, // no -map
		{"-dsn", "a", "-table", "t", "-map", "m", "-rows", "-1"},
		{"-dsn", "a", "-table", "t", "-map", "m", "extra"},
	} {
		if _, err := quietParse(t, parseSlice, args...); !errors.As(err, &ue) {
			t.Errorf("%q: got %v, want a usage error", args, err)
		}
	}
	// Flags of the comparison are not flags of slice.
	if _, err := quietParse(t, parseSlice, "-dsn1", "a"); err == nil {
		t.Error("-dsn1 accepted by slice")
	}
}

func TestKeyOf(t *testing.T) {
	d := &description{side: "reference", schema: "pg_catalog", name: "pg_aggregate", pk: []column{
		{name: "aggfnoid", typeOID: 24, typeName: "regproc", baseType: "regproc"},
		{name: "n", typeOID: 1700, typeName: "numeric(10,2)", baseType: "numeric"},
	}}
	key, err := keyOf(d)
	if err != nil {
		t.Fatal(err)
	}
	// regproc does not read its own text back: bounds go through the
	// sibling. Any other type: the type without its modifier.
	if key[0].bound != "regprocedure" || key[1].bound != "numeric" || key[1].typeName != "numeric(10,2)" {
		t.Fatalf("key: %+v", key)
	}
	d.pk = nil
	if _, err := keyOf(d); err == nil {
		t.Fatal("a table with no primary key: no error")
	}
}

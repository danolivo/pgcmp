package main

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func testSlicer(rows int) *slicer {
	return &slicer{
		table: `"public"."t"`,
		key: []keyColumn{
			{column: column{name: "f", typeOID: 24, baseType: "regproc"}, bound: "regprocedure"},
			{column: column{name: "Name", typeOID: 25, baseType: "text"}, bound: "text"},
		},
		rows:     rows,
		maxBatch: 8,
	}
}

func TestSlicerCursorQuery(t *testing.T) {
	s := testSlicer(1000)
	s.filter = " v <> 'x' "
	// Bare key columns, except the one whose text does not read back; the
	// order qualified with the table, never by the output column "f".
	want := `DECLARE pgcmp_slice NO SCROLL CURSOR FOR SELECT "f"::regprocedure, "Name" FROM "public"."t"` +
		` WHERE (v <> 'x') ORDER BY "public"."t"."f", "public"."t"."Name"`
	if got := s.declare(); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestSlicerBatch(t *testing.T) {
	if got, want := testSlicer(1000).batch(2),
		"MOVE FORWARD 999 IN pgcmp_slice; FETCH FORWARD 1 FROM pgcmp_slice; "+
			"MOVE FORWARD 999 IN pgcmp_slice; FETCH FORWARD 1 FROM pgcmp_slice; "; got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
	// N = 1: nothing to skip; MOVE FORWARD 0 would re-read the current row.
	if got, want := testSlicer(1).batch(2),
		"FETCH FORWARD 1 FROM pgcmp_slice; FETCH FORWARD 1 FROM pgcmp_slice; "; got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}

func move(n string) *pgconn.Result {
	return &pgconn.Result{CommandTag: pgconn.NewCommandTag("MOVE " + n)}
}

func fetch(vals ...string) *pgconn.Result {
	r := &pgconn.Result{CommandTag: pgconn.NewCommandTag("FETCH 0")}
	if vals != nil {
		r.CommandTag = pgconn.NewCommandTag("FETCH 1")
		row := make([][]byte, len(vals))
		for i, v := range vals {
			row[i] = []byte(v)
		}
		r.Rows = [][][]byte{row}
	}
	return r
}

func TestSlicerTakeBounds(t *testing.T) {
	s := testSlicer(100)
	cases := []struct {
		name string
		res  []*pgconn.Result
		want string // bounds, "|"-separated; "$" at the end: the end of the table
	}{
		{"full batch", []*pgconn.Result{move("99"), fetch("a(int)", "x"), move("99"), fetch("b(int)", "y")}, "a(int),x|b(int),y"},
		{"short MOVE: the end", []*pgconn.Result{move("99"), fetch("a(int)", "x"), move("42"), fetch(), move("0"), fetch()}, "a(int),x$"},
		{"full MOVE, empty FETCH: the end", []*pgconn.Result{move("99"), fetch()}, "$"},
		{"nothing at all", []*pgconn.Result{move("0"), fetch()}, "$"},
	}
	for _, c := range cases {
		bounds, end, err := s.takeBounds(c.res)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		var parts []string
		for _, b := range bounds {
			parts = append(parts, strings.Join(b, ","))
		}
		got := strings.Join(parts, "|")
		if end {
			got += "$"
		}
		if got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}

	// N = 1: FETCH only.
	bounds, end, err := testSlicer(1).takeBounds([]*pgconn.Result{fetch("a(int)", "x"), fetch()})
	if err != nil || len(bounds) != 1 || !end {
		t.Fatalf("N = 1: %v %v %v", bounds, end, err)
	}

	for name, res := range map[string][]*pgconn.Result{
		"FETCH where MOVE belongs": {fetch("a(int)", "x")},
		"MOVE without its FETCH":   {move("99")},
		"too few columns":          {move("99"), fetch("a(int)")},
		"MOVE where FETCH belongs": {move("99"), move("99")},
		"NULL in a key column":     {move("99"), {CommandTag: pgconn.NewCommandTag("FETCH 1"), Rows: [][][]byte{{[]byte("a"), nil}}}},
	} {
		if _, _, err := s.takeBounds(res); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

// k bounds make k+1 blocks that cover the key space with no gaps or
// overlaps: each block starts right after the bound the previous one ended
// at, the first is open below and the last open above.
func TestBlockAfter(t *testing.T) {
	for k := range 4 {
		var blocks []keyRange
		var prev []string
		for i := range k {
			b := []string{string(rune('a' + i))}
			blocks = append(blocks, blockAfter(prev, b))
			prev = b
		}
		blocks = append(blocks, blockAfter(prev, nil))
		var parts []string
		for _, r := range blocks {
			parts = append(parts, showRange(r))
		}
		got := strings.Join(parts, " ")
		want := map[int]string{
			0: "[]",
			1: "[a <= a] [a > a]",
			2: "[a <= a] [a > a AND a <= b] [a > b]",
			3: "[a <= a] [a > a AND a <= b] [a > b AND a <= c] [a > c]",
		}[k]
		if got != want {
			t.Errorf("%d bounds: got %s, want %s", k, got, want)
		}
	}
}

func TestRowsForWidth(t *testing.T) {
	cases := []struct {
		mem   byteSize
		width float64
		want  int
	}{
		{64 << 20, 160, 64 << 20 / 256}, // 160 bytes of text + 96 of overhead
		{1 << 10, 4000, minSliceRows},   // never fewer than minSliceRows
		{1 << 40, 0, maxSliceRows},      // never more than maxSliceRows
	}
	for _, c := range cases {
		if got := rowsForWidth(c.mem, c.width); got != c.want {
			t.Errorf("rowsForWidth(%s, %v) = %d, want %d", c.mem, c.width, got, c.want)
		}
	}
}

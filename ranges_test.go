package main

import (
	"context"
	"strings"
	"testing"
)

// showRange prints a range as its conditions on columns a, b, ..., with
// the values inlined as they are.
func showRange(r keyRange) string {
	c := r.conditions([]string{"a", "b", "c"}, func(_ int, v string) string { return v })
	return "[" + strings.Join(c, " AND ") + "]"
}

func TestHistogramRanges(t *testing.T) {
	cases := []struct {
		bounds []string
		want   string
	}{
		{nil, "[]"},
		{[]string{"7"}, "[]"},
		{[]string{"1", "9"}, "[]"}, // one bin: the whole key space
		{[]string{"1", "5", "9"}, "[a < 5] [a >= 5]"},
		{[]string{"1", "5", "7", "9"}, "[a < 5] [a >= 5 AND a < 7] [a >= 7]"},
	}
	for _, c := range cases {
		var parts []string
		for _, r := range histogramRanges(c.bounds) {
			parts = append(parts, showRange(r))
		}
		if got := strings.Join(parts, " "); got != c.want {
			t.Errorf("bounds %v: got %s, want %s", c.bounds, got, c.want)
		}
	}
}

func TestKeyRangeConditions(t *testing.T) {
	cases := []struct {
		r    keyRange
		want string
	}{
		{keyRange{}, "[]"},
		// Ends on the whole key: row comparisons.
		{keyRange{lo: keyBound{vals: []string{"1", "x"}}, hi: keyBound{vals: []string{"2", "y"}, inclusive: true}},
			"[(a, b) > (1, x) AND (a, b) <= (2, y)]"},
		{keyRange{lo: keyBound{vals: []string{"1", "x", "z"}, inclusive: true}},
			"[(a, b, c) >= (1, x, z)]"},
		// An end on a prefix of the key constrains only that prefix.
		{keyRange{lo: keyBound{vals: []string{"1"}, inclusive: true}, hi: keyBound{vals: []string{"2", "y"}}},
			"[a >= 1 AND (a, b) < (2, y)]"},
	}
	for _, c := range cases {
		if got := showRange(c.r); got != c.want {
			t.Errorf("%+v: got %s, want %s", c.r, got, c.want)
		}
	}
	if !(keyRange{}).unbounded() || histogramRanges([]string{"1", "5", "9"})[0].unbounded() {
		t.Error("unbounded() is wrong")
	}
}

func TestKeySplit(t *testing.T) {
	feed := func(context.Context, chan<- keyRange) error { return nil }
	cases := []struct {
		name          string
		s             keySplit
		jobs          int
		inCoordinator bool
		workers       int
		planRanges    int
	}{
		{"sequential", keySplit{ranges: []keyRange{{}}}, 1, true, 1, 1},
		{"no histogram, -j 4", keySplit{ranges: []keyRange{{}}}, 4, true, 1, 1},
		{"histogram, -j 4", keySplit{ranges: histogramRanges([]string{"1", "5", "7", "9"})}, 4, false, 3, 3},
		{"histogram, -j 2", keySplit{ranges: histogramRanges([]string{"1", "5", "7", "9"})}, 2, false, 2, 3},
		// A feed may hold the coordinator transaction: workers even at -j 1,
		// and the plan of the whole key space is checked.
		{"feed, -j 1", keySplit{feed: feed}, 1, false, 1, 1},
		{"feed, -j 8", keySplit{feed: feed}, 8, false, 8, 1},
	}
	for _, c := range cases {
		if got := c.s.inCoordinator(); got != c.inCoordinator {
			t.Errorf("%s: inCoordinator = %v", c.name, got)
		}
		if got := c.s.workers(c.jobs); got != c.workers {
			t.Errorf("%s: workers = %d, want %d", c.name, got, c.workers)
		}
		if got := len(c.s.planRanges()); got != c.planRanges {
			t.Errorf("%s: %d plan ranges, want %d", c.name, got, c.planRanges)
		}
	}
	if r := (keySplit{feed: feed}).planRanges()[0]; !r.unbounded() {
		t.Errorf("feed: plan range %+v, want the whole key space", r)
	}
}

// A block of the slicer, (lo, hi] on the whole key, needs nothing new from
// either strategy.
func TestSliceBlockQueries(t *testing.T) {
	block := keyRange{lo: keyBound{vals: []string{"5"}}, hi: keyBound{vals: []string{"10"}, inclusive: true}}
	idKey := []keyCol{{colPlan: colPlan{name: "id"}, base: [2]string{"integer", "bigint"}}}
	got := buildQuery(`"t"`, &plan{key: idKey, intKey: true}, 0, "", block)
	want := `SELECT "id", '()'::text FROM "t" WHERE "id" > 5 AND "id" <= 10 ORDER BY "id"`
	if got != want {
		t.Fatalf("merge:\ngot  %s\nwant %s", got, want)
	}

	h := &hashStrategy{tables: [2]string{`"r"`, `"t"`}, p: &plan{key: []keyCol{
		{colPlan: colPlan{name: "a"}, base: [2]string{"integer", "bigint"}},
		{colPlan: colPlan{name: "b"}, base: [2]string{"character varying", "text"}},
	}}}
	block = keyRange{lo: keyBound{vals: []string{"1", "x"}}, hi: keyBound{vals: []string{"2", "it's"}, inclusive: true}}
	got = h.rangeQuery(1, block)
	want = `SELECT ROW("a", "b")::text, '()'::text FROM "t"` +
		` WHERE ("a", "b") > ('1'::bigint, 'x'::text) AND ("a", "b") <= ('2'::bigint, 'it''s'::text)` +
		` ORDER BY "t"."a", "t"."b"`
	if got != want {
		t.Fatalf("hash, test side:\ngot  %s\nwant %s", got, want)
	}
	got = h.rangeQuery(0, block)
	want = `SELECT ROW("a", "b")::text, "a"::integer::text, "b"::character varying::text, '()'::text FROM "r"` +
		` WHERE ("a", "b") > ('1'::integer, 'x'::character varying) AND ("a", "b") <= ('2'::integer, 'it''s'::character varying)` +
		` ORDER BY "r"."a", "r"."b"`
	if got != want {
		t.Fatalf("hash, reference side:\ngot  %s\nwant %s", got, want)
	}
}

// Merge inlines integer bounds and never anything else.
func TestBuildQueryBoundLiteral(t *testing.T) {
	p := &plan{key: []keyCol{{colPlan: colPlan{name: "id"}, base: [2]string{"integer", "bigint"}}}}
	r := keyRange{lo: keyBound{vals: []string{"1); DROP TABLE t; --"}, inclusive: true}}
	got := buildQuery(`"t"`, p, 1, "", r)
	want := `SELECT "id", '()'::text FROM "t" WHERE "id" >= '1); DROP TABLE t; --'::bigint ORDER BY "id"`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

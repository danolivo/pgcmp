package main

import (
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

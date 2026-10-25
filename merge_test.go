package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func feed(rows ...row) <-chan row {
	ch := make(chan row, len(rows))
	for _, r := range rows {
		ch <- r
	}
	close(ch)
	return ch
}

func TestMergeIdentical(t *testing.T) {
	var buf bytes.Buffer
	st, err := merge(
		feed(row{1, "(a)"}, row{2, "(b)"}, row{3, "(c)"}),
		feed(row{1, "(a)"}, row{2, "(b)"}, row{3, "(c)"}),
		&buf)
	if err != nil {
		t.Fatal(err)
	}
	if st.diffs() != 0 || st.rows1 != 3 || st.rows2 != 3 {
		t.Fatalf("unexpected stats: %+v", st)
	}
	if buf.Len() != 0 {
		t.Fatalf("unexpected output: %q", buf.String())
	}
}

func TestMergeMismatch(t *testing.T) {
	var buf bytes.Buffer
	st, err := merge(
		feed(row{1, "(a)"}, row{2, "(b)"}, row{4, "(d)"}, row{5, "(e)"}),
		feed(row{1, "(a)"}, row{3, "(c)"}, row{4, "(D)"}, row{6, "(f)"}),
		&buf)
	if err != nil {
		t.Fatal(err)
	}
	want := stats{rows1: 4, rows2: 4, differ: 1, onlyLeft: 2, onlyRight: 2}
	if st != want {
		t.Fatalf("stats = %+v, want %+v", st, want)
	}
	wantOut := "2 (b) NULL\n" +
		"3 NULL (c)\n" +
		"4 (d) (D)\n" +
		"5 (e) NULL\n" +
		"6 NULL (f)\n"
	if buf.String() != wantOut {
		t.Fatalf("output:\n%s\nwant:\n%s", buf.String(), wantOut)
	}
}

func TestMergeOneSideEmpty(t *testing.T) {
	var buf bytes.Buffer
	st, err := merge(feed(), feed(row{7, "(x)"}), &buf)
	if err != nil {
		t.Fatal(err)
	}
	if st.onlyRight != 1 || st.rows1 != 0 || st.rows2 != 1 {
		t.Fatalf("unexpected stats: %+v", st)
	}
	if buf.String() != "7 NULL (x)\n" {
		t.Fatalf("unexpected output: %q", buf.String())
	}
}

func TestBuildQuery(t *testing.T) {
	idKey := []keyCol{{colPlan: colPlan{name: "id"}}}
	p := &plan{key: idKey, intKey: true, cols: []colPlan{
		{name: "name"},
		{name: "Odd Col", cast: [2]string{"", "numeric(12,3)"}},
	}}
	got := buildQuery(`"public"."t"`, p, 0, "id > 10", keyRange{})
	want := `SELECT "id", ROW("name", "Odd Col")::text FROM "public"."t" WHERE (id > 10) ORDER BY "id"`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	got = buildQuery(`"public"."t"`, p, 1, "", keyRange{})
	want = `SELECT "id", ROW("name", "Odd Col"::numeric(12,3))::text FROM "public"."t" ORDER BY "id"`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	got = buildQuery(`"public"."t"`, &plan{key: idKey}, 0, "", keyRange{})
	want = `SELECT "id", '()'::text FROM "public"."t" ORDER BY "id"`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	r := keyRange{lo: keyBound{vals: []string{"-5"}, inclusive: true}, hi: keyBound{vals: []string{"100"}}}
	got = buildQuery(`"public"."t"`, &plan{key: idKey}, 0, "x = 1", r)
	want = `SELECT "id", '()'::text FROM "public"."t" WHERE (x = 1) AND "id" >= -5 AND "id" < 100 ORDER BY "id"`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

// fakeCommon stands in for the server: it knows one UNION and nothing else.
func fakeCommon(c1, c2 column) (string, error) {
	if c1.typeName == "numeric(10,2)" && c2.typeName == "integer" {
		return "numeric", nil
	}
	if c1.typeName == "integer" && c2.typeName == "bigint" {
		return "bigint", nil
	}
	return "", errors.New("cannot be matched")
}

func TestNegotiate(t *testing.T) {
	ref := &description{side: "reference", schema: "public", name: "t",
		columns: []column{
			{name: "id", typeOID: 23, typeName: "integer"},
			{name: "a", typeOID: 25, typeName: "text"},
			{name: "b", typeOID: 1700, typeName: "numeric(10,2)"},
			{name: "only_ref", typeOID: 25, typeName: "text"},
		},
	}
	ref.pk = []column{ref.columns[0]}
	tst := &description{side: "test", schema: "public", name: "t",
		columns: []column{
			{name: "only_test", typeOID: 25, typeName: "text"},
			{name: "b", typeOID: 1700, typeName: "numeric(10,2)"},
			{name: "id", typeOID: 20, typeName: "bigint"}, // other integer width is fine
			{name: "a", typeOID: 25, typeName: "text"},
		},
	}
	tst.pk = []column{tst.columns[2]}

	// Default list: the columns present on both sides, others ignored.
	p, err := negotiate(ref, tst, nil, fakeCommon)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.key) != 1 || p.key[0].name != "id" || !p.intKey ||
		len(p.cols) != 2 || p.cols[0].name != "a" || p.cols[1].name != "b" {
		t.Fatalf("unexpected plan: %+v", p)
	}
	if len(p.ignored) != 2 || p.ignored[0] != "reference.only_ref" || p.ignored[1] != "test.only_test" {
		t.Fatalf("unexpected ignored list: %v", p.ignored)
	}

	// Explicit column list, key included: key is dropped from the row.
	p, err = negotiate(ref, tst, []string{"id", "b"}, fakeCommon)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.cols) != 1 || p.cols[0].name != "b" {
		t.Fatalf("unexpected plan: %+v", p)
	}

	// An explicitly named column must exist on both sides.
	if _, err := negotiate(ref, tst, []string{"only_ref"}, fakeCommon); err == nil {
		t.Fatal("expected a missing column error")
	}

	// Differing types: every side whose type is not the common one is cast.
	tst.columns[1].typeName = "integer"
	p, err = negotiate(ref, tst, nil, fakeCommon)
	if err != nil {
		t.Fatal(err)
	}
	if b := p.cols[1]; b.cast != [2]string{"numeric", "numeric"} {
		t.Fatalf("unexpected casts for b: %+v", b)
	}

	// No common type is an error, and the first one stops: the probe that
	// finds it aborts the transaction, so the server is asked nothing more.
	tst.columns[1].typeName = "character varying(10)"
	tst.columns[3].typeName = "integer" // a: text against integer, before b
	asked := 0
	counting := func(c1, c2 column) (string, error) { asked++; return fakeCommon(c1, c2) }
	_, err = negotiate(ref, tst, nil, counting)
	if err == nil || !strings.Contains(err.Error(), `column "a"`) || asked != 1 {
		t.Fatalf("got %v after %d probes, want the error of column a after 1", err, asked)
	}
	// Missing columns are all reported first, with no probe at all.
	asked = 0
	_, err = negotiate(ref, tst, []string{"a", "only_ref", "only_test", "b"}, counting)
	if err == nil || !strings.Contains(err.Error(), "only_ref") || !strings.Contains(err.Error(), "only_test") || asked != 0 {
		t.Fatalf("got %v after %d probes, want both missing columns and no probe", err, asked)
	}
	tst.columns[3].typeName = "text"

	tst.columns[1].typeName = "numeric(10,2)"

	// A non-integer key goes to the hash strategy; the key column types
	// without modifiers are kept for the range bounds.
	ref.pk = []column{{name: "id", typeOID: 25, typeName: "text", baseType: "text"}}
	tst.pk = []column{{name: "id", typeOID: 25, typeName: "text", baseType: "text"}}
	p, err = negotiate(ref, tst, nil, fakeCommon)
	if err != nil {
		t.Fatal(err)
	}
	if p.intKey || len(p.key) != 1 || p.key[0].base != [2]string{"text", "text"} || p.key[0].cast != [2]string{} {
		t.Fatalf("unexpected key: %+v", p.key)
	}

	// regproc prints overloaded functions ambiguously: the key text and the
	// bounds go through regprocedure.
	ref.pk = []column{{name: "id", typeOID: 24, typeName: "regproc", baseType: "regproc"}}
	tst.pk = []column{{name: "id", typeOID: 24, typeName: "regproc", baseType: "regproc"}}
	p, err = negotiate(ref, tst, nil, fakeCommon)
	if err != nil {
		t.Fatal(err)
	}
	if k := p.key[0]; k.base != [2]string{"regprocedure", "regprocedure"} || k.cast != [2]string{"regprocedure", "regprocedure"} {
		t.Fatalf("unexpected key: %+v", k)
	}

	// Composite key: also hash; an integer column of different widths is
	// cast to the common type, and no key column is compared as data.
	ref.pk = []column{ref.columns[0], ref.columns[1]}
	tst.pk = []column{tst.columns[2], tst.columns[3]}
	ref.pk[0].baseType, tst.pk[0].baseType = "integer", "bigint"
	p, err = negotiate(ref, tst, nil, fakeCommon)
	if err != nil {
		t.Fatal(err)
	}
	if p.intKey || len(p.key) != 2 || p.key[0].cast != [2]string{"bigint", ""} ||
		p.key[0].base != [2]string{"integer", "bigint"} || p.key[1].name != "a" {
		t.Fatalf("unexpected key: %+v", p.key)
	}
	if len(p.cols) != 1 || p.cols[0].name != "b" {
		t.Fatalf("key columns must not be compared as data: %+v", p.cols)
	}

	// Key columns must match by name and order.
	tst.pk = []column{tst.columns[3], tst.columns[2]}
	if _, err := negotiate(ref, tst, nil, fakeCommon); err == nil {
		t.Fatal("expected a key mismatch error")
	}

	// A key column without a common type is an error.
	ref.pk = []column{{name: "id", typeOID: 25, typeName: "text"}}
	tst.pk = []column{tst.columns[2]}
	if _, err := negotiate(ref, tst, nil, fakeCommon); err == nil {
		t.Fatal("expected a key type error")
	}
}

func TestPlanShape(t *testing.T) {
	cases := []struct {
		json    string
		shape   string
		seqScan bool
	}{
		{`{"Node Type": "Index Scan"}`, "Index Scan", false},
		{`{"Node Type": "Sort", "Plans": [{"Node Type": "Seq Scan"}]}`, "Sort -> Seq Scan", true},
		{`{"Node Type": "Gather Merge", "Plans": [{"Node Type": "Sort",
		   "Plans": [{"Node Type": "Seq Scan", "Parallel Aware": true}]}]}`,
			"Gather Merge -> Sort -> Parallel Seq Scan", true},
		{`{"Node Type": "Sort", "Plans": [{"Node Type": "Bitmap Heap Scan",
		   "Plans": [{"Node Type": "Bitmap Index Scan"}]}]}`,
			"Sort -> Bitmap Heap Scan -> Bitmap Index Scan", false}, // sorts a range only
	}
	for _, c := range cases {
		var n planNode
		if err := json.Unmarshal([]byte(c.json), &n); err != nil {
			t.Fatal(err)
		}
		shape, seq := planShape(n)
		if shape != c.shape || seq != c.seqScan {
			t.Errorf("got %q %v, want %q %v", shape, seq, c.shape, c.seqScan)
		}
	}
}

func TestCheckEncodings(t *testing.T) {
	ok := [][2]string{{"UTF8", "UTF8"}, {"UTF8", "LATIN1"}, {"SQL_ASCII", "SQL_ASCII"}}
	bad := [][2]string{{"SQL_ASCII", "UTF8"}, {"LATIN1", "SQL_ASCII"}}
	for _, e := range ok {
		if err := checkEncodings(e[0], e[1]); err != nil {
			t.Errorf("%v: unexpected error: %v", e, err)
		}
	}
	for _, e := range bad {
		if err := checkEncodings(e[0], e[1]); err == nil {
			t.Errorf("%v: expected an error", e)
		}
	}
}

func TestMajorVersion(t *testing.T) {
	cases := map[string]string{
		"18.0 (Homebrew)": "18",
		"17.6":            "17",
		"19devel":         "19",
		"19beta1":         "19",
		"10.23":           "10",
		"9.6.24":          "9.6",
		"weird":           "weird",
	}
	for in, want := range cases {
		if got := majorVersion(in); got != want {
			t.Errorf("majorVersion(%q) = %q, want %q", in, got, want)
		}
	}
	if w := versionWarning("18.0", "18.3 (Debian)"); w != "" {
		t.Errorf("same major: unexpected warning %q", w)
	}
	if w := versionWarning("17.6", "18.0"); w == "" {
		t.Error("different majors: expected a warning")
	}
}

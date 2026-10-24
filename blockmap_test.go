package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestMapMetaCompatible(t *testing.T) {
	key := []keyColumn{
		{column: column{name: "a", typeName: "integer"}, bound: "integer"},
		{column: column{name: "f", typeName: "regproc"}, bound: "regprocedure"},
	}
	m := &mapMeta{Format: mapFormat, Key: keyMeta(key), Filter: "a > 5"}
	if err := m.compatible(key, " a > 5 "); err != nil {
		t.Fatalf("the same key and filter: %v", err)
	}
	for name, c := range map[string]struct {
		key    []keyColumn
		filter string
		want   string
	}{
		"another filter":     {key, "", "-filter"},
		"another column":     {[]keyColumn{key[0], {column: column{name: "g"}, bound: "regprocedure"}}, "a > 5", "(a integer, g regprocedure)"},
		"another bound type": {[]keyColumn{{column: column{name: "a"}, bound: "bigint"}, key[1]}, "a > 5", "(a bigint, f regprocedure)"},
		"a shorter key":      {key[:1], "a > 5", "(a integer)"},
	} {
		err := m.compatible(c.key, c.filter)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want an error mentioning %q", name, err, c.want)
		}
	}
}

// The meta is read back by later builds: its JSON names are a format.
func TestMapMetaJSON(t *testing.T) {
	m := mapMeta{
		Format: mapFormat, Table: `"public"."t"`, Rows: 1000, Filter: "x", Snapshot: "1:1:",
		Server: "18.6", Built: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC), Blocks: 3, Total: 2500,
		Key: []mapKeyCol{{Name: "id", Type: "integer", Bound: "integer"}},
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"pgcmp_map":1,"table":"\"public\".\"t\"","key":[{"name":"id","type":"integer","bound":"integer"}],` +
		`"rows_per_block":1000,"filter":"x","snapshot":"1:1:","server_version":"18.6",` +
		`"built_at":"2026-10-24T12:00:00Z","blocks":3,"rows":2500}`
	if string(b) != want {
		t.Fatalf("got  %s\nwant %s", b, want)
	}
	var back mapMeta
	if err := json.Unmarshal(b, &back); err != nil || back.Blocks != 3 || !back.Built.Equal(m.Built) || back.Key[0].Bound != "integer" {
		t.Fatalf("round trip: %+v, %v", back, err)
	}
}

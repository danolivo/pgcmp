package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// A block map is the cut of the key space that the slicer makes, stored in
// a table on the server it was cut on (pgcmp slice), to be used again
// instead of cutting the table once more (-map), or to hang block hashes
// on. One row per block, in key order:
//
//	seq   bigint NOT NULL  -- block number, from 0, in key order
//	lo    text[]           -- lower bound, excluded; NULL: open
//	hi    text[]           -- upper bound, included; NULL: open
//	rows  bigint NOT NULL  -- rows in the block when the map was built
//	hash  bytea            -- NULL; room for a block hash
//
// The bounds hold one element per key column, as text of the bound type of
// the column on that server (keyColumn.bound). What the map was built from
// is kept as JSON in the comment on the table (mapMeta).

const mapFormat = 1

// mapMeta is what a block map says about itself.
type mapMeta struct {
	Format   int         `json:"pgcmp_map"`
	Table    string      `json:"table"`
	Key      []mapKeyCol `json:"key"`
	Rows     int         `json:"rows_per_block"`
	Filter   string      `json:"filter"`
	Snapshot string      `json:"snapshot"` // pg_current_snapshot() of the slicer
	Server   string      `json:"server_version"`
	Built    time.Time   `json:"built_at"`
	Blocks   int64       `json:"blocks"`
	Total    int64       `json:"rows"`
	Pgcmp    string      `json:"pgcmp,omitempty"` // the revision pgcmp was built from
}

type mapKeyCol struct {
	Name  string `json:"name"`
	Type  string `json:"type"`  // format_type with the modifier
	Bound string `json:"bound"` // the type the bounds are written in
}

func keyMeta(key []keyColumn) []mapKeyCol {
	m := make([]mapKeyCol, len(key))
	for i, k := range key {
		m[i] = mapKeyCol{Name: k.name, Type: k.typeName, Bound: k.bound}
	}
	return m
}

// compatible checks that the map can cut the key space of a comparison:
// the same key columns with the same bound types on the reference side,
// and the same filter. A map from an older snapshot is still correct (its
// blocks cover the whole key space), only no longer N rows each.
func (m *mapMeta) compatible(key []keyColumn, filter string) error {
	if got := keyMeta(key); !slices.EqualFunc(m.Key, got, func(a, b mapKeyCol) bool {
		return a.Name == b.Name && a.Bound == b.Bound
	}) {
		return fmt.Errorf("the map was cut on key %s, the table has key %s", showKey(m.Key), showKey(got))
	}
	if strings.TrimSpace(m.Filter) != strings.TrimSpace(filter) {
		return fmt.Errorf("the map was cut with -filter %q, the comparison has %q", m.Filter, filter)
	}
	return nil
}

func showKey(key []mapKeyCol) string {
	parts := make([]string, len(key))
	for i, k := range key {
		parts[i] = k.Name + " " + k.Bound
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// pgcmpRevision is the VCS revision pgcmp was built from, if known.
func pgcmpRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	rev, dirty := "", false
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if rev != "" && dirty {
		rev += "+dirty"
	}
	return rev
}

var errNotAMap = errors.New("not a pgcmp block map")

// querier is a connection or a transaction.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// mapMetaOf reads the meta of the table name through q, a session of side
// s, and the name as the server quotes it. A table without a valid meta is
// errNotAMap.
func (s *side) mapMetaOf(ctx context.Context, q querier, name string) (*mapMeta, string, error) {
	var qualified string
	var comment *string
	err := q.QueryRow(ctx, `
		SELECT c.oid::regclass::text, pg_catalog.obj_description(c.oid, 'pg_class')
		FROM pg_catalog.pg_class c WHERE c.oid = pg_catalog.to_regclass($1)`, name).Scan(&qualified, &comment)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", fmt.Errorf("%s: block map %q does not exist", s.name, name)
	}
	if err != nil {
		return nil, "", fmt.Errorf("%s: block map %q: %w", s.name, name, err)
	}
	var m mapMeta
	if comment == nil || json.Unmarshal([]byte(*comment), &m) != nil || m.Format == 0 {
		return nil, qualified, fmt.Errorf("%s: %s: %w", s.name, qualified, errNotAMap)
	}
	if m.Format != mapFormat {
		return nil, qualified, fmt.Errorf("%s: %s: block map format %d, this pgcmp reads %d", s.name, qualified, m.Format, mapFormat)
	}
	return &m, qualified, nil
}

// mapFeed sends the blocks of a map in key order, read through the
// coordinator transaction of s, and checks on the way that they cover the
// key space with no gaps or overlaps: numbered from 0, each starting at the
// bound the previous one ended at, the outer ones open.
func mapFeed(s *side, table string, blocks int64) rangeFeed {
	return func(ctx context.Context, out chan<- keyRange) error {
		rows, err := s.tx.Query(ctx, "SELECT seq, lo, hi FROM "+table+" ORDER BY seq")
		if err != nil {
			return fmt.Errorf("%s: block map %s: %w", s.name, table, err)
		}
		defer rows.Close()
		var prev []string
		var n int64
		for rows.Next() {
			var seq int64
			var lo, hi []string
			if err := rows.Scan(&seq, &lo, &hi); err != nil {
				return fmt.Errorf("%s: block map %s: %w", s.name, table, err)
			}
			if seq != n || !slices.Equal(lo, prev) || (n == 0) != (lo == nil) {
				return fmt.Errorf("%s: block map %s is broken at block %d: blocks must follow each other with no gaps", s.name, table, n)
			}
			select {
			case out <- blockAfter(lo, hi):
			case <-ctx.Done():
				return ctx.Err()
			}
			prev, n = hi, n+1
			if hi == nil {
				break
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("%s: block map %s: %w", s.name, table, err)
		}
		rows.Close()
		if n != blocks || prev != nil {
			return fmt.Errorf("%s: block map %s is incomplete: %d blocks of %d, the last one %s",
				s.name, table, n, blocks, map[bool]string{true: "open", false: "closed"}[prev == nil])
		}
		return nil
	}
}

// countAfter counts the rows after bound lo (all of them if lo is nil)
// that match the filter: the rows of the last block, through an index
// scan of the tail only.
func (sl *slicer) countAfter(ctx context.Context, s *side, lo []string) (int64, error) {
	var conds []string
	if f := strings.TrimSpace(sl.filter); f != "" {
		conds = append(conds, "("+f+")")
	}
	if lo != nil {
		cols := make([]string, len(sl.key))
		for i, k := range sl.key {
			cols[i] = pgx.Identifier{k.name}.Sanitize()
		}
		conds = append(conds, keyCondition(cols, ">", lo, func(i int, v string) string {
			return quoteLiteral(v) + "::" + sl.key[i].bound
		}))
	}
	q := "SELECT count(*) FROM " + sl.table
	if len(conds) > 0 {
		q += " WHERE " + strings.Join(conds, " AND ")
	}
	var n int64
	if err := s.tx.QueryRow(ctx, q).Scan(&n); err != nil {
		return 0, fmt.Errorf("%s: rows of the last block: %w", s.name, err)
	}
	return n, nil
}

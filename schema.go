package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Session settings that affect the text output of values or name
// resolution. They must be identical on both sides, otherwise equal rows
// would serialise differently or names would resolve to different objects.
//
// search_path is "public" rather than the default "$user", public: $user
// expands to the role name, which may differ between the two instances.
// pg_catalog is still searched first implicitly.
//
// standard_conforming_strings is on by default; it is pinned because
// histogram bounds of the hash strategy are inlined as '...' literals.
const sessionSetup = `
SET search_path = public;
SET standard_conforming_strings = on;
SET TimeZone = 'UTC';
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET bytea_output = 'hex';
SET extra_float_digits = 3;
SET lc_monetary = 'C';
SET quote_all_identifiers = on;
SET client_encoding = 'UTF8';
SET statement_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
`

// Key types the merge strategy handles: int2, int4, int8. The key is read
// into int64, so the two sides may use different integer widths. Any other
// key goes to the hash-block strategy.
var integerTypes = map[uint32]bool{21: true, 23: true, 20: true}

type side struct {
	name     string
	conn     *pgx.Conn
	tx       pgx.Tx
	encoding string // server_encoding of the connected database
	version  string // server_version, e.g. "18.0 (Homebrew)"
}

// majorVersion extracts the major version from server_version:
// "18.0 (Homebrew)" -> "18", "19devel" -> "19", "9.6.24" -> "9.6".
func majorVersion(v string) string {
	end := 0
	for end < len(v) && (v[end] == '.' || v[end] >= '0' && v[end] <= '9') {
		end++
	}
	parts := strings.Split(v[:end], ".")
	if parts[0] == "" {
		return v
	}
	if len(parts[0]) == 1 && len(parts) > 1 { // before 10, the major was "X.Y"
		return parts[0] + "." + parts[1]
	}
	return parts[0]
}

// versionWarning returns a warning if the servers run different major
// versions, or "" otherwise. Output functions sometimes change between
// major versions (float8 is printed differently before and after PG 12),
// so mismatches may come from the servers rather than from the data.
func versionWarning(ref, test string) string {
	m1, m2 := majorVersion(ref), majorVersion(test)
	if m1 == m2 {
		return ""
	}
	return fmt.Sprintf("warning: different major versions: %s (reference) vs %s (test); "+
		"value output may differ between versions and show up as mismatches", m1, m2)
}

// checkEncodings refuses to compare an SQL_ASCII database with a database
// in any other encoding. An SQL_ASCII server does no conversion to
// client_encoding and returns raw bytes, while any other server converts to
// UTF8, so equal text could arrive as different bytes. Two SQL_ASCII
// databases are fine: both return raw bytes.
func checkEncodings(ref, test string) error {
	if (ref == "SQL_ASCII") != (test == "SQL_ASCII") {
		return fmt.Errorf("server encodings are incompatible: %s (reference) vs %s (test); "+
			"SQL_ASCII must be on both sides or on neither", ref, test)
	}
	return nil
}

// column is one attribute as one server describes it.
//
// typeOID is meaningful only on the server it came from: built-in types
// have the same OIDs everywhere, but user-defined types (enums, domains,
// extension types) get theirs at creation time. Across servers a type is
// identified by typeName, which is comparable because both sessions use the
// same search_path.
type column struct {
	name     string
	typeOID  uint32
	typeName string // format_type(atttypid, atttypmod), e.g. numeric(10,2)
	baseType string // format_type(atttypid, -1), e.g. numeric; see describe
	category string // pg_type.typcategory, e.g. "A" for an array
}

// description is what one side knows about the table.
type description struct {
	side    string
	oid     uint32
	schema  string
	name    string
	columns []column
	pk      []column
}

func (d *description) qualifiedName() string {
	return pgx.Identifier{d.schema, d.name}.Sanitize()
}

func (d *description) column(name string) (column, bool) {
	for _, c := range d.columns {
		if c.name == name {
			return c, true
		}
	}
	return column{}, false
}

// colPlan is one compared column. cast[i] is the type the column is cast
// to on side i (0 = reference, 1 = test), or "" when no cast is needed.
type colPlan struct {
	name string
	cast [2]string
}

// keyCol is one primary key column. The colPlan part casts the column to
// the common type where the key is compared as text. base[i] is the column
// type on side i without its modifier: range bounds sent to that side as
// parameters are cast to it, so that the condition compares in the
// column's own type and can use the index.
type keyCol struct {
	colPlan
	base [2]string
}

// plan is the negotiated result: which key and which columns are compared.
type plan struct {
	key     []keyCol  // primary key columns, in index order
	intKey  bool      // one int2/int4/int8 key column on both sides
	cols    []colPlan // non-key columns, in reference-table order
	ignored []string  // columns present on one side only (default column list)
}

func (p *plan) isKey(name string) bool {
	for _, k := range p.key {
		if k.name == name {
			return true
		}
	}
	return false
}

// commonTypeFunc returns the most general type of two column definitions,
// as a format_type string, or an error if there is none.
type commonTypeFunc func(c1, c2 column) (string, error)

// openSide connects and opens the read-only REPEATABLE READ transaction
// every reading session works in (see connect and begin).
func openSide(ctx context.Context, name, dsn, snapshot string) (*side, error) {
	s, err := connect(ctx, name, dsn)
	if err != nil {
		return nil, err
	}
	if err := s.begin(ctx, snapshot); err != nil {
		s.close()
		return nil, err
	}
	return s, nil
}

// connect opens a session and applies the session settings, with no
// transaction: a session that writes (the one that stores a block map)
// starts its own.
func connect(ctx context.Context, name, dsn string) (*side, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("%s: connect: %w", name, err)
	}
	// The server reports these at startup; no query needed.
	s := &side{
		name:     name,
		conn:     conn,
		encoding: conn.PgConn().ParameterStatus("server_encoding"),
		version:  conn.PgConn().ParameterStatus("server_version"),
	}
	// Simple protocol on purpose: several statements in one round trip. The
	// application name tells the sessions apart in pg_stat_activity.
	setup := sessionSetup + "SET application_name = " + quoteLiteral("pgcmp "+name) + ";"
	if _, err := conn.PgConn().Exec(ctx, setup).ReadAll(); err != nil {
		s.close()
		return nil, fmt.Errorf("%s: session setup: %w", name, err)
	}
	return s, nil
}

// begin opens the read-only REPEATABLE READ transaction of the session. A
// non-empty snapshot is an exported snapshot id: the transaction then sees
// exactly what the exporting one sees.
//
// One snapshot per side for the whole run: catalog lookups and the data
// stream see the same state. REPEATABLE READ also allows exporting the
// snapshot later, for parallel range workers.
func (s *side) begin(ctx context.Context, snapshot string) error {
	tx, err := s.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("%s: begin: %w", s.name, err)
	}
	s.tx = tx
	if snapshot != "" {
		sql := "SET TRANSACTION SNAPSHOT " + quoteLiteral(snapshot)
		if _, err := tx.Exec(ctx, sql); err != nil {
			return fmt.Errorf("%s: import snapshot: %w", s.name, err)
		}
	}
	return nil
}

func (s *side) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if s.tx != nil {
		_ = s.tx.Rollback(ctx)
	}
	_ = s.conn.Close(ctx)
}

func (s *side) describe(ctx context.Context, table string) (*description, error) {
	d := &description{side: s.name}

	var relkind string
	err := s.tx.QueryRow(ctx, `
		SELECT c.oid, n.nspname, c.relname, c.relkind::text
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE c.oid = pg_catalog.to_regclass($1)`, table).
		Scan(&d.oid, &d.schema, &d.name, &relkind)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%s: relation %q does not exist", s.name, table)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: resolve %q: %w", s.name, table, err)
	}
	switch relkind {
	case "r":
	case "p":
		return nil, fmt.Errorf("%s: %s is a partitioned table; not supported yet", s.name, d.qualifiedName())
	default:
		return nil, fmt.Errorf("%s: %s is not a table (relkind %q)", s.name, d.qualifiedName(), relkind)
	}
	// A SELECT from an inheritance parent also returns the rows of its
	// children, which the key does not cover. A partition or an inheritance
	// child alone is an ordinary table.
	var parent bool
	err = s.tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT FROM pg_catalog.pg_inherits WHERE inhparent = $1)`, d.oid).Scan(&parent)
	if err != nil {
		return nil, fmt.Errorf("%s: inheritance of %s: %w", s.name, d.qualifiedName(), err)
	}
	if parent {
		return nil, fmt.Errorf("%s: %s has inheritance children; not supported yet", s.name, d.qualifiedName())
	}

	// The base type is the type without its modifier, as a cast target for
	// key bounds. It must be format_type(oid, -1), not (oid, NULL): with no
	// modifier given, bpchar and bit come out as "character" and "bit",
	// which mean char(1) and bit(1), and an explicit cast to them silently
	// truncates the bound. With -1 they are bpchar and "bit", unconstrained.
	rows, err := s.tx.Query(ctx, `
		SELECT a.attname, a.atttypid,
		       pg_catalog.format_type(a.atttypid, a.atttypmod),
		       pg_catalog.format_type(a.atttypid, -1),
		       t.typcategory::text
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum`, d.oid)
	if err != nil {
		return nil, fmt.Errorf("%s: columns of %s: %w", s.name, d.qualifiedName(), err)
	}
	d.columns, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (column, error) {
		var c column
		err := r.Scan(&c.name, &c.typeOID, &c.typeName, &c.baseType, &c.category)
		return c, err
	})
	if err != nil {
		return nil, fmt.Errorf("%s: columns of %s: %w", s.name, d.qualifiedName(), err)
	}

	// Only the first indnkeyatts entries of indkey are key columns; the rest
	// are INCLUDE columns, which are not part of the key.
	rows, err = s.tx.Query(ctx, `
		SELECT a.attname
		FROM pg_catalog.pg_index i
		JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid
		WHERE i.indrelid = $1 AND i.indisprimary
		  AND a.attnum = ANY ((i.indkey::int2[])[0:i.indnkeyatts - 1])
		ORDER BY pg_catalog.array_position(i.indkey::int2[], a.attnum)`, d.oid)
	if err != nil {
		return nil, fmt.Errorf("%s: primary key of %s: %w", s.name, d.qualifiedName(), err)
	}
	pkNames, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("%s: primary key of %s: %w", s.name, d.qualifiedName(), err)
	}
	for _, n := range pkNames {
		c, ok := d.column(n)
		if !ok {
			return nil, fmt.Errorf("%s: primary key column %q not found among attributes", s.name, n)
		}
		d.pk = append(d.pk, c)
	}
	return d, nil
}

// commonType asks this server for the common type of two column definitions:
// the type PostgreSQL itself gives a UNION of the two. The decision is left
// to the server entirely, type modifier included:
//
//	int4          + int8          -> bigint
//	timestamp     + timestamptz   -> timestamp with time zone
//	numeric(10,2) + numeric(12,3) -> numeric      (different modifiers are dropped)
//	integer       + text          -> error: no unambiguous mapping
//
// Casting to such a type never loses data. It may make equal values print
// differently (15.50 vs 15.500 as numeric), which shows up as a mismatch,
// never as a false match.
//
// The result type is read from the description of the prepared UNION:
// pg_typeof() would give the type without its modifier.
//
// s must be the side c1 came from; types are matched across the servers by
// name only.
func (s *side) commonType(ctx context.Context, c1, c2 column) (string, error) {
	// Preparing fails when there is no common type ("UNION types ... cannot
	// be matched"). That aborts the transaction, and the run with it: the
	// caller stops at the first such column, and the user fixes it and
	// starts again.
	sql := fmt.Sprintf("SELECT x FROM (SELECT NULL::%s UNION ALL SELECT NULL::%s) s(x)",
		c1.typeName, c2.typeName)
	sd, err := s.conn.PgConn().Prepare(ctx, "", sql, nil)
	if err != nil {
		return "", err
	}

	f := sd.Fields[0]
	var name string
	err = s.tx.QueryRow(ctx, "SELECT pg_catalog.format_type($1, $2)", f.DataTypeOID, f.TypeModifier).Scan(&name)
	return name, err
}

// negotiate checks that both descriptions agree on the primary key and maps
// every compared column to a common type.
//
// With no explicit column list, exactly the columns present on both sides
// are compared; a column present on one side only is ignored. A column
// named explicitly must exist on both sides.
//
// The primary key may be of any type and have any number of columns, but
// both sides must have the same key columns in the same order. A single
// int2/int4/int8 column on both sides sets intKey: such a key is compared
// as an integer and needs no casts.
func negotiate(d1, d2 *description, requested []string, common commonTypeFunc) (*plan, error) {
	key1, err := keyOf(d1)
	if err != nil {
		return nil, err
	}
	key2, err := keyOf(d2)
	if err != nil {
		return nil, err
	}
	if keyList(d1.pk) != keyList(d2.pk) {
		return nil, fmt.Errorf("primary key differs: (%s) (reference) vs (%s) (test)", keyList(d1.pk), keyList(d2.pk))
	}

	p := &plan{intKey: len(d1.pk) == 1 && integerTypes[d1.pk[0].typeOID] && integerTypes[d2.pk[0].typeOID]}
	for i := range key1 {
		k := keyCol{
			colPlan: colPlan{name: key1[i].name},
			base:    [2]string{key1[i].baseType, key2[i].baseType},
		}
		if !p.intKey {
			cp, err := castPlan(key1[i].column, key2[i].column, common)
			if err != nil {
				return nil, fmt.Errorf("primary key column %q: %w", k.name, err)
			}
			k.colPlan = cp
			for side, kc := range []keyColumn{key1[i], key2[i]} {
				k.base[side] = kc.bound
				// The key text goes through the sibling as well, unless
				// the column is cast to a common type anyway.
				if kc.bound != kc.baseType && k.cast[side] == "" {
					k.cast[side] = kc.bound
				}
			}
		}
		p.key = append(p.key, k)
	}

	if len(requested) == 0 {
		for _, c := range d1.columns {
			if _, ok := d2.column(c.name); ok {
				requested = append(requested, c.name)
			} else {
				p.ignored = append(p.ignored, "reference."+c.name)
			}
		}
		for _, c := range d2.columns {
			if _, ok := d1.column(c.name); !ok {
				p.ignored = append(p.ignored, "test."+c.name)
			}
		}
	}

	// Missing columns are found without asking a server, so all of them are
	// reported at once. Common types are asked for only then, column by
	// column: the first that has none fails the run (see commonType).
	var problems []string
	var pairs [][2]column
	seen := map[string]bool{}
	for _, name := range requested {
		if seen[name] || p.isKey(name) {
			continue
		}
		seen[name] = true
		c1, ok1 := d1.column(name)
		c2, ok2 := d2.column(name)
		switch {
		case !ok1 && !ok2:
			problems = append(problems, fmt.Sprintf("column %q exists on neither side", name))
		case !ok1:
			problems = append(problems, fmt.Sprintf("column %q is missing on the reference side", name))
		case !ok2:
			problems = append(problems, fmt.Sprintf("column %q is missing on the test side", name))
		default:
			pairs = append(pairs, [2]column{c1, c2})
		}
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("table structures do not match:\n  %s", strings.Join(problems, "\n  "))
	}
	for _, pair := range pairs {
		cp, err := castPlan(pair[0], pair[1], common)
		if err != nil {
			return nil, fmt.Errorf("table structures do not match: column %q: %w", pair[0].name, err)
		}
		p.cols = append(p.cols, cp)
	}
	return p, nil
}

// unambiguousKeyType maps key types whose text does not identify the value
// to a sibling type with the same values whose text does. regproc and
// regoper print an overloaded function or operator by its bare name: two
// keys of pg_aggregate both print as "avg", and "avg" does not read back.
// regprocedure and regoperator add the argument types. The key text and the
// block bounds use the sibling; the column keeps its own type, ordering
// and index. Built-in types: matched by OID, which is the same everywhere
// (a name may come quoted, under quote_all_identifiers).
var unambiguousKeyType = map[uint32]string{
	24:   "regprocedure", // regproc
	2203: "regoperator",  // regoper
}

// keyColumn is one primary key column as its own server sees it.
type keyColumn struct {
	column
	// bound is the type the values of the column are written in and read
	// back as on its own server, as range bounds and as key text: the
	// column type without its modifier, or the unambiguous sibling of a
	// type whose text does not identify the value (unambiguousKeyType).
	bound string
}

// keyOf returns the primary key of the table as its own server sees it, in
// index order. That is all cutting the key space on one server needs, with
// no other side involved (pgcmp slice); negotiate builds the plan of the
// two sides on top of it.
func keyOf(d *description) ([]keyColumn, error) {
	if len(d.pk) == 0 {
		return nil, fmt.Errorf("%s: %s has no primary key", d.side, d.qualifiedName())
	}
	key := make([]keyColumn, len(d.pk))
	for i, c := range d.pk {
		key[i] = keyColumn{column: c, bound: c.baseType}
		if alt, ok := unambiguousKeyType[c.typeOID]; ok {
			key[i].bound = alt
		}
	}
	return key, nil
}

// keyList returns the key column names, comma-separated, for messages and
// for comparing the keys of the two sides.
func keyList(pk []column) string {
	names := make([]string, len(pk))
	for i, c := range pk {
		names[i] = c.name
	}
	return strings.Join(names, ", ")
}

// castPlan maps one column to the common type of its two definitions.
func castPlan(c1, c2 column, common commonTypeFunc) (colPlan, error) {
	cp := colPlan{name: c1.name}
	if c1.typeName == c2.typeName {
		return cp, nil
	}
	target, err := common(c1, c2)
	if err != nil {
		return cp, fmt.Errorf("no common type for %s (reference) and %s (test): %v", c1.typeName, c2.typeName, err)
	}
	if c1.typeName != target {
		cp.cast[0] = target
	}
	if c2.typeName != target {
		cp.cast[1] = target
	}
	return cp, nil
}

// expr returns the quoted column name, cast to the common type on side if
// needed.
func (c colPlan) expr(side int) string {
	e := pgx.Identifier{c.name}.Sanitize()
	if t := c.cast[side]; t != "" {
		e += "::" + t
	}
	return e
}

// rowExpr is the server-side text form of the compared non-key columns on
// one side: ROW(c1, c2, ...)::text, i.e. record_out, which is exactly what
// psql shows for a composite value. Columns whose types differ between the
// sides are cast to their common type first.
func rowExpr(p *plan, side int) string {
	if len(p.cols) == 0 {
		return "'()'::text"
	}
	exprs := make([]string, len(p.cols))
	for i, c := range p.cols {
		exprs[i] = c.expr(side)
	}
	return "ROW(" + strings.Join(exprs, ", ") + ")::text"
}

// buildQuery produces the SELECT of the merge strategy for one side
// (0 = reference, 1 = test): the integer key and the row text, ordered by
// the key. r restricts the key range.
func buildQuery(qualifiedTable string, p *plan, side int, filter string, r keyRange) string {
	pk := pgx.Identifier{p.key[0].name}.Sanitize()
	rowExpr := rowExpr(p, side)
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s, %s FROM %s", pk, rowExpr, qualifiedTable)
	var conds []string
	if f := strings.TrimSpace(filter); f != "" {
		conds = append(conds, "("+f+")")
	}
	// The bounds come from the server as integers and are inlined as they
	// are; anything else would be quoted and cast, never inlined raw.
	conds = append(conds, r.conditions([]string{pk}, func(_ int, v string) string {
		if _, err := strconv.ParseInt(v, 10, 64); err == nil {
			return v
		}
		return quoteLiteral(v) + "::" + p.key[0].base[side]
	})...)
	if len(conds) > 0 {
		fmt.Fprintf(&b, " WHERE %s", strings.Join(conds, " AND "))
	}
	fmt.Fprintf(&b, " ORDER BY %s", pk)
	return b.String()
}

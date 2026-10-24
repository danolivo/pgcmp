#!/bin/bash
#
# Hash-block strategy: keys other than a single integer column.
#
# Every table is loaded identically on both sides, then the test side gets
# a known number of updates, deletes and inserts, spread over the whole key
# range and beyond both ends of it. pgcmp must report exactly these counts,
# with a tiny block (-mem 1: many blocks, bounds everywhere) and with one
# big block, and both reports must contain the same lines.
#
# Needs initdb, pg_ctl and psql in PATH. Usage: test/keys.sh

set -euo pipefail

HERE=$(cd "$(dirname "$0")/.." && pwd)
BIN=$HERE/pgcmp
WORK=$(mktemp -d)
D1="host=localhost port=55491 user=postgres dbname=postgres"
D2="host=localhost port=55492 user=postgres dbname=postgres"
ROWS=${ROWS:-200000}

stop() { pg_ctl -D "$WORK/$1" -m fast stop >/dev/null 2>&1 || true; }
trap 'stop n1; stop n2; rm -rf "$WORK"' EXIT
start() { # dir port
	initdb -D "$WORK/$1" -U postgres --no-locale -E UTF8 >/dev/null
	pg_ctl -D "$WORK/$1" -l "$WORK/$1.log" -w start \
		-o "-p $2 -k '' -c listen_addresses=localhost -c autovacuum=off" >/dev/null
}
sql() { psql -X -q -At -v ON_ERROR_STOP=1 -d "$1" -c "$2"; }
both() { sql "$D1" "$1"; sql "$D2" "$1"; }

FAILED=0
pass() { echo "ok   $1"; }
fail() { echo "FAIL $1"; FAILED=1; }

# run <table> <mem> <expected exit> <expected "differ left right" or ""> [pgcmp args]
run() {
	local t=$1 mem=$2 want=$3 counts=$4 rc=0
	shift 4
	"$BIN" -dsn1 "$D1" -dsn2 "$D2" -table "$t" -mem "$mem" -out "$WORK/$t.$mem.out" "$@" \
		>"$WORK/log" 2>&1 || rc=$?
	if [ "$rc" != "$want" ]; then
		fail "$t -mem $mem: exit $rc, expected $want"; sed 's/^/    /' "$WORK/log"; return
	fi
	if ! grep -q 'method: hash blocks' "$WORK/log"; then
		fail "$t: hash strategy was not chosen"; sed 's/^/    /' "$WORK/log"; return
	fi
	if [ -n "$counts" ]; then
		local got
		got=$(sed -n 's/.*mismatch: \([0-9]*\) differ, \([0-9]*\) only in reference, \([0-9]*\) only in test.*/\1 \2 \3/p' "$WORK/log")
		if [ "$got" != "$counts" ]; then
			fail "$t -mem $mem: got '$got', expected '$counts'"; sed 's/^/    /' "$WORK/log"; return
		fi
	fi
	pass "$t -mem $mem $* ($(grep -o 'blocks: [0-9]*' "$WORK/log"))"
}

# same <table> [mem mem]: two runs of a table with different -mem (and
# maybe -j) hold the same report lines. Default: 1 MiB and 256 MiB blocks.
same() {
	local a=${2:-1} b=${3:-256}
	if diff <(sort "$WORK/$1.$a.out") <(sort "$WORK/$1.$b.out") >/dev/null; then
		pass "$1: same report lines for -mem $a and -mem $b"
	else
		fail "$1: reports differ between -mem $a and -mem $b"
		diff <(sort "$WORK/$1.$a.out") <(sort "$WORK/$1.$b.out") | head | sed 's/^/    /'
	fi
}

# plant <table> <data column>: 3 updates and 3 deletes on the test side,
# picked by the row number n. Each table then gets 4 test-only rows: two
# below the minimum of the key and two above its maximum.
plant() {
	local t=$1 col=$2
	sql "$D2" "UPDATE $t SET $col = 'changed' WHERE n IN (1, $((ROWS / 2)), $ROWS)"
	sql "$D2" "DELETE FROM $t WHERE n IN (2, $((ROWS / 3)), $((ROWS - 1)))"
}

(cd "$HERE" && go build -o "$BIN")
start n1 55491
start n2 55492

# Shift user type OIDs on the test side.
sql "$D2" "CREATE TYPE pad1 AS ENUM ('x'); CREATE TYPE pad2 AS ENUM ('x')"
both "CREATE TYPE color AS ENUM ('red', 'green', 'blue')"

echo "--- loading $ROWS rows per table"

# 1. Text key with spaces, quotes, commas and parentheses.
both "CREATE TABLE t_text (k text PRIMARY KEY, n int, v text)"
both "INSERT INTO t_text SELECT format('key %s, \"q\" (%s)', lpad(g::text, 8, '0'), g), g, md5(g::text) FROM generate_series(1, $ROWS) g"
plant t_text v
sql "$D2" "INSERT INTO t_text VALUES ('', 0, 'empty'), (' ', 0, 'space'), ('zzz', 0, 'z1'), ('zzzz', 0, 'z2')"

# 2. Composite key (int, text) on the reference, (bigint, varchar) on the
#    test side: the key text is compared in the common types.
sql "$D1" "CREATE TABLE t_comp (a int, b text, n int, v text, PRIMARY KEY (a, b))"
sql "$D2" "CREATE TABLE t_comp (a bigint, b varchar(40), n int, v text, PRIMARY KEY (a, b))"
both "INSERT INTO t_comp SELECT g / 10, 'b' || (g % 10), g, md5(g::text) FROM generate_series(1, $ROWS) g"
plant t_comp v
sql "$D2" "INSERT INTO t_comp VALUES (-1, 'x', 0, 'below'), (0, 'a', 0, 'below'), ($ROWS, 'x', 0, 'above'), ($ROWS, 'y', 0, 'above')"

# 3. uuid key.
both "CREATE TABLE t_uuid (k uuid PRIMARY KEY, n int, v text)"
both "INSERT INTO t_uuid SELECT md5(g::text)::uuid, g, g::text FROM generate_series(1, $ROWS) g"
plant t_uuid v
sql "$D2" "INSERT INTO t_uuid VALUES ('00000000-0000-0000-0000-000000000000', 0, 'min'), ('00000000-0000-0000-0000-000000000001', 0, 'min'),
                                      ('ffffffff-ffff-ffff-ffff-fffffffffffe', 0, 'max'), ('ffffffff-ffff-ffff-ffff-ffffffffffff', 0, 'max')"

# 4. timestamptz key.
both "CREATE TABLE t_ts (k timestamptz PRIMARY KEY, n int, v text)"
both "INSERT INTO t_ts SELECT '2026-01-01 00:00:00+00'::timestamptz + g * interval '1.5 second', g, g::text FROM generate_series(1, $ROWS) g"
plant t_ts v
sql "$D2" "INSERT INTO t_ts VALUES ('1999-01-01 00:00+00', 0, 'min'), ('-infinity', 0, 'min'), ('2099-01-01 00:00+00', 0, 'max'), ('infinity', 0, 'max')"

# 5. numeric key, including NaN, which sorts above every number.
both "CREATE TABLE t_num (k numeric PRIMARY KEY, n int, v text)"
both "INSERT INTO t_num SELECT g / 7.0, g, g::text FROM generate_series(1, $ROWS) g"
plant t_num v
sql "$D2" "INSERT INTO t_num VALUES (-1, 0, 'min'), (0, 0, 'min'), (1e30, 0, 'max'), ('NaN', 0, 'max')"

# 6. Composite key with an enum, whose OIDs differ between the servers:
#    bounds of a user type are passed as text, never by OID.
both "CREATE TABLE t_enum (c color, k int, n int, v text, PRIMARY KEY (c, k))"
both "INSERT INTO t_enum SELECT (ARRAY['red', 'green', 'blue']::color[])[1 + g % 3], g, g, g::text FROM generate_series(1, $ROWS) g"
plant t_enum v
sql "$D2" "INSERT INTO t_enum VALUES ('red', -1, 0, 'min'), ('red', 0, 0, 'min'), ('blue', $((ROWS + 1)), 0, 'max'), ('blue', $((ROWS + 2)), 0, 'max')"

# 7. Key only: no data columns at all.
both "CREATE TABLE t_keyonly (a text, b date, PRIMARY KEY (a, b))"
both "INSERT INTO t_keyonly SELECT 'x' || (g % 100), date '2000-01-01' + g FROM generate_series(1, $ROWS) g"

# 8a. char(n) and bit(n) keys: a bound cast to the type without its
#     modifier must not turn into char(1) or bit(1) and get truncated.
both "CREATE TABLE t_char (k char(12) PRIMARY KEY, n int, v text)"
both "INSERT INTO t_char SELECT 'c' || lpad(g::text, 8, '0'), g, md5(g::text) FROM generate_series(1, $ROWS) g"
plant t_char v
sql "$D2" "INSERT INTO t_char VALUES ('a', 0, 'min'), ('b', 0, 'min'), ('x', 0, 'max'), ('y', 0, 'max')"
both "CREATE TABLE t_bit (k bit(24) PRIMARY KEY, n int, v text)"
both "INSERT INTO t_bit SELECT (g + 16)::bit(24), g, md5(g::text) FROM generate_series(1, $ROWS) g"
plant t_bit v
sql "$D2" "INSERT INTO t_bit VALUES (0::bit(24), 0, 'min'), (1::bit(24), 0, 'min'), ((1 << 23)::bit(24), 0, 'max'), (((1 << 24) - 1)::bit(24), 0, 'max')"

# 9. A first key column whose histogram repeats a bound: with statistics
#    target 5 the MCV list holds 5 of the 6 heavy values, the sixth stays in
#    the histogram and spans two bins.
both "CREATE TABLE t_dup (a text, b int, n int, v text, PRIMARY KEY (a, b))"
both "ALTER TABLE t_dup ALTER COLUMN a SET STATISTICS 5"
both "INSERT INTO t_dup SELECT CASE WHEN g % 100 < 72 THEN 'h' || (g % 100) / 12 ELSE 'x' || lpad((g % 1000)::text, 4, '0') END, g, g, md5(g::text) FROM generate_series(1, $ROWS) g"
plant t_dup v
sql "$D2" "INSERT INTO t_dup VALUES ('a', 1, 0, 'min'), ('a', 2, 0, 'min'), ('zz', 1, 0, 'max'), ('zz', 2, 0, 'max')"

# 10. Partitioned tables and inheritance parents are not supported yet: a
#     SELECT from the parent returns the rows of the children too.
both "CREATE TABLE t_part (d date, s text, PRIMARY KEY (d, s)) PARTITION BY RANGE (d)"
both "CREATE TABLE t_part_1 PARTITION OF t_part DEFAULT"
both "CREATE TABLE t_inh (k text PRIMARY KEY)"
both "CREATE TABLE t_inh_child (k text PRIMARY KEY) INHERITS (t_inh)"

both "CREATE TABLE t_same (k uuid PRIMARY KEY, v text)"
both "INSERT INTO t_same SELECT md5(g::text)::uuid, repeat('x', g % 50) FROM generate_series(1, $ROWS) g"

# 8. Empty sides and a heavily skewed range.
both "CREATE TABLE t_skew (k text PRIMARY KEY, v text)"
sql "$D1" "INSERT INTO t_skew SELECT 'a' || lpad(g::text, 8, '0'), 'v' FROM generate_series(1, 10) g"
sql "$D2" "INSERT INTO t_skew SELECT 'a' || lpad(g::text, 8, '0'), 'v' FROM generate_series(1, $ROWS) g"
both "CREATE TABLE t_empty (k text PRIMARY KEY, v text)"
sql "$D1" "INSERT INTO t_empty VALUES ('a', 'v'), ('b', 'v')"

both "VACUUM ANALYZE"

echo "--- identical tables"
run t_keyonly 1 0 ""
run t_same 1 0 ""

echo "--- planted differences: 3 differ, 3 only in reference, 4 only in test"
for t in t_text t_comp t_uuid t_ts t_num t_enum t_char t_bit t_dup; do
	run "$t" 1 1 "3 3 4"
	run "$t" 256 1 "3 3 4"
	same "$t"
done

echo "--- parallel: the same lines as a sequential run"
for t in t_text t_comp t_uuid t_ts t_num t_enum t_char t_bit t_dup; do
	run "$t" 512kB 1 "3 3 4" -j 4
	if [ "$t" = t_enum ]; then
		# Three values, all of them most common: no histogram.
		grep -q 'no histogram' "$WORK/log" && pass "$t: no histogram, sequential" || fail "$t: expected a no-histogram warning"
	else
		grep -q 'parallel: ' "$WORK/log" && pass "$t: $(grep -o '[0-9]* key ranges' "$WORK/log")" ||
			{ fail "$t: did not run in parallel"; sed 's/^/    /' "$WORK/log"; }
	fi
	same "$t" 512kB 256
done
B=$(sql "$D1" "SELECT array_length(histogram_bounds::text::text[], 1) FROM pg_stats WHERE tablename = 't_dup' AND attname = 'a'")
D=$(sql "$D1" "SELECT count(DISTINCT b) FROM pg_stats, unnest(histogram_bounds::text::text[]) b WHERE tablename = 't_dup' AND attname = 'a'")
"$BIN" -dsn1 "$D1" -dsn2 "$D2" -table t_dup -j 4 -out "$WORK/dup.out" >"$WORK/log" 2>&1 || true
if [ "$D" -lt "$B" ] && grep -q "parallel: [0-9]* workers, $((D - 1)) key ranges" "$WORK/log"; then
	pass "t_dup: $B histogram bounds, $D distinct, $((D - 1)) ranges"
else
	fail "t_dup: $B bounds, $D distinct: $(grep 'parallel:' "$WORK/log")"
fi

echo "--- partitioned tables and inheritance parents are refused; children are tables"
for t in t_part:'partitioned table' t_inh:'inheritance children'; do
	rc=0
	"$BIN" -dsn1 "$D1" -dsn2 "$D2" -table "${t%%:*}" -out "$WORK/out" >"$WORK/log" 2>&1 || rc=$?
	if [ $rc = 2 ] && grep -q "${t#*:}" "$WORK/log"; then pass "${t%%:*} refused"; else
		fail "${t%%:*}: exit $rc"; sed 's/^/    /' "$WORK/log"; fi
done
for t in t_part_1 t_inh_child; do
	rc=0
	"$BIN" -dsn1 "$D1" -dsn2 "$D2" -table "$t" -out "$WORK/out" >"$WORK/log" 2>&1 || rc=$?
	[ $rc = 0 ] && pass "$t compared" || { fail "$t: exit $rc"; sed 's/^/    /' "$WORK/log"; }
done

echo "--- skew and empty sides"
run t_skew 1 1 "0 0 $((ROWS - 10))"
run t_empty 1 1 "0 2 0"
sql "$D1" "TRUNCATE t_empty"; sql "$D2" "INSERT INTO t_empty VALUES ('c', 'v')"
run t_empty 1 1 "0 0 1"

echo "--- filter and explicit columns"
run t_text 1 1 "1 1 3" -filter "k < 'key 00000003' OR k = 'zzz'" -columns v

[ $FAILED -eq 0 ] && echo "--- all checks passed" || { echo "--- FAILED"; exit 1; }

#!/bin/bash
#
# Block maps: pgcmp slice stores the cut of the key space in a table, and
# pgcmp -map compares by it, on two local throwaway clusters.
#
# A map must cut exactly as the slicer of a comparison does (the -map report
# is the -split fixed report), count its rows right (N per block, the rest in
# the last), appear whole or not at all (a slicer killed mid-pass leaves no
# map and no temporary table), never overwrite without -replace nor drop
# anything but a map, and be refused when it does not fit the comparison
# (another key, another filter) or is broken (a gap, a missing tail).
#
# Needs initdb, pg_ctl and psql in PATH. Usage: test/map.sh

set -euo pipefail

HERE=$(cd "$(dirname "$0")/.." && pwd)
BIN=$HERE/pgcmp
WORK=$(mktemp -d)
D1="host=localhost port=55441 user=postgres dbname=postgres"
D2="host=localhost port=55442 user=postgres dbname=postgres"
ROWS=${ROWS:-100000}
N=1000

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
is() { # description, expected, actual
	[ "$2" = "$3" ] && pass "$1" || fail "$1: got '$3', expected '$2'"
}

# cmp_ <name> <expected exit> [pgcmp args] / slice <name> <expected exit> [args]
cmp_() {
	local name=$1 want=$2 rc=0
	shift 2
	"$BIN" -dsn1 "$D1" -dsn2 "$D2" -out "$WORK/$name" "$@" >"$WORK/$name.log" 2>&1 || rc=$?
	[ "$rc" = "$want" ] || { fail "$name: exit $rc, expected $want"; sed 's/^/    /' "$WORK/$name.log"; return 1; }
}
slice() {
	local name=$1 want=$2 rc=0
	shift 2
	"$BIN" slice -dsn "$D1" "$@" >"$WORK/$name.log" 2>&1 || rc=$?
	[ "$rc" = "$want" ] || { fail "$name: exit $rc, expected $want"; sed 's/^/    /' "$WORK/$name.log"; return 1; }
}
logs() { grep -q -- "$2" "$WORK/$1.log" && pass "$1: $3" || { fail "$1: $3 (no '$2')"; sed 's/^/    /' "$WORK/$1.log"; }; }
bytes() {
	cmp -s "$WORK/$1" "$WORK/$2" && pass "$2: report identical to $1" ||
		{ fail "$2: report differs from $1"; diff "$WORK/$1" "$WORK/$2" | head | sed 's/^/    /' || true; }
}
set_() {
	diff <(sort "$WORK/$1") <(sort "$WORK/$2") >/dev/null && pass "$2: same lines as $1" ||
		{ fail "$2: lines differ from $1"; diff <(sort "$WORK/$1") <(sort "$WORK/$2") | head | sed 's/^/    /' || true; }
}

(cd "$HERE" && go build -o "$BIN")
start n1 55441
start n2 55442

echo "--- loading $ROWS rows"
both "CREATE TABLE big (id bigint PRIMARY KEY, v text)"
both "INSERT INTO big SELECT g, md5(g::text) FROM generate_series(1, $ROWS) g"
both "CREATE TABLE comp (a int, b text, v text, PRIMARY KEY (a, b))"
both "INSERT INTO comp SELECT g / 1000, md5(g::text), 'v' FROM generate_series(1, $ROWS) g"
both "CREATE TABLE procs (f regproc PRIMARY KEY, v text)"
both "INSERT INTO procs SELECT oid::regproc, proname FROM pg_proc"
both "CREATE TABLE empty (k text PRIMARY KEY)"
both "CREATE TABLE slow (id int PRIMARY KEY)"
both "INSERT INTO slow SELECT generate_series(1, 20000)"
sql "$D2" "UPDATE big SET v = 'changed' WHERE id IN (1, $((N - 1)), $N, $((N + 1)), $ROWS)"
sql "$D2" "INSERT INTO big VALUES (-1, 'test only'), ($((ROWS + 1)), 'test only')"
sql "$D2" "UPDATE comp SET v = 'changed' WHERE a IN (0, 1, 50, $((ROWS / 1000)))"
sql "$D2" "UPDATE procs SET v = 'changed' WHERE f::regprocedure::text LIKE 'avg(%'"
both "VACUUM ANALYZE"
sql "$D1" "CREATE SCHEMA maps"

echo "--- a map cuts as the slicer does"
for t in big comp; do
	slice "$t.slice" 0 -table $t -map maps.$t -rows $N
	is "$t: blocks" "$((ROWS / N + 1))" "$(sql "$D1" "SELECT count(*) FROM maps.$t")"
	is "$t: rows" "$ROWS" "$(sql "$D1" "SELECT sum(rows) FROM maps.$t")"
	is "$t: N rows in every block but the last" "$((ROWS / N))" "$(sql "$D1" "SELECT count(*) FROM maps.$t WHERE rows = $N AND hi IS NOT NULL")"
	is "$t: open ends" "t t" "$(sql "$D1" "SELECT (SELECT lo IS NULL FROM maps.$t WHERE seq = 0), (SELECT hi IS NULL FROM maps.$t ORDER BY seq DESC LIMIT 1)" | tr '|' ' ')"
	is "$t: each block starts where the previous one ended" "0" \
		"$(sql "$D1" "SELECT count(*) FROM maps.$t m JOIN maps.$t p ON p.seq = m.seq - 1 WHERE m.lo IS DISTINCT FROM p.hi")"
	cmp_ "$t.seq" 1 -table $t
	cmp_ "$t.fixed" 1 -table $t -split fixed -rows $N -j 4
	cmp_ "$t.map" 1 -table $t -map maps.$t -j 4
	logs "$t.map" "$((ROWS / N + 1)) blocks of $N rows from the block map" "blocks from the map"
	cmp_ "$t.map1" 1 -table $t -map maps.$t -j 1
done
bytes big.seq big.map
bytes big.fixed big.map
bytes big.seq big.map1
set_ comp.seq comp.map
set_ comp.fixed comp.map
set_ comp.seq comp.map1

echo "--- regproc key, filter, empty table"
slice procs.slice 0 -table procs -map maps.procs -rows 7
cmp_ procs.seq 1 -table procs
cmp_ procs.map 1 -table procs -map maps.procs -j 4 && set_ procs.seq procs.map
slice bigf.slice 0 -table big -map maps.bigf -rows 100 -filter "id % 10 = 0"
is "filter: rows of the map are the matching rows" "$((ROWS / 10))" "$(sql "$D1" "SELECT sum(rows) FROM maps.bigf")"
cmp_ bigf.seq 1 -table big -filter "id % 10 = 0"
cmp_ bigf.map 1 -table big -filter "id % 10 = 0" -map maps.bigf -j 4 && bytes bigf.seq bigf.map
slice empty.slice 0 -table empty -map maps.empty -rows 10
is "empty table: one open block of no rows" "0|||0" "$(sql "$D1" "SELECT seq, lo, hi, rows FROM maps.empty")"

echo "--- refusals"
slice again 2 -table big -map maps.big -rows 500 && logs again "exists; -replace replaces it" "an existing map without -replace"
is "the old map is intact" "$((ROWS / N + 1))" "$(sql "$D1" "SELECT count(*) FROM maps.big")"
slice replace 0 -table big -map maps.big -rows 500 -replace
is "-replace: the new map" "$((ROWS / 500 + 1))" "$(sql "$D1" "SELECT count(*) FROM maps.big")"
slice notamap 2 -table big -map comp -replace && logs notamap "drops only a block map" "-replace on a table that is not a map"
is "the table is intact" "$ROWS" "$(sql "$D1" "SELECT count(*) FROM comp")"
cmp_ wrongkey 2 -table big -map maps.comp && logs wrongkey "the map was cut on key" "a map of another key"
cmp_ wrongfilter 2 -table big -map maps.big -filter "id > 0" &&
	logs wrongfilter "the map was cut with -filter" "a map cut without the filter of the comparison"
cmp_ wrongfilter2 2 -table big -map maps.bigf &&
	logs wrongfilter2 "the map was cut with -filter" "a map cut with a filter the comparison has not"
cmp_ splitset 1 -table big -map maps.big -split fixed && logs splitset "-split is ignored" "-split with -map: a warning"
sql "$D1" "CREATE TABLE maps.gap AS SELECT * FROM maps.big WHERE seq <> 3"
sql "$D1" "COMMENT ON TABLE maps.gap IS $(sql "$D1" "SELECT quote_literal(obj_description('maps.big'::regclass, 'pg_class'))")"
cmp_ gap 2 -table big -map maps.gap && logs gap "is broken at block 3" "a map with a gap"
sql "$D1" "CREATE TABLE maps.tail AS SELECT * FROM maps.big WHERE seq < 10"
sql "$D1" "COMMENT ON TABLE maps.tail IS $(sql "$D1" "SELECT quote_literal(obj_description('maps.big'::regclass, 'pg_class'))")"
cmp_ tail 2 -table big -map maps.tail && logs tail "is incomplete" "a map without its tail"

echo "--- the slicer killed mid-pass: no map, no temporary table"
# The filter makes the pass slow (a millisecond a row), so that there is
# time to kill the slicer session.
"$BIN" slice -dsn "$D1" -table slow -map maps.slow -rows 10 -filter "pg_sleep(0.001) IS NOT NULL" \
	>"$WORK/killed.log" 2>&1 &
pid=$!
for _ in $(seq 100); do
	[ "$(sql "$D1" "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'pgcmp slicer' AND query LIKE '%FETCH%'")" = 1 ] && break
	sleep 0.1
done
is "the slicer was killed" "t" "$(sql "$D1" "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'pgcmp slicer'")"
rc=0; wait $pid || rc=$?
is "killed: exit code" 2 "$rc"
is "killed: no map" "" "$(sql "$D1" "SELECT to_regclass('maps.slow')")"
sleep 0.5 # the writer's backend may still be exiting
is "killed: no temporary table" 0 "$(sql "$D1" "SELECT count(*) FROM pg_class WHERE relname = 'pgcmp_map'")"

[ $FAILED -eq 0 ] && echo "--- all checks passed" || { echo "--- FAILED"; exit 1; }

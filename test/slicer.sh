#!/bin/bash
#
# The slicer (-split fixed, and -split histogram falling back to it) on two
# local throwaway clusters.
#
# Blocks of a fixed number of rows must give the same report as a
# sequential run: byte for byte for merge (a single integer key), the same
# set of lines for hash blocks. Differences are planted where the block
# logic can break: on block bounds and next to them, on the first and the
# last key, below the minimum and above the maximum of the reference side.
# Then the edge cases (empty tables, one row, exactly N rows, a filter that
# keeps 1 % of the rows, a table never analysed, a regproc key, blocks of
# one row, hash sub-blocks inside a block), the choice -split makes, and
# the plan warning of the slicer.
#
# Needs initdb, pg_ctl and psql in PATH. Usage: test/slicer.sh

set -euo pipefail

HERE=$(cd "$(dirname "$0")/.." && pwd)
BIN=$HERE/pgcmp
WORK=$(mktemp -d)
D1="host=localhost port=55451 user=postgres dbname=postgres"
D2="host=localhost port=55452 user=postgres dbname=postgres"
ROWS=${ROWS:-200000}
N=1000 # rows in a block

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

# run <name> <expected exit> [pgcmp args]: the report goes to $WORK/<name>,
# the log to $WORK/<name>.log.
run() {
	local name=$1 want=$2 rc=0
	shift 2
	"$BIN" -dsn1 "$D1" -dsn2 "$D2" -out "$WORK/$name" "$@" >"$WORK/$name.log" 2>&1 || rc=$?
	if [ "$rc" != "$want" ]; then
		fail "$name: exit $rc, expected $want"; sed 's/^/    /' "$WORK/$name.log"; return 1
	fi
}
# bytes <a> <b>: the reports are identical; set <a> <b>: the same lines.
bytes() {
	if cmp -s "$WORK/$1" "$WORK/$2"; then pass "$2: report identical to $1"; else
		fail "$2: report differs from $1"; diff "$WORK/$1" "$WORK/$2" | head | sed 's/^/    /' || true; fi
}
set_() {
	if diff <(sort "$WORK/$1") <(sort "$WORK/$2") >/dev/null; then pass "$2: same lines as $1"; else
		fail "$2: lines differ from $1"; diff <(sort "$WORK/$1") <(sort "$WORK/$2") | head | sed 's/^/    /' || true; fi
}
# logs <name> <pattern> <description>: the log of a run has the pattern.
logs() {
	if grep -q -- "$2" "$WORK/$1.log"; then pass "$1: $3"; else
		fail "$1: $3 (no '$2' in the log)"; sed 's/^/    /' "$WORK/$1.log"; fi
}
nologs() {
	if ! grep -q -- "$2" "$WORK/$1.log"; then pass "$1: $3"; else
		fail "$1: $3 (unexpected '$2')"; sed 's/^/    /' "$WORK/$1.log"; fi
}
lines() { # name expected
	local got
	got=$(wc -l <"$WORK/$1" | tr -d ' ')
	[ "$got" = "$2" ] && pass "$1: $2 report lines" || fail "$1: $got report lines, expected $2"
}

(cd "$HERE" && go build -o "$BIN")
start n1 55451
start n2 55452

echo "--- loading $ROWS rows"
both "CREATE TABLE big (id bigint PRIMARY KEY, v text)"
both "INSERT INTO big SELECT g, md5(g::text) FROM generate_series(1, $ROWS) g"
# A composite key whose first column has few distinct values.
both "CREATE TABLE comp (a int, b text, v text, PRIMARY KEY (a, b))"
both "INSERT INTO comp SELECT g / 1000, md5(g::text), 'v' FROM generate_series(1, $ROWS) g"

# Bounds of the blocks are the keys number N, 2N, ... of the reference
# side. Planted: the first and the last key, N-1, N+1 (changed), N and 2N
# (deleted on the test side), two test-only rows outside the reference
# range, two reference-only rows outside it too: 10 lines.
sql "$D2" "UPDATE big SET v = 'changed' WHERE id IN (1, $((N - 1)), $((N + 1)), $ROWS)"
sql "$D2" "DELETE FROM big WHERE id IN ($N, $((2 * N)))"
sql "$D2" "INSERT INTO big VALUES (-5, 'test only'), ($((ROWS + 100)), 'test only')"
sql "$D1" "INSERT INTO big VALUES (-10, 'reference only'), ($((ROWS + 50)), 'reference only')"
ranked="SELECT a, b FROM (SELECT a, b, row_number() OVER (ORDER BY a, b) AS rn FROM comp) s"
sql "$D2" "UPDATE comp SET v = 'changed' WHERE (a, b) IN ($ranked WHERE rn IN (1, $((N - 1)), $((N + 1)), $ROWS))"
sql "$D2" "DELETE FROM comp WHERE (a, b) IN ($ranked WHERE rn IN ($N, $((2 * N))))"
sql "$D2" "INSERT INTO comp VALUES (-1, 'x', 'test only'), (1000000, 'x', 'test only')"
sql "$D1" "INSERT INTO comp VALUES (-2, 'y', 'reference only'), (1000001, 'y', 'reference only')"

# Edge cases, all keyed by text, N = 10 rows in a block.
for t in e_empty e_refempty e_one e_n e_n1 e_nm1 e_small; do
	both "CREATE TABLE $t (k text PRIMARY KEY, v text)"
done
sql "$D2" "INSERT INTO e_refempty VALUES ('a', 'v'), ('b', 'v'), ('c', 'v')"
both "INSERT INTO e_one VALUES ('a', 'v')"
sql "$D2" "UPDATE e_one SET v = 'changed'"
for t in e_n:10 e_n1:11 e_nm1:9 e_small:300; do
	both "INSERT INTO ${t%%:*} SELECT lpad(g::text, 4, '0'), 'v' FROM generate_series(1, ${t#*:}) g"
	sql "$D2" "INSERT INTO ${t%%:*} VALUES ('zzzz', 'test only')"
	sql "$D2" "UPDATE ${t%%:*} SET v = 'changed' WHERE k IN ('0001', '0010')"
done

# Keyed by regproc: bounds must go through regprocedure, overloaded
# functions print as a bare name. Same OIDs on both clusters.
both "CREATE TABLE procs (f regproc PRIMARY KEY, v text)"
both "INSERT INTO procs SELECT oid::regproc, proname FROM pg_proc"
sql "$D2" "UPDATE procs SET v = 'changed' WHERE f::regprocedure::text LIKE 'avg(%' OR f::regprocedure::text LIKE 'sum(%'"

both "VACUUM ANALYZE"
# Never analysed: no histogram, no reltuples.
both "CREATE TABLE fresh (id int PRIMARY KEY, v text)"
both "INSERT INTO fresh SELECT g, 'v' FROM generate_series(1, 5000) g"

echo "--- merge: blocks of the slicer against a sequential run"
run big.seq 1 -table big && lines big.seq 10
run big.fixed 1 -table big -split fixed -rows $N -j 4 && bytes big.seq big.fixed
logs big.fixed "slicer: $(((ROWS + 2) / N)) bounds" "$(((ROWS + 2) / N)) bounds for $((ROWS + 2)) rows"
run big.fixed1 1 -table big -split fixed -rows $N -j 1 && bytes big.seq big.fixed1
logs big.fixed1 "parallel: 1 workers, blocks of $N rows" "the slicer with one worker"
run big.odd 1 -table big -split fixed -rows 7777 -j 3 && bytes big.seq big.odd
run big.mem 1 -table big -split fixed -mem 1MB -j 4 && bytes big.seq big.mem
logs big.mem "estimated from -mem 1MB and the average row width" "rows of a block estimated from -mem"

echo "--- hash blocks: blocks of the slicer against a sequential run"
run comp.seq 1 -table comp && lines comp.seq 10
run comp.fixed 1 -table comp -split fixed -rows $N -j 4 && set_ comp.seq comp.fixed
# 16 kB of rows hold about a hundred: every block of the slicer is
# compared in several hash sub-blocks.
run comp.sub 1 -table comp -split fixed -rows $N -j 4 -mem 16kB && set_ comp.seq comp.sub
blocks=$(sed -n 's/.*blocks: \([0-9]*\).*/\1/p' "$WORK/comp.sub.log")
slices=$(sed -n 's/.*slicer: [0-9]* bounds, \([0-9]*\) blocks.*/\1/p' "$WORK/comp.sub.log")
[ "${blocks:-0}" -gt "${slices:-0}" ] && pass "comp.sub: $blocks hash blocks in $slices blocks of the slicer" ||
	fail "comp.sub: $blocks hash blocks, $slices blocks of the slicer"

echo "--- edge cases (blocks of 10 rows)"
edge() { # table, expected exit, expected bounds
	run "$1.seq" "$2" -table "$1" || return 0
	run "$1.fixed" "$2" -table "$1" -split fixed -rows 10 -j 2 || return 0
	set_ "$1.seq" "$1.fixed"
	logs "$1.fixed" "slicer: $3 bounds" "$3 bounds"
}
edge e_empty 0 0
edge e_refempty 1 0
edge e_one 1 0
edge e_n 1 1  # exactly N rows: one bound, the second block empty on the reference side
edge e_n1 1 1
edge e_nm1 1 0
run e_small.seq 1 -table e_small
run e_small.rows1 1 -table e_small -split fixed -rows 1 -j 4 && set_ e_small.seq e_small.rows1
logs e_small.rows1 "slicer: 300 bounds" "blocks of one row: 300 bounds"

echo "--- regproc key"
run procs.seq 1 -table procs
run procs.fixed 1 -table procs -split fixed -rows 7 -j 4 && set_ procs.seq procs.fixed

echo "--- filter: blocks over the matching rows only"
run big.fseq 1 -table big -filter "id % 100 = 0"
run big.ffixed 1 -table big -filter "id % 100 = 0" -split fixed -rows 100 -j 4 && bytes big.fseq big.ffixed
logs big.ffixed "slicer: $((ROWS / 100 / 100)) bounds" "$((ROWS / 100 / 100)) bounds for $((ROWS / 100)) matching rows"

echo "--- -split chooses"
run big.hist 1 -table big -j 4 && bytes big.seq big.hist
logs big.hist "key ranges from the histogram" "histogram: analysed, enough bins"
run big.few 1 -table big -j 30 && bytes big.seq big.few
logs big.few "fewer than 4 per worker" "few bins per worker: the slicer"
run big.filt 1 -table big -j 4 -filter "id % 100 = 0" && bytes big.fseq big.filt
logs big.filt "-filter is set" "-filter: the slicer"
run fresh.par 0 -table fresh -j 4
logs fresh.par "no histogram for" "never analysed: the slicer"
logs fresh.par "the table has never been analysed" "never analysed: rows of a block by default"
run big.none 1 -table big -j 4 -split none && bytes big.seq big.none
nologs big.none "parallel:" "-split none: one range"
nologs big.seq "parallel:" "-j 1: one range"

echo "--- plan check: the slicer cannot walk the index"
for s in enable_indexscan enable_indexonlyscan enable_bitmapscan; do sql "$D1" "ALTER DATABASE postgres SET $s = off"; done
run big.noidx 1 -table big -split fixed -rows $N -j 2 && bytes big.seq big.noidx
logs big.noidx "the slicer will read" "warns about a sequential scan"
sql "$D1" "ALTER DATABASE postgres RESET ALL"
run big.idx 1 -table big -split fixed -rows $N -j 2
nologs big.idx "the slicer will read" "no warning with the index"

[ $FAILED -eq 0 ] && echo "--- all checks passed" || { echo "--- FAILED"; exit 1; }

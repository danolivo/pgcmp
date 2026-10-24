#!/bin/bash
#
# Parallel mode check on two local throwaway clusters.
#
# The report of -j 4 must be byte-identical to the report of -j 1: same
# differences, same order. Differences are planted where range logic can
# break: on histogram bounds, next to them, and outside the histogram
# (below the minimum and above the maximum the statistics saw).
#
# Needs initdb, pg_ctl and psql in PATH. Usage: test/parallel.sh

set -euo pipefail

HERE=$(cd "$(dirname "$0")/.." && pwd)
BIN=$HERE/pgcmp
WORK=$(mktemp -d)
P1=55461
P2=55462
D1="host=localhost port=$P1 user=postgres dbname=postgres"
D2="host=localhost port=$P2 user=postgres dbname=postgres"
ROWS=${ROWS:-2000000}

stop() { pg_ctl -D "$WORK/$1" -m fast stop >/dev/null 2>&1 || true; }
trap 'stop n1; stop n2; rm -rf "$WORK"' EXIT

start() { # dir port
	initdb -D "$WORK/$1" -U postgres --no-locale -E UTF8 >/dev/null
	pg_ctl -D "$WORK/$1" -l "$WORK/$1.log" -w start \
		-o "-p $2 -k '' -c listen_addresses=localhost -c autovacuum=off" >/dev/null
}
sql() { psql -X -q -At -v ON_ERROR_STOP=1 -d "$1" -c "$2"; }

check() { # description, expected exit code, actual exit code
	if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1 (exit $3, expected $2)"; FAILED=1; fi
}
FAILED=0

(cd "$HERE" && go build -o "$BIN")
start n1 $P1
start n2 $P2

echo "--- loading $ROWS rows"
for D in "$D1" "$D2"; do
	sql "$D" "CREATE TABLE big (id bigint PRIMARY KEY, v text, n numeric)"
	sql "$D" "INSERT INTO big SELECT g, md5(g::text), g / 7.0 FROM generate_series(1, $ROWS) g"
done
sql "$D1" "ANALYZE big"
BOUNDS=$(sql "$D1" "SELECT array_length(histogram_bounds::text::int8[], 1) FROM pg_stats WHERE tablename = 'big' AND attname = 'id'")
B10=$(sql "$D1" "SELECT (histogram_bounds::text::int8[])[10] FROM pg_stats WHERE tablename = 'big' AND attname = 'id'")
B50=$(sql "$D1" "SELECT (histogram_bounds::text::int8[])[50] FROM pg_stats WHERE tablename = 'big' AND attname = 'id'")
echo "--- histogram: $BOUNDS bounds; planting differences at bounds $B10 and $B50"

# Differences on the test side.
sql "$D2" "UPDATE big SET v = 'first' WHERE id = 1"
sql "$D2" "UPDATE big SET v = 'on bound' WHERE id = $B10"
sql "$D2" "DELETE FROM big WHERE id = $B10 - 1"
sql "$D2" "DELETE FROM big WHERE id = $B50"
sql "$D2" "INSERT INTO big VALUES ($B50 + 1000000000, 'test only, far right', 0)"
sql "$D2" "INSERT INTO big VALUES (-5, 'test only, below min', 0)"
sql "$D2" "UPDATE big SET n = -n WHERE id = $ROWS"
# Rows the reference histogram has never seen.
sql "$D1" "INSERT INTO big VALUES (-100, 'reference only, below min', 0), ($ROWS + 5, 'reference only, above max', 0)"
EXPECTED=9

run() { # out-file, args... ; returns pgcmp's exit code
	local out=$1; shift
	local rc=0
	"$BIN" -dsn1 "$D1" -dsn2 "$D2" -out "$out" "$@" >"$out.log" 2>&1 || rc=$?
	grep -h -e parallel -e warning -e 'done in' "$out.log" | sed 's/^/    /' || true
	return $rc
}

echo "--- big table, -j 1 vs -j 4"
rc=0; run "$WORK/j1" -table big -j 1 || rc=$?; check "-j 1 finds differences" 1 $rc
rc=0; run "$WORK/j4" -table big -j 4 || rc=$?; check "-j 4 finds differences" 1 $rc
check "-j 1 report has $EXPECTED lines" $EXPECTED "$(wc -l <"$WORK/j1" | tr -d ' ')"
if cmp -s "$WORK/j1" "$WORK/j4"; then echo "ok   reports are identical"; else
	echo "FAIL reports differ"; diff "$WORK/j1" "$WORK/j4" | head -20; FAILED=1; fi
sed 's/^/    /' "$WORK/j4" | cut -c1-100

echo "--- filter combined with ranges"
rc=0; run "$WORK/f1" -table big -j 1 -filter "id % 2 = 0" || rc=$?
rc=0; run "$WORK/f4" -table big -j 4 -filter "id % 2 = 0" || rc=$?
if cmp -s "$WORK/f1" "$WORK/f4"; then echo "ok   filtered reports are identical"; else
	echo "FAIL filtered reports differ"; FAILED=1; fi

# 100 ranges -> 100 workers -> 101 connections per server, more than the
# default max_connections: pgcmp must go on with the workers that connected.
echo "--- more workers than ranges and than connection slots"
rc=0; run "$WORK/j500" -table big -j 500 || rc=$?; check "-j 500 finds differences" 1 $rc
cmp -s "$WORK/j1" "$WORK/j500" && echo "ok   -j 500 report is identical" || { echo "FAIL -j 500"; FAILED=1; }

echo "--- no histogram: warning, sequential run"
for D in "$D1" "$D2"; do
	sql "$D" "CREATE TABLE nohist (id int PRIMARY KEY, v text)"
	sql "$D" "INSERT INTO nohist SELECT g, g::text FROM generate_series(1, 1000) g"
done
rc=0; run "$WORK/nh" -table nohist -j 4 || rc=$?; check "no histogram: match" 0 $rc
grep -q "no histogram" "$WORK/nh.log" && echo "ok   warning printed" || { echo "FAIL no warning"; FAILED=1; }

echo "--- plan check: index usable, no warning"
if grep -h "sequential scan" "$WORK/j1.log" "$WORK/j4.log"; then
	echo "FAIL unexpected plan warning"; FAILED=1
else echo "ok   no plan warning"; fi

echo "--- plan check: index scans disabled on the test side"
sql "$D2" "ALTER DATABASE postgres SET enable_indexscan = off"
sql "$D2" "ALTER DATABASE postgres SET enable_bitmapscan = off"
rc=0; run "$WORK/s1" -table big -j 1 || rc=$?; check "-j 1 still finds differences" 1 $rc
grep -q "test: .* sequential scan and sorted in full" "$WORK/s1.log" \
	&& echo "ok   -j 1 warns about a full sort" || { echo "FAIL -j 1: no warning"; FAILED=1; }
grep -q "reference: .* sequential scan" "$WORK/s1.log" \
	&& { echo "FAIL reference side warned"; FAILED=1; } || echo "ok   reference side does not warn"
rc=0; run "$WORK/s4" -table big -j 4 || rc=$?
grep -q "test: 100 of 100 key range queries .* sequential scan" "$WORK/s4.log" \
	&& echo "ok   -j 4 warns with a range count" || { echo "FAIL -j 4: no warning"; FAILED=1; }
cmp -s "$WORK/j1" "$WORK/s4" && echo "ok   report unchanged" || { echo "FAIL report changed"; FAILED=1; }
sql "$D2" "ALTER DATABASE postgres RESET ALL"

[ $FAILED -eq 0 ] && echo "--- all checks passed" || { echo "--- FAILED"; exit 1; }

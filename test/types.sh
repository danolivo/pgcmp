#!/bin/bash
#
# Type negotiation with user-defined types whose OIDs differ between the
# two servers. Types must be matched by name, never by OID.
#
# Needs initdb, pg_ctl and psql in PATH. Usage: test/types.sh

set -euo pipefail

HERE=$(cd "$(dirname "$0")/.." && pwd)
BIN=$HERE/pgcmp
WORK=$(mktemp -d)
D1="host=localhost port=55481 user=postgres dbname=postgres"
D2="host=localhost port=55482 user=postgres dbname=postgres"

stop() { pg_ctl -D "$WORK/$1" -m fast stop >/dev/null 2>&1 || true; }
trap 'stop n1; stop n2; rm -rf "$WORK"' EXIT
start() { # dir port
	initdb -D "$WORK/$1" -U postgres --no-locale -E UTF8 >/dev/null
	pg_ctl -D "$WORK/$1" -l "$WORK/$1.log" -w start -o "-p $2 -k '' -c listen_addresses=localhost" >/dev/null
}
sql() { psql -X -q -At -v ON_ERROR_STOP=1 -d "$1" -c "$2"; }

FAILED=0
expect() { # description, expected exit code, pgcmp args...
	local what=$1 want=$2 rc=0; shift 2
	"$BIN" -dsn1 "$D1" -dsn2 "$D2" -out "$WORK/out" "$@" >"$WORK/log" 2>&1 || rc=$?
	if [ "$rc" = "$want" ]; then echo "ok   $what"; else
		echo "FAIL $what (exit $rc, expected $want)"; sed 's/^/    /' "$WORK/log"; FAILED=1; fi
}

(cd "$HERE" && go build -o "$BIN")
start n1 55481
start n2 55482

# Shift OIDs on the test side, then create the same types on both.
sql "$D2" "CREATE TYPE pad1 AS ENUM ('x'); CREATE TYPE pad2 AS ENUM ('x'); CREATE DOMAIN pad3 AS int"
for D in "$D1" "$D2"; do
	sql "$D" "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')"
	sql "$D" "CREATE DOMAIN posint AS int CHECK (VALUE > 0)"
done
echo "--- OIDs (reference / test):"
for t in mood posint; do
	echo "    $t: $(sql "$D1" "SELECT '$t'::regtype::oid") / $(sql "$D2" "SELECT '$t'::regtype::oid")"
done

sql "$D1" "CREATE TABLE t (id int PRIMARY KEY, m mood, p posint, d posint, n numeric(10,2), v varchar(5))"
sql "$D2" "CREATE TABLE t (id bigint PRIMARY KEY, m mood, p posint, d bigint, n numeric(10,2), v varchar(10))"
for D in "$D1" "$D2"; do
	sql "$D" "INSERT INTO t VALUES (1, 'happy', 7, 42, 15.5, 'ab'), (2, 'sad', 1, 1, 0, '')"
done
# d: domain over int vs bigint -> bigint; v: varchar(5) vs varchar(10) ->
# varchar, whose output does not depend on the modifier.
expect "same user types, different OIDs; domain vs bigint; varchar(5) vs varchar(10)" 0 -table t
grep -h 'columns:' "$WORK/log" | sed 's/^/    /'

sql "$D2" "UPDATE t SET m = 'ok' WHERE id = 2"
expect "enum value difference is reported" 1 -table t
sed 's/^/    /' "$WORK/out"

sql "$D1" "CREATE TABLE u (id int PRIMARY KEY, m mood)"
sql "$D2" "CREATE TABLE u (id int PRIMARY KEY, m text)"
expect "enum vs text: no common type" 2 -table u
grep -h 'no common type' "$WORK/log" | sed 's/^/    /'

# The server drops differing modifiers. Nothing may be lost on the way:
# 15.554 vs 15.55 must stay a difference (casting to numeric(10,2) would
# have rounded it away), microseconds vs milliseconds must stay a
# difference, and equal numerics with different scales show up as noise.
sql "$D1" "CREATE TABLE w (id int PRIMARY KEY, n numeric(12,3), ts timestamp(6))"
sql "$D2" "CREATE TABLE w (id int PRIMARY KEY, n numeric(10,2), ts timestamptz(3))"
sql "$D1" "INSERT INTO w VALUES (1, 15.554, '2026-01-01 10:00:00.5'), (2, 1.5, '2026-01-01 10:00:00.123456')"
sql "$D2" "INSERT INTO w VALUES (1, 15.55,  '2026-01-01 10:00:00.5+00'), (2, 1.5, '2026-01-01 10:00:00.123+00')"
expect "different modifiers: nothing rounded away" 1 -table w
sed 's/^/    /' "$WORK/out"
grep -q '^1 (15.554,' "$WORK/out" && grep -q '123456' "$WORK/out" \
	&& echo "ok   15.554 and .123456 survived the casts" || { echo "FAIL a value was rounded"; FAILED=1; }

[ $FAILED -eq 0 ] && echo "--- all checks passed" || { echo "--- FAILED"; exit 1; }

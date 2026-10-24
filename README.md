# pgcmp — comparing one table on two PostgreSQL instances

A mock-up of the "straightforward" approach from the document *Table Comparison — Do we need
technology?*: two streams of rows in primary key order, with no hash
trees. Serialization happens entirely on the server: a row is compared as the text
`ROW(c1, c2, …)` — exactly what psql shows for a composite value.

The way rows are matched by key is chosen once, when the
columns of the primary key are known (`strategy.go`):

* **merge** (`merge.go`) — a single-column key of type `int2`/`int4`/`int8`.
  Both sides are read as streams and merged on the client;
* **hash blocks** (`hashblock.go`) — any other key: of any type, with
  any number of columns. The client never decides which of two keys
  is smaller — the servers do that. Details below.

Both have a parallel mode: ranges are taken from the histogram of the
(first) key column.

## Build

```sh
go mod tidy
go build
go test
```

CI (`.github/workflows/tests.yml`) runs on every pull request and every
push to `main`: `gofmt`, `go vet` and the unit tests with `-race`; the
scripts on local clusters below (every `test/*.sh` but `regress.sh`) on
PostgreSQL 16, 17 and 18; and the regression suite on two instances
(`regress.sh`).

Regression suite on two instances (needs Docker; ~2 minutes, the first
run takes ~12 minutes to build the image):

```sh
test/regress.sh        # IMAGE=postgres:18 by default, KEEP=1 — keep the containers,
                       # JOBS=4 — workers for the parallel pass, MEM=4kB — block size,
                       # SELF=0 — no self-comparison, CROSS=0 — no second instance
```

The script builds a local image once from the stock image:
`pgcmp-regress:<version>-<base image id>`. It contains build tools and the
regression suite compiled from the sources of the same version
(the tarball is cached in `~/.cache/pgcmp`). After that, this image is
reused until the server version or the base image itself changes;
`REBUILD=1` forces a rebuild.

Two instances are started from the image: the reference A and the test B. On B, before
the suite runs, `test/oidshift.sql` creates and drops one object of each
kind that is assigned an OID: table, type, enum, function, operator, operator
class, access method, cast, rule, trigger, event
trigger, language, transform, full-text search objects, FDW, server,
user mapping, publication, subscription, extension, large object, role,
tablespace, database, etc. Fresh instances start from the same
OID, and without this step the suite would produce almost the same
OIDs on both; with the OID shift, the relfilenodes, the order of `reg*` values and the parity of enum
label OIDs do not match on the two sides. The script itself checks the coverage
by catalog: every `pg_catalog` catalog with an `oid` column must
get a new row (a new server version with a new catalog will
stop the script), and none of them must be left after the drop.

Then `make installcheck` runs on both at the same time, and `test/keytables.sql`
adds the schema `pgcmp_keys`: one table for each column type in the database
that can be a primary key, with a key of that type and all the
distinct values that the test suite left in such columns
(NaN, infinities, empty ranges, odd strings…). The names of these
tables do not depend on the type OID (`<typname>_<md5(format_type)>`), so
the tables of the two instances are matched by name. Then autovacuum
is turned off (between the reads of pgcmp and the oracle nobody must write to the
data or to the catalogs from which the key tables were built; the script waits for
workers that are already running),
and `ANALYZE` of the whole database gives the tables histograms.

Every table with a primary key of any type and any columns is compared,
except the system catalogs in `pg_catalog`: between instances they differ
only in OIDs and statistics. Partitioned tables and
inheritance parents are also skipped. Blocks are
`MEM=4kB`, so that keys are sure to cross block boundaries. Each
table is compared sequentially and with `-j JOBS`, in two passes:

1. **A against itself** over two connections. Any result other than
   `match` is a pgcmp error.
2. **A against B**, if the table on B has the same structure: the same columns of the
   same types and the same key. The instances do, of course, differ — OIDs in
   the catalogs and everywhere they are stored, random and temporary data from
   the suite, the order in which objects are created in parallel test groups.
   So the check is not for `match` but for agreement with an independent oracle:
   the script reads the whole table from both sides through `psql` with the same
   session settings as pgcmp, joins rows by key text on the
   client (`awk`) and builds the report that pgcmp should have written.
   The pgcmp and oracle reports are compared as sorted sets of lines,
   and the exit code is 0 or 1 according to the oracle.

   Tables whose contents legitimately differ from run to run
   are left out of this pass (the `CROSS_SKIP` list in the script; self-comparison
   keeps them): `gtest20a` (`random()`), the time key tables
   (`'now'` from `type_sanity`), `text` (names of TOAST tables with OIDs from
   `event_trigger`), and also the key tables filled from the catalogs:
   `oid`, `name`, `oidvector`, `int4`, `float4` and others — OIDs, `relpages`,
   `reltuples` and statistics.

Tables with a key made of `reg*` columns (for example, `pgcmp_keys.reg*`) are ordered by OID, and the OIDs on the two instances differ — such a
comparison is not supported (see "Not supported (yet)"). Their differences from the oracle
are printed on a separate `known:` line and do not fail the test.

Checking the parallel mode on two local clusters (needs `initdb`,
`pg_ctl`, `psql` in `PATH`):

```sh
test/parallel.sh       # ROWS=2000000 by default
```

The `-j 4` report must match the `-j 1` report byte for byte; differences
are planted on bin boundaries, next to them and outside the histogram.

Matching user-defined types (enum, domain) whose OIDs differ on the two
servers (same setup, local clusters):

```sh
test/types.sh
```

Types are matched between servers only by name (`format_type`);
an OID is used only on the server it came from.

Hash blocks (same setup, local clusters):

```sh
test/keys.sh           # ROWS=200000 by default
```

Keys: `text` (with spaces, quotes, commas), a composite
`(int, text)` against `(bigint, varchar)`, `uuid`, `timestamptz` with
`±infinity`, `numeric` with `NaN`, a composite with an enum whose OID differs on the
two sides, `char(n)`, `bit(n)`, a composite with a repeated value in the histogram of the
first column, a table that consists of the key only, skew, empty sides. On the
test side, 3 updates, 3 deletes and 4 inserts are planted, including
some below the minimum and above the maximum key; pgcmp must find exactly these
with `-mem 1` (dozens of blocks), with one large block and with `-j 4`, and the
report lines must match. Partitioned tables and inheritance
parents must be rejected, while their partitions and children must
be compared.

## Usage

```sh
./pgcmp -dsn1 'host=n1 dbname=db user=u' \
        -dsn2 'host=n2 dbname=db user=u' \
        -table public.orders \
        [-columns id,amount,status] \
        [-filter "status <> 'archived'"] \
        [-j 4] \
        [-mem 64MB] \
        [-rows 3000] \
        [-out orders.diff]
```

* `-dsn1` — the reference side, `-dsn2` — the test side.
* `-table` — the name, with a schema if needed. Without a schema, it is looked up in `public`
  on both sides.
* `-columns` — which columns to compare. By default — exactly those that exist
  on both sides; columns that exist on only one side are
  ignored (their list is printed to the log). A column named explicitly
  must exist on both sides. Primary key columns are removed
  from the list.
* `-filter` — a `WHERE` expression, inserted as is on both sides.
* `-j` — the number of parallel workers (1 by default). Each worker is
  a pair of connections, one per server; the coordinator's own pair is one of
  them, so each server needs `j` connections.
* `-mem` — memory for one block of reference-side rows (by default
  `64MB`), for hash blocks only. Units as in PostgreSQL: `kB`, `MB`,
  `GB`, `TB` (powers of 1024); a number without a unit means megabytes. This is an estimate based on
  row lengths plus per-row overhead; the process RSS can be about twice as large
  because of the Go garbage collector's headroom. In parallel mode
  each worker has its own block, up to `j × mem` in total.
* `-rows` — at most this many reference-side rows in one block (by
  default 0 — no limit), for hash blocks only. Together with `-mem`,
  a block is closed at whichever limit is reached first. A small block
  lets the sides work at the same time: while the test side returns the
  range of the current block, the reference backend writes the next block into the
  socket buffers, and if the block fits in them, it has already been read by the time
  the client returns to it. The cost is a query to the test side for
  each block.

## Hash blocks

1. The reference side is read with **one** query in key order:
   `SELECT ROW(k1, k2)::text, k1::T1::text, k2::T2::text, ROW(c…)::text … ORDER BY t.k1, t.k2`.
   Rows are put into a hash table by key text until the block
   reaches `-mem`. The text values of the key columns of the last row taken
   are the upper bound of the block.
2. The same range is sent to the test side:
   `… WHERE (k1, k2) > ($1::T1, $2::T2) AND (k1, k2) <= ($3::T1, $4::T2) ORDER BY …`.
   The bounds are passed as text parameters and cast to the column types of the
   test side without a modifier, so the comparison is done in the column
   type and uses the index (a btree index serves the row comparison
   `(a, b) > (…)` directly). The type without a modifier is
   `format_type(oid, -1)`, not `(oid, NULL)`: in the second case `bpchar` and
   `bit` are printed as `character` and `bit`, that is, `char(1)` and `bit(1)`, and
   an explicit cast silently truncates the bound. Each row is looked up in the block and goes straight to
   the report; test-side rows are not stored, so memory is limited to
   one block whatever the skew.
3. Block rows that found no match exist only on the reference side.

The first block has no lower bound, and the last one has no upper bound: everything on the
test side below the minimum or above the maximum of the reference side is also included
in the comparison. The reference-side query is not closed between blocks: the client
simply stops reading it, the socket fills up, and the backend waits. No cursor
(`DECLARE CURSOR` is planned with `cursor_tuple_fraction`), no extra
round trips, no repeated index descents.

Keys are matched by the text `ROW(k1, k2, …)::text`, with a cast to a
common type where the types of the key columns differ between the sides. The order
is set by the servers; it is assumed to be the same on both sides (a different
collation of the key gives itself away with a sequential scan during the plan
check).

`regproc` and `regoper` print an overloaded function or operator as a bare
name: in `pg_aggregate` two keys look like `avg`, and `avg` cannot be read
back. For such columns, the key text and the bounds go through
`regprocedure` and `regoperator` (with argument types); the column itself, its
order and its index stay the same.

Report order within a block: first the differences and the rows that exist only on the test
side (in key order), then the rows that exist only on the reference side (also in key
order). For the same data the report is deterministic, but with a different
`-mem` the lines come in a different order; globally it is not sorted by key.

## Parallel mode

The key space is split by the histogram of the primary key of the reference
table (`pg_stats.histogram_bounds`); for a composite key, by the histogram
of its first column: one range per bin, the first one open on the left
and the last one open on the right, so rows outside stale statistics
are not lost. Ranges are half-open (`lo <= k1 < hi`), with no overlaps or
gaps.

* `histogram_bounds` has type `anyarray`: it cannot be cast,
  unnested or subscripted. So the histogram is parsed through
  text: `histogram_bounds::text::<column type>[]`. For hash blocks, the bounds
  are then printed as text again and put into the queries as literals
  `'…'::<type>` (this way the range queries can be checked with `EXPLAIN` before
  the start).
* The first column of a composite key is not unique: a value that occurs more often
  than the bin width but is not in the most common values list appears in the histogram
  several times. The server discards the repeats (`b > lag(b)`); otherwise each
  would give an empty range.
* A type that cannot read its own text (`regproc`) gives a warning
  and a sequential run.
* Within a range, hash blocks work as usual: the lower bound of the first block
  is the range bound, and so is the upper bound of the last one.

* Both sides export the snapshot of the coordinator transaction
  (`pg_export_snapshot()`), and all workers import it — visibility within
  each side is the same. The coordinator pair itself works as one of the `j`
  workers.
* Ranges are handed to workers through a queue with no limit: whatever cuts the
  key space never waits for the workers, and a queued range costs only its
  bounds in memory. A range with differences writes
  them to a temporary file next to `-out`; a range that matched writes nothing
  and leaves no file. As soon as all ranges before a range are done, its file is
  appended to the report and removed. So the report stays in range order, and
  only ranges with differences that finished ahead of a slower one wait on disk. For merge, the report stays sorted by key and
  matches the report of a sequential run; for hash blocks the
  set of lines matches, and the order is as with a different `-mem`.
* No histogram (the table has not been analyzed) — a warning and
  a sequential run.
* The server refused a connection because of `max_connections` — a warning,
  and the work continues with the workers that managed to connect, at least
  the coordinator.

## What it does

1. Connects to both sides and sets the same session settings:
   `search_path = public`, `TimeZone`, `DateStyle`, `IntervalStyle`,
   `bytea_output`, `extra_float_digits`, `lc_monetary`,
   `quote_all_identifiers`, `client_encoding`. Opens a
   `REPEATABLE READ READ ONLY` transaction — the catalog and the data are read from
   one snapshot, which can later be exported for parallel
   workers. The `SQL_ASCII` encoding is allowed only if both sides
   use it: such a server does not convert text and returns raw bytes, while
   any other one converts to UTF8. If the major versions of the servers differ,
   a warning is printed: output functions sometimes change between
   versions (for example, float8 before and after PG 12), and such differences
   come from the servers, not from the data.
2. Matches the structure: on each side, it gets the columns (`format_type`
   with and without typmod) and the primary key.
   * The table is a plain one (`relkind = 'r'`) and not an inheritance parent:
     `SELECT` from a parent would also return the rows of its children. Partitioned
     tables and parents are not supported yet; a single partition or
     child is a plain table.
   * There is a PK, and on both sides it has the same columns in the same
     order. A single `int2`/`int4`/`int8` column (the width may differ between
     the sides) — merge; everything else — hash blocks.
   * If a column type differs between the sides, the common type is chosen entirely by
     the reference server — the type it would give itself for a `UNION` of these two types
     (the type with its modifier is taken from the description of a prepared statement).
     The column is cast to it on each side where the type is different:
     - `int4` + `int8` → `bigint`, `timestamp` + `timestamptz` → `timestamptz`;
     - the server drops differing modifiers: `numeric(10,2)` +
       `numeric(12,3)` → `numeric`, `varchar(5)` + `varchar(10)` → `varchar`;
     - `UNION` cannot match the types (`integer` + `text`) — an error.

     Such a cast loses nothing — it cannot round or truncate a value,
     so a real difference will not be hidden. The downside:
     equal values are sometimes printed differently (`numeric` stores the
     scale of the value: `1.50` against `1.500`; `char(n)` stores padding
     spaces) — this will appear in the report as a difference.
3. Before the start, each query (in parallel mode, the query of each
   range) goes through `EXPLAIN` on its side. If the plan contains
   `Seq Scan` or `Parallel Seq Scan`, a warning is printed: the table
   will be read in full and sorted instead of being walked through the primary key
   index (in parallel mode, each such range reads
   the whole table). Sorting a single range after a bitmap index
   scan does not count: the whole table is not read in that case.
4. Merge: each side gets the query
   `SELECT "pk", ROW("c1", "c2"::<common type>, …)::text FROM … [WHERE (…)] ORDER BY "pk"`.
   Rows are streamed in two goroutines and merged.
   Hash blocks — see above.
5. The report is written to `-out`. It is either a single line `match`, or one line for
   each difference:

   ```
   <pk> <reference table row> <test table row>
   ```

   `<pk>` is an integer for merge and the text `ROW(k1, k2, …)` for hash blocks,
   for example `(42,"a b")`. A missing row is shown as `NULL`.
   The separator is a space: `record_out` quotes any field with a
   whitespace character, so there is never a bare space inside a row, and
   the row itself always starts with `(` and cannot be confused with `NULL`.

Exit codes: `0` — match, `1` — there are differences, `2` — error.

## Not supported (yet)

* Keys of types that cannot read back their own text
  (`pg_node_tree`, `pg_mcv_list`, `pg_dependencies`): the very first block
  bound gives an error on the test side. If the table fits in one
  block, the comparison succeeds.
* Keys of `reg*` types are ordered by OID. Between different servers, object
  OIDs usually differ, and such a key is meaningful to compare only
  when the OIDs match (for example, a table against itself).
* A check that the servers order the key in the same way (different versions of
  glibc/ICU): if the order diverges, the test-side ranges may
  overlap or leave gaps. This is cheap to catch with a `$lo < $hi` check on the
  test side for each block.
* Partitioned tables and tables with inheritance.
* The `timestamp` → `timestamptz` cast is done with `TimeZone = UTC`. If the
  subscriber received the values in a different time zone, there will be false differences.
* A cast to the other side's type can fail on overflow
  (`int4` 1000 → `numeric(3,1)`) — then the comparison ends with an error.
* Escaping a newline inside a value: `record_out` quotes it
  but does not escape it, and the report line breaks into two.

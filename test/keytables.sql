-- Key tables for test/regress.sh.
--
-- For every type used by a column anywhere in the regression database
-- (catalogs included), creates
--
--     pgcmp_keys."<typname>_<hash>" (k <type> PRIMARY KEY, src text)
--
-- where <hash> is a prefix of md5(format_type(type)): the name tells types
-- of the same name in different schemas apart and, unlike the type OID, is
-- the same in every database, so the key tables of two databases built by
-- separate runs of the suite pair up by name (the cross-database pass of
-- test/regress.sh). It fills each table with the distinct values of all
-- columns of that type; src
-- names the column a value came from. A type without a default btree
-- operator class cannot be a primary key and is skipped, as is a column
-- whose values do not fit (bit(n) into bit, a value too long for a btree
-- entry, ...). Tables that end up empty are dropped.
--
-- The regression suite stores plenty of edge values (NaN, infinities,
-- empty and odd strings, extreme dates, empty ranges), so every such table
-- is a good test of the key round trip in the hash-block strategy: the
-- text of a block bound must come back as exactly the same key on the
-- other side.

-- The round-trip check below runs with the output settings pgcmp uses
-- (sessionSetup in schema.go).
SET TimeZone = 'UTC';
SET DateStyle = 'ISO, YMD';
SET IntervalStyle = 'postgres';
SET bytea_output = 'hex';
SET extra_float_digits = 3;
SET lc_monetary = 'C';

CREATE SCHEMA pgcmp_keys;

DO $$
DECLARE
	ty      record;
	col     record;
	tbl     text;
	n       bigint;
	lossy   bigint;
	made    int := 0;
	total   bigint := 0;
	skipped text[] := '{}';
	noinput text[] := '{}';
BEGIN
	FOR ty IN
		SELECT DISTINCT a.atttypid AS oid, t.typname
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace s ON s.oid = c.relnamespace
		JOIN pg_type t ON t.oid = a.atttypid
		WHERE c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
		  AND s.nspname NOT IN ('information_schema', 'pgcmp_keys')
		  AND s.nspname NOT LIKE 'pg\_toast%' AND s.nspname NOT LIKE 'pg\_temp%'
		ORDER BY 2, 1
	LOOP
		tbl := format('%s_%s', left(ty.typname, 50), left(md5(format_type(ty.oid, NULL)), 8));
		BEGIN
			EXECUTE format('CREATE TABLE pgcmp_keys.%I (k %s PRIMARY KEY, src text)',
			               tbl, format_type(ty.oid, NULL));
		EXCEPTION WHEN OTHERS THEN
			skipped := skipped || format_type(ty.oid, NULL);
			CONTINUE;
		END;

		FOR col IN
			SELECT s.nspname, c.relname, a.attname
			FROM pg_attribute a
			JOIN pg_class c ON c.oid = a.attrelid
			JOIN pg_namespace s ON s.oid = c.relnamespace
			WHERE a.atttypid = ty.oid AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
			  AND s.nspname NOT IN ('information_schema', 'pgcmp_keys')
			  AND s.nspname NOT LIKE 'pg\_toast%' AND s.nspname NOT LIKE 'pg\_temp%'
			-- A value found in several columns keeps the src of the first one:
			-- the order must not depend on the physical order of pg_attribute,
			-- or the same data gets a different src on another instance.
			ORDER BY s.nspname, c.relname, a.attnum
		LOOP
			BEGIN
				EXECUTE format(
					'INSERT INTO pgcmp_keys.%I '
					'SELECT DISTINCT %I, %L FROM %I.%I '
					'WHERE %I IS NOT NULL AND pg_column_size(%I) < 2000 '
					'ON CONFLICT DO NOTHING',
					tbl, col.attname, col.relname || '.' || col.attname,
					col.nspname, col.relname, col.attname, col.attname);
			EXCEPTION WHEN OTHERS THEN
				NULL;
			END;
		END LOOP;

		EXECUTE format('SELECT count(*) FROM pgcmp_keys.%I', tbl) INTO n;
		IF n = 0 THEN
			EXECUTE format('DROP TABLE pgcmp_keys.%I', tbl);
			CONTINUE;
		END IF;

		-- Block bounds travel as text and are read back by the input
		-- function of the key type. Some internal types refuse any input
		-- (pg_node_tree, pg_mcv_list, ...), and regproc prints overloaded
		-- functions by a name it cannot resolve back: such a key cannot be
		-- bounded, the table is dropped. A lossy round trip is reported but kept:
		-- that is exactly what the comparison must survive or expose.
		BEGIN
			EXECUTE format('SELECT count(*) FROM pgcmp_keys.%I WHERE k::text::%s IS DISTINCT FROM k',
			               tbl, format_type(ty.oid, NULL)) INTO lossy;
		EXCEPTION WHEN OTHERS THEN
			noinput := noinput || format_type(ty.oid, NULL);
			EXECUTE format('DROP TABLE pgcmp_keys.%I', tbl);
			CONTINUE;
		END;
		IF lossy > 0 THEN
			RAISE NOTICE 'pgcmp_keys: % of % values of % do not survive a text round trip',
				lossy, n, format_type(ty.oid, NULL);
		END IF;
		made := made + 1;
		total := total + n;
	END LOOP;

	RAISE NOTICE 'pgcmp_keys: % key tables, % rows', made, total;
	RAISE NOTICE 'pgcmp_keys: % types cannot be a primary key: %',
		cardinality(skipped), array_to_string(skipped, ', ');
	RAISE NOTICE 'pgcmp_keys: % types do not read their own text back: %',
		cardinality(noinput), array_to_string(noinput, ', ');
END
$$;

-- Histograms, for parallel mode.
ANALYZE;

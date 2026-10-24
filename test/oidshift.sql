-- Shift the OID counter for test/regress.sh.
--
-- Run in the "postgres" database of the test instance before
-- `make installcheck` there.
-- Creates one object of every kind that takes an OID, checks that every
-- catalog with an oid column got a new row, then drops everything and
-- checks that nothing is left behind.
--
-- The OID counter is cluster-wide: every database draws relation, type,
-- function and all other OIDs from it, and relfilenodes as well. Two fresh
-- instances start from the same OID, and without this step the suite would
-- allocate nearly the same OIDs on both; with it, everything on the test
-- instance comes after a gap, so OIDs, relfilenodes, the order of reg*
-- values and the parity of enum label OIDs no longer coincide.
--
-- Coverage is checked against the catalogs themselves: a new server
-- version that adds a catalog with an oid column fails here until the
-- script creates an object of that kind too.

\set ON_ERROR_STOP on
SET client_min_messages = warning;

CREATE SCHEMA oidshift;
-- Every object below gets an OID above this one: the counter only grows.
SELECT oid AS base FROM pg_namespace WHERE nspname = 'oidshift' \gset
SELECT set_config('oidshift.base', :'base', false) AS ignored \gset

CREATE SCHEMA oidshift_pub;

-- pg_class, pg_type (row type), pg_attrdef, pg_constraint
CREATE TABLE oidshift.t (a int DEFAULT 1 CHECK (a > 0), b int);
-- pg_rewrite
CREATE VIEW oidshift.v AS SELECT a FROM oidshift.t;
-- pg_statistic_ext
CREATE STATISTICS oidshift.st ON a, b FROM oidshift.t;
-- pg_policy
CREATE POLICY oidshift_pol ON oidshift.t USING (true);
-- pg_type, pg_enum
CREATE TYPE oidshift.e AS ENUM ('x', 'y');
-- pg_proc
CREATE FUNCTION oidshift.f(int) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT $1';
-- pg_trigger
CREATE FUNCTION oidshift.tg() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END';
CREATE TRIGGER oidshift_tg BEFORE INSERT ON oidshift.t FOR EACH ROW EXECUTE FUNCTION oidshift.tg();
-- pg_event_trigger (dropped first below, so it never sees our own DDL for long)
CREATE FUNCTION oidshift.evt() RETURNS event_trigger LANGUAGE plpgsql AS 'BEGIN END';
CREATE EVENT TRIGGER oidshift_evt ON ddl_command_start EXECUTE FUNCTION oidshift.evt();
-- pg_operator
CREATE OPERATOR oidshift.=== (LEFTARG = int4, RIGHTARG = int4, FUNCTION = int4eq);
-- pg_opfamily, pg_opclass, pg_amop, pg_amproc
CREATE OPERATOR CLASS oidshift.oc FOR TYPE int4 USING hash AS
	OPERATOR 1 = , FUNCTION 1 hashint4(int4);
-- pg_am
CREATE ACCESS METHOD oidshift_am TYPE TABLE HANDLER heap_tableam_handler;
-- pg_cast
CREATE CAST (oidshift.e AS int4) WITH INOUT;
-- pg_collation
CREATE COLLATION oidshift.coll (provider = libc, locale = 'C');
-- pg_conversion
CREATE CONVERSION oidshift.conv FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;
-- pg_language
CREATE FUNCTION oidshift.lh() RETURNS language_handler
	AS '$libdir/plpgsql', 'plpgsql_call_handler' LANGUAGE C;
CREATE LANGUAGE oidshift_pl HANDLER oidshift.lh;
-- pg_transform (the function is never called; only its signature is checked)
CREATE FUNCTION oidshift.fromsql(internal) RETURNS internal
	LANGUAGE internal IMMUTABLE STRICT AS 'int4recv';
CREATE TRANSFORM FOR oidshift.e LANGUAGE oidshift_pl (FROM SQL WITH FUNCTION oidshift.fromsql(internal));
-- pg_ts_parser, pg_ts_template, pg_ts_dict, pg_ts_config
CREATE TEXT SEARCH PARSER oidshift.prs
	(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);
CREATE TEXT SEARCH TEMPLATE oidshift.tmpl (INIT = dsimple_init, LEXIZE = dsimple_lexize);
CREATE TEXT SEARCH DICTIONARY oidshift.dict (TEMPLATE = oidshift.tmpl);
CREATE TEXT SEARCH CONFIGURATION oidshift.cfg (COPY = pg_catalog.simple);
-- pg_foreign_data_wrapper, pg_foreign_server, pg_user_mapping
CREATE FOREIGN DATA WRAPPER oidshift_fdw;
CREATE SERVER oidshift_srv FOREIGN DATA WRAPPER oidshift_fdw;
CREATE USER MAPPING FOR CURRENT_USER SERVER oidshift_srv;
-- pg_publication, pg_publication_rel, pg_publication_namespace
CREATE PUBLICATION oidshift_pub FOR TABLE oidshift.t, TABLES IN SCHEMA oidshift_pub;
-- pg_subscription (never connects)
CREATE SUBSCRIPTION oidshift_sub CONNECTION 'dbname=postgres' PUBLICATION oidshift_pub
	WITH (connect = false);
-- pg_extension
CREATE EXTENSION tsm_system_rows SCHEMA oidshift;
-- pg_default_acl
ALTER DEFAULT PRIVILEGES IN SCHEMA oidshift GRANT SELECT ON TABLES TO PUBLIC;
-- pg_largeobject_metadata
SELECT lo_create(0) AS lo \gset
-- pg_authid, pg_auth_members
CREATE ROLE oidshift_r1;
CREATE ROLE oidshift_r2 IN ROLE oidshift_r1;
-- pg_parameter_acl
GRANT SET ON PARAMETER oidshift.param TO oidshift_r1;
-- pg_tablespace (in place: no directory to prepare)
SET allow_in_place_tablespaces = on;
CREATE TABLESPACE oidshift_ts LOCATION '';
-- pg_database
CREATE DATABASE oidshift_db TEMPLATE template0;

-- Every catalog with an oid column must have a row above the base now.
DO $$
DECLARE
	base    oid := current_setting('oidshift.base')::oid;
	cat     text;
	n       bigint;
	missing text[] := '{}';
	covered int := 0;
BEGIN
	FOR cat IN
		SELECT c.relname
		FROM pg_class c
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'oid' AND a.atttypid = 'oid'::regtype
		WHERE c.relnamespace = 'pg_catalog'::regnamespace AND c.relkind = 'r'
		ORDER BY 1
	LOOP
		EXECUTE format('SELECT count(*) FROM pg_catalog.%I WHERE oid >= $1', cat) INTO n USING base;
		IF n = 0 THEN
			missing := missing || cat;
		ELSE
			covered := covered + 1;
		END IF;
	END LOOP;
	IF cardinality(missing) > 0 THEN
		RAISE EXCEPTION 'oidshift: no new object in %', array_to_string(missing, ', ');
	END IF;
	RAISE WARNING 'oidshift: new objects in all % catalogs with an oid column', covered;
END
$$;

DROP EVENT TRIGGER oidshift_evt;
DROP DATABASE oidshift_db;
DROP TABLESPACE oidshift_ts;
ALTER SUBSCRIPTION oidshift_sub SET (slot_name = NONE);
DROP SUBSCRIPTION oidshift_sub;
DROP PUBLICATION oidshift_pub;
REVOKE SET ON PARAMETER oidshift.param FROM oidshift_r1;
DROP ROLE oidshift_r2;
DROP ROLE oidshift_r1;
SELECT lo_unlink(:lo) AS ignored \gset
ALTER DEFAULT PRIVILEGES IN SCHEMA oidshift REVOKE SELECT ON TABLES FROM PUBLIC;
DROP EXTENSION tsm_system_rows;
DROP USER MAPPING FOR CURRENT_USER SERVER oidshift_srv;
DROP SERVER oidshift_srv;
DROP FOREIGN DATA WRAPPER oidshift_fdw;
DROP TRANSFORM FOR oidshift.e LANGUAGE oidshift_pl;
DROP LANGUAGE oidshift_pl;
DROP CAST (oidshift.e AS int4);
DROP OPERATOR FAMILY oidshift.oc USING hash;
DROP ACCESS METHOD oidshift_am;
DROP SCHEMA oidshift CASCADE;
DROP SCHEMA oidshift_pub;

-- Nothing of ours may survive: the databases compared afterwards share the
-- global catalogs, and a stray object would be a difference of its own.
DO $$
DECLARE
	base oid := current_setting('oidshift.base')::oid;
	cat  text;
	n    bigint;
	leftover text[] := '{}';
	probe oid;
BEGIN
	FOR cat IN
		SELECT c.relname
		FROM pg_class c
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'oid' AND a.atttypid = 'oid'::regtype
		WHERE c.relnamespace = 'pg_catalog'::regnamespace AND c.relkind = 'r'
		ORDER BY 1
	LOOP
		EXECUTE format('SELECT count(*) FROM pg_catalog.%I WHERE oid >= $1', cat) INTO n USING base;
		IF n > 0 THEN
			leftover := leftover || format('%s (%s)', cat, n);
		END IF;
	END LOOP;
	IF cardinality(leftover) > 0 THEN
		RAISE EXCEPTION 'oidshift: left behind in %', array_to_string(leftover, ', ');
	END IF;
	-- One more OID, just to see where the counter is now.
	probe := lo_create(0);
	PERFORM lo_unlink(probe);
	RAISE WARNING 'oidshift: everything dropped; the OID counter moved from % to %', base, probe;
END
$$;

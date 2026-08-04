package server

import (
	"database/sql"
	"testing"
)

// TestArrayToStringThreeArg covers the PostgreSQL 3-arg array_to_string(arr, sep,
// nullstr) form. DuckDB's builtin only has the 2-arg arity, so initPgCatalog
// shadows it with a dual-arity macro; both arities must keep working.
func TestArrayToStringThreeArg(t *testing.T) {
	runMacroCases(t, []macroCase{
		// 3-arg: NULL elements rendered as nullstr
		{"array_to_string_3arg_nullstr", `SELECT array_to_string(ARRAY['a', NULL, 'c'], ',', '*')`, "a,*,c", false},
		// 3-arg with nullstr=NULL degrades to 2-arg semantics (NULLs omitted)
		{"array_to_string_3arg_null_nullstr", `SELECT array_to_string(ARRAY['a', NULL, 'c'], ',', NULL)`, "a,c", false},
		// 2-arg must keep the builtin behavior: NULLs omitted, matching PG
		{"array_to_string_2arg_skips_nulls", `SELECT array_to_string(ARRAY['a', NULL, 'c'], ',')`, "a,c", false},
		// non-text arrays work via ::VARCHAR in both arities
		{"array_to_string_2arg_ints", `SELECT array_to_string(ARRAY[1, NULL, 2], ',')`, "1,2", false},
		{"array_to_string_3arg_ints", `SELECT array_to_string(ARRAY[1, NULL, 2], ',', '0')`, "1,0,2", false},
	})
}

// TestPgCatalogStubRelations covers pg_catalog relations that DuckDB lacks
// entirely but real clients/BI tools query. pg_user is derived from pg_roles
// (1 row), pg_timezone_names is backed by DuckDB's native table function, and
// the rest are typed WHERE-false stubs.
func TestPgCatalogStubRelations(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := initPgCatalog(db, processStartTime, processStartTime, "dev", "dev"); err != nil {
		t.Fatalf("initPgCatalog: %v", err)
	}

	// pg_user: one row, the connected user, derived from pg_roles.
	var usename string
	if err := db.QueryRow(`SELECT usename FROM pg_user`).Scan(&usename); err != nil {
		t.Fatalf("pg_user query failed: %v", err)
	}
	if usename != "duckdb" {
		t.Errorf("pg_user.usename = %q, want %q", usename, "duckdb")
	}
	var superuser bool
	if err := db.QueryRow(`SELECT usesuper FROM pg_user WHERE usecreatedb AND NOT userepl AND NOT usebypassrls AND passwd = '********' AND valuntil IS NULL`).Scan(&superuser); err != nil {
		t.Fatalf("pg_user column-shape query failed: %v", err)
	}
	if !superuser {
		t.Error("pg_user.usesuper = false, want true")
	}

	// pg_timezone_names: backed by DuckDB's native table function — real rows.
	var tzCount int
	if err := db.QueryRow(`SELECT count(*) FROM pg_timezone_names WHERE name IS NOT NULL AND abbrev IS NOT NULL AND utc_offset IS NOT NULL AND is_dst IS NOT NULL`).Scan(&tzCount); err != nil {
		t.Fatalf("pg_timezone_names query failed: %v", err)
	}
	if tzCount < 100 {
		t.Errorf("pg_timezone_names rows = %d, want >= 100", tzCount)
	}
	var utc string
	if err := db.QueryRow(`SELECT name FROM pg_timezone_names WHERE name = 'UTC'`).Scan(&utc); err != nil {
		t.Fatalf("pg_timezone_names UTC lookup failed: %v", err)
	}

	// Empty typed stubs: each must exist, return zero rows, and expose its
	// load-bearing PG column names with usable types.
	emptyStubs := []struct {
		relation string
		columns  string
	}{
		{"pg_shadow", "usename, usesysid, usecreatedb, usesuper, userepl, usebypassrls, passwd, valuntil, useconfig"},
		{"pg_authid", "oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, rolvaliduntil"},
		{"pg_cast", "oid, castsource, casttarget, castfunc, castcontext, castmethod"},
		{"pg_operator", "oid, oprname, oprnamespace, oprowner, oprkind, oprcanmerge, oprcanhash, oprleft, oprright, oprresult, oprcom, oprnegate, oprcode, oprrest, oprjoin"},
		{"pg_aggregate", "aggfnoid, aggkind, aggnumdirectargs, aggtransfn, aggfinalfn, aggsortop, aggtranstype, agginitval"},
		{"pg_event_trigger", "oid, evtname, evtevent, evtowner, evtfoid, evtenabled, evttags"},
		{"pg_available_extensions", "name, default_version, installed_version, comment"},
		{"pg_stat_database", "datid, datname, numbackends, xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted, conflicts, temp_files, temp_bytes, deadlocks, stats_reset"},
		{"pg_stat_all_tables", "relid, schemaname, relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd, n_tup_del, n_live_tup, n_dead_tup, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze, vacuum_count, autovacuum_count, analyze_count, autoanalyze_count"},
		{"pg_replication_slots", "slot_name, plugin, slot_type, datoid, database, temporary, active, active_pid, xmin, catalog_xmin, restart_lsn, confirmed_flush_lsn"},
		{"pg_db_role_setting", "setdatabase, setrole, setconfig"},
		{"pg_default_acl", "oid, defaclrole, defaclnamespace, defaclobjtype, defaclacl"},
		{"pg_range", "rngtypid, rngsubtype, rngmultitypid, rngcollation, rngsubopc, rngcanonical, rngsubdiff"},
		{"pg_largeobject", "loid, pageno, data"},
		{"pg_cursors", "name, statement, is_holdable, is_binary, is_scrollable, creation_time"},
	}
	for _, stub := range emptyStubs {
		t.Run(stub.relation, func(t *testing.T) {
			var count int
			if err := db.QueryRow(`SELECT count(*) FROM (SELECT ` + stub.columns + ` FROM ` + stub.relation + `)`).Scan(&count); err != nil {
				t.Fatalf("%s query failed: %v", stub.relation, err)
			}
			if count != 0 {
				t.Errorf("%s rows = %d, want 0", stub.relation, count)
			}
		})
	}
}

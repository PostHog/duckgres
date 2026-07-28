package server

import (
	"testing"
)

func TestQueryAccessPolicyAllowsProjectReads(t *testing.T) {
	policy := &QueryAccessPolicy{
		ReadOnly:         true,
		AllowedSchemas:   []string{"team_42", "team_42_data_imports", "shadow_42_models"},
		AllowedRelations: []string{"posthog.events_prod", "posthog.persons_prod"},
	}

	queries := []string{
		"SELECT * FROM team_42.events",
		"SELECT * FROM ducklake.team_42_data_imports.customers",
		"SELECT * FROM shadow_42_models.revenue",
		"SELECT * FROM posthog.events_prod",
		"WITH recent AS (SELECT * FROM team_42.events) SELECT * FROM recent",
		"SELECT * FROM team_42.events WHERE EXISTS (WITH recent AS (SELECT * FROM team_42.events) SELECT * FROM recent)",
		"SELECT count(*) FROM information_schema.tables",
		"SELECT * FROM pg_index",
		"SHOW search_path",
		"SET statement_timeout = '30s'",
		"SET application_name = 'posthog-sql-editor'",
		"USE ducklake",
		`USE "ducklake";`,
		"BEGIN; SELECT * FROM team_42.events; COMMIT",
	}
	for _, query := range queries {
		if err := policy.Authorize(query); err != nil {
			t.Errorf("Authorize(%q) returned error: %v", query, err)
		}
	}
}

func TestQueryAccessPolicyRejectsCrossProjectAndWrites(t *testing.T) {
	policy := &QueryAccessPolicy{
		ReadOnly:       true,
		AllowedSchemas: []string{"team_42"},
	}

	queries := []string{
		"SELECT * FROM team_7.events",
		"SELECT * FROM events",
		"INSERT INTO team_42.events VALUES (1)",
		"UPDATE team_42.events SET event = 'changed'",
		"DELETE FROM team_42.events",
		"CREATE TABLE team_42.extra (id integer)",
		"DROP TABLE team_42.events",
		"COPY team_42.events TO '/tmp/events.csv'",
		"ATTACH 'other.duckdb' AS other",
		"INSTALL httpfs",
		"LOAD httpfs",
		"SELECT * FROM read_parquet('s3://other-project/data.parquet')",
		"SELECT * FROM glob('/tmp/*')",
		"SELECT query('SELECT * FROM team_7.events')",
		"SELECT current_setting('s3_access_key_id')",
		"SELECT * FROM postgres_scan('host=other', 'public', 'events')",
		"SELECT nextval('shared_sequence')",
		"SELECT * FROM parquet_metadata('/tmp/other.parquet')",
		"SELECT * INTO team_42.copied_events FROM team_42.events",
		"WITH removed AS (DELETE FROM team_42.events RETURNING *) SELECT * FROM removed",
		"SELECT set_config('search_path', 'team_7', false)",
		"SELECT * FROM duckdb_tables()",
		"SELECT * FROM pragma_table_info('team_7.events')",
		"SELECT * FROM postgres_query('host=other', 'SELECT secret FROM private')",
		"SELECT * FROM information_schema.table_constraints",
		"SELECT * FROM pg_catalog.pg_stat_activity",
		"SHOW s3_access_key_id",
		"SHOW ALL",
		"SET search_path = team_7",
		"SET ROLE root",
		"USE memory",
		"USE ducklake; SELECT * FROM team_42.events",
		"DECLARE project_rows CURSOR FOR SELECT * FROM team_42.events",
		"FETCH 10 FROM project_rows",
		"CLOSE project_rows",
		"SELECT * FROM hidden; WITH hidden AS (SELECT * FROM team_42.events) SELECT * FROM hidden",
		"WITH hidden AS (SELECT * FROM team_42.events) SELECT * FROM hidden; SELECT * FROM hidden",
		"SELECT * FROM hidden WHERE EXISTS (WITH hidden AS (SELECT * FROM team_42.events) SELECT 1)",
	}
	for _, query := range queries {
		if err := policy.Authorize(query); err == nil {
			t.Errorf("Authorize(%q) succeeded, want rejection", query)
		}
	}
}

// projectUserPolicy is the read/write counterpart of the reader policy above:
// identical namespaces, ReadOnly cleared.
func projectUserPolicy() *QueryAccessPolicy {
	return &QueryAccessPolicy{
		ReadOnly:         false,
		AllowedSchemas:   []string{"team_42", "team_42_data_imports", "shadow_42_models"},
		AllowedRelations: []string{"posthog.events_prod"},
	}
}

func TestQueryAccessPolicyAllowsProjectWrites(t *testing.T) {
	policy := projectUserPolicy()

	queries := []string{
		// Everything a project reader can do stays available.
		"SELECT * FROM team_42.events",
		"SELECT count(*) FROM information_schema.tables",
		"USE ducklake",
		// DML across the project's own namespaces.
		"INSERT INTO team_42.events VALUES (1)",
		"INSERT INTO team_42.events SELECT * FROM shadow_42_models.revenue",
		"INSERT INTO posthog.events_prod VALUES (1)",
		"UPDATE team_42.events SET event = 'changed'",
		"DELETE FROM team_42.events WHERE id = 1",
		"TRUNCATE team_42.events",
		"MERGE INTO team_42.events t USING team_42.staging s ON t.id = s.id WHEN MATCHED THEN DO NOTHING",
		// A WITH clause on a write statement: `staged` is a legal unqualified
		// reference in the body and must not trip the qualification rule.
		"WITH staged AS (SELECT 1 AS v) INSERT INTO team_42.events SELECT v FROM staged",
		// DDL, as long as the object lands in a project namespace.
		"CREATE TABLE team_42.extra (id integer)",
		"CREATE TABLE IF NOT EXISTS shadow_42_models.revenue (id integer)",
		"CREATE TABLE team_42.copied AS SELECT * FROM team_42.events",
		"SELECT * INTO team_42.copied_events FROM team_42.events",
		"CREATE OR REPLACE VIEW team_42.recent AS SELECT * FROM team_42.events",
		"CREATE INDEX events_id_idx ON team_42.events (id)",
		"CREATE SEQUENCE team_42.event_ids",
		"ALTER TABLE team_42.events ADD COLUMN source text",
		"ALTER TABLE team_42.events RENAME TO events_v2",
		"ALTER TABLE team_42.events RENAME COLUMN event TO event_name",
		"DROP TABLE team_42.extra",
		"DROP TABLE IF EXISTS ducklake.team_42.extra",
		"DROP VIEW team_42.recent",
		"DROP INDEX team_42.events_id_idx",
		// COPY FROM STDIN is how clients bulk-load; the binary and CSV framings
		// both resolve to the same scoped relation.
		"COPY team_42.events FROM STDIN",
		"COPY team_42.events (id, event) FROM STDIN WITH (FORMAT csv, HEADER)",
		"COPY team_42.events FROM STDIN BINARY",
		"BEGIN; INSERT INTO team_42.events VALUES (1); COMMIT",
	}
	for _, query := range queries {
		if err := policy.Authorize(query); err != nil {
			t.Errorf("Authorize(%q) returned error: %v", query, err)
		}
	}
}

// Write authorization must never widen the reachable relation set: a project
// user is exactly a project reader that may also mutate what it can already
// see, and the escape hatches stay shut in both modes.
func TestQueryAccessPolicyRejectsWritesOutsideProject(t *testing.T) {
	policy := projectUserPolicy()

	queries := []string{
		// Cross-project targets, in every write shape.
		"INSERT INTO team_7.events VALUES (1)",
		"UPDATE team_7.events SET event = 'changed'",
		"DELETE FROM team_7.events",
		"TRUNCATE team_7.events",
		"CREATE TABLE team_7.extra (id integer)",
		"CREATE VIEW team_7.recent AS SELECT * FROM team_42.events",
		"CREATE INDEX events_id_idx ON team_7.events (id)",
		"DROP TABLE team_7.events",
		"COPY team_7.events FROM STDIN",
		// A relation the reader policy does not grant is not writable either.
		"INSERT INTO posthog.persons_prod VALUES (1)",
		// Cross-project SOURCES are denied even when the target is in scope.
		"INSERT INTO team_42.events SELECT * FROM team_7.events",
		"CREATE TABLE team_42.copied AS SELECT * FROM team_7.events",
		"UPDATE team_42.events SET event = other.event FROM team_7.events other",
		// A partially-out-of-scope multi-object DROP fails as a whole.
		"DROP TABLE team_42.extra, team_7.events",
		// Unqualified names would resolve through a search path scoped
		// connections do not have.
		"CREATE TABLE extra (id integer)",
		"INSERT INTO events VALUES (1)",
		"DROP TABLE extra",
		// Namespace-level DDL escapes the project boundary entirely.
		"CREATE SCHEMA scratch",
		"DROP SCHEMA team_42",
		"ALTER TABLE team_42.events SET SCHEMA team_7",
		"ALTER SCHEMA team_42 RENAME TO team_7",
		// COPY only ever reads from STDIN: file, URL, PROGRAM and export forms
		// all reach outside the project.
		"COPY team_42.events FROM '/etc/passwd'",
		"COPY team_42.events FROM 's3://other-project/data.parquet'",
		"COPY team_42.events FROM PROGRAM 'curl http://example.com'",
		"COPY team_42.events TO STDOUT",
		"COPY team_42.events TO '/tmp/events.csv'",
		"COPY (SELECT * FROM team_42.events) TO STDOUT",
		// The native-DuckDB escape hatches remain closed for writers.
		"CREATE TABLE team_42.copied AS SELECT * FROM read_parquet('s3://other/data.parquet')",
		"INSERT INTO team_42.events SELECT * FROM read_csv('/tmp/x.csv')",
		"SELECT * FROM duckdb_secrets()",
		"ATTACH 'other.duckdb' AS other",
		"INSTALL httpfs",
		"LOAD httpfs",
		"SET search_path = team_7",
		"GRANT SELECT ON team_42.events TO other",
		"CREATE SECRET s (TYPE s3, KEY_ID 'k', SECRET 'v')",
		"USE memory",
	}
	for _, query := range queries {
		if err := policy.Authorize(query); err == nil {
			t.Errorf("Authorize(%q) succeeded, want rejection", query)
		}
	}
}

// Regression: a WRITE TARGET may never be an unqualified name.
//
// A bare name is legal in a READ position for two reasons — it may be a CTE
// (which provably shadows any base relation of the same name) or one of the
// unqualified pg_catalog compat relations. Neither holds for a write target: an
// INSERT/UPDATE/DELETE/DDL target does NOT bind to a CTE, so it falls through to
// the session search_path (sessionmeta leaves it at `main,memory.main`) and
// would reach a real relation outside the project's granted schemas. Naming a
// CTE after the victim table was a working escape while the target went through
// the read-position check.
func TestQueryAccessPolicyRejectsUnqualifiedWriteTargets(t *testing.T) {
	policy := projectUserPolicy()

	queries := []string{
		// A CTE name must not launder an out-of-project write target.
		"WITH shared AS (SELECT 1) INSERT INTO shared VALUES (1)",
		"WITH shared AS (SELECT 1) INSERT INTO shared SELECT * FROM team_42.events",
		"WITH shared AS (SELECT 1) UPDATE shared SET v = 1",
		"WITH shared AS (SELECT 1) DELETE FROM shared",
		"WITH shared AS (SELECT 1) SELECT * INTO shared FROM team_42.events",
		// Nor may an unqualified pg_catalog compat name, in any write shape.
		"INSERT INTO pg_class VALUES (1)",
		"UPDATE pg_class SET oid = 1",
		"DELETE FROM pg_class",
		"MERGE INTO pg_class t USING team_42.events s ON t.oid = s.id WHEN MATCHED THEN DO NOTHING",
		"TRUNCATE pg_class",
		"CREATE TABLE pg_class (a integer)",
		"CREATE VIEW pg_tables AS SELECT * FROM team_42.events",
		"CREATE INDEX i ON pg_class (oid)",
		"CREATE SEQUENCE pg_class",
		"ALTER TABLE pg_class ADD COLUMN c integer",
		"ALTER TABLE pg_class RENAME TO x",
		"DROP TABLE pg_class",
		"COPY pg_class FROM STDIN",
		// A partially-qualified multi-target TRUNCATE fails as a whole.
		"TRUNCATE team_42.events, pg_class",
		// The catalog's shared default schema is not the project's.
		"INSERT INTO ducklake.main.shared VALUES (1)",
		"INSERT INTO main.shared VALUES (1)",
	}
	for _, query := range queries {
		if err := policy.Authorize(query); err == nil {
			t.Errorf("Authorize(%q) succeeded, want rejection", query)
		}
	}

	// The read-position concessions must survive the fix: a CTE reference in
	// the BODY of a write statement is still a legal unqualified name, and
	// reads of the compat relations still work.
	allowed := []string{
		"WITH staged AS (SELECT 1 AS v) INSERT INTO team_42.events SELECT v FROM staged",
		"WITH recent AS (SELECT * FROM team_42.events) SELECT * FROM recent",
		"SELECT * FROM pg_index",
	}
	for _, query := range allowed {
		if err := policy.Authorize(query); err != nil {
			t.Errorf("Authorize(%q) returned error: %v", query, err)
		}
	}
}

// A scoped user whose team cannot be resolved gets an empty policy. It must
// deny every persistent relation, not fall open — including when the mode said
// read/write before the team went away.
func TestQueryAccessPolicyWithoutNamespacesDeniesEverything(t *testing.T) {
	policy := &QueryAccessPolicy{ReadOnly: false}

	queries := []string{
		"SELECT * FROM team_42.events",
		"INSERT INTO team_42.events VALUES (1)",
		"CREATE TABLE team_42.extra (id integer)",
		"DROP TABLE team_42.events",
		"COPY team_42.events FROM STDIN",
	}
	for _, query := range queries {
		if err := policy.Authorize(query); err == nil {
			t.Errorf("Authorize(%q) succeeded on an empty policy, want rejection", query)
		}
	}
}

func TestQueryAccessPolicyNilIsUnrestricted(t *testing.T) {
	var policy *QueryAccessPolicy
	if err := policy.Authorize("DROP TABLE anything"); err != nil {
		t.Fatalf("nil policy should be unrestricted: %v", err)
	}
}

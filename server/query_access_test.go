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

func TestQueryAccessPolicyNilIsUnrestricted(t *testing.T) {
	var policy *QueryAccessPolicy
	if err := policy.Authorize("DROP TABLE anything"); err != nil {
		t.Fatalf("nil policy should be unrestricted: %v", err)
	}
}

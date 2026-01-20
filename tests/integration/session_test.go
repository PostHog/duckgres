package integration

import (
	"testing"
)

// TestSessionSetCommands tests SET command handling
func TestSessionSetCommands(t *testing.T) {
	// These SET commands should be handled (either executed or safely ignored)
	tests := []QueryTest{
		// Working SET commands
		{
			Name:         "set_search_path",
			Query:        "SET search_path TO public",
			DuckgresOnly: true,
		},
		{
			Name:         "set_search_path_multiple",
			Query:        "SET search_path TO public, myschema",
			DuckgresOnly: true,
		},
		{
			Name:         "set_client_encoding",
			Query:        "SET client_encoding = 'UTF8'",
			DuckgresOnly: true,
		},
		{
			Name:         "set_timezone",
			Query:        "SET timezone = 'UTC'",
			DuckgresOnly: true,
		},
		{
			Name:         "set_datestyle",
			Query:        "SET datestyle = 'ISO, MDY'",
			DuckgresOnly: true,
		},

		// Ignored SET commands (should not error)
		{
			Name:         "set_statement_timeout",
			Query:        "SET statement_timeout = '30s'",
			DuckgresOnly: true,
		},
		{
			Name:         "set_lock_timeout",
			Query:        "SET lock_timeout = '10s'",
			DuckgresOnly: true,
		},
		{
			Name:         "set_work_mem",
			Query:        "SET work_mem = '256MB'",
			DuckgresOnly: true,
		},
		{
			Name:         "set_effective_cache_size",
			Query:        "SET effective_cache_size = '4GB'",
			DuckgresOnly: true,
		},
		{
			Name:         "set_random_page_cost",
			Query:        "SET random_page_cost = 1.1",
			DuckgresOnly: true,
		},
		{
			Name:         "set_enable_seqscan",
			Query:        "SET enable_seqscan = on",
			DuckgresOnly: true,
		},
		{
			Name:         "set_enable_hashjoin",
			Query:        "SET enable_hashjoin = on",
			DuckgresOnly: true,
		},
		{
			Name:         "set_log_statement",
			Query:        "SET log_statement = 'all'",
			DuckgresOnly: true,
		},
		{
			Name:         "set_client_min_messages",
			Query:        "SET client_min_messages = 'warning'",
			DuckgresOnly: true,
		},
		{
			Name:         "set_synchronous_commit",
			Query:        "SET synchronous_commit = 'off'",
			DuckgresOnly: true,
		},
		{
			Name:         "set_application_name",
			Query:        "SET application_name = 'test_app'",
			DuckgresOnly: true,
		},
		{
			Name:         "set_standard_conforming_strings",
			Query:        "SET standard_conforming_strings = on",
			DuckgresOnly: true,
		},
		{
			Name:         "set_extra_float_digits",
			Query:        "SET extra_float_digits = 3",
			DuckgresOnly: true,
		},
		{
			Name:         "set_default_transaction_isolation",
			Query:        "SET default_transaction_isolation = 'read committed'",
			DuckgresOnly: true,
		},

		// Test SET SESSION CHARACTERISTICS commands
		{
			Name:         "set_session_characteristics_isolation_level",
			Query:        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
			DuckgresOnly: true,
		},
		{
			Name:         "set_session_characteristics_read_only",
			Query:        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
			DuckgresOnly: true,
		},
		{
			Name:         "set_session_characteristics_read_committed",
			Query:        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED",
			DuckgresOnly: true,
		},
		{
			Name:         "set_session_characteristics_repeatable_read",
			Query:        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ",
			DuckgresOnly: true,
		},
		{
			Name:         "set_session_characteristics_serializable",
			Query:        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestSessionShowCommands tests SHOW command handling
func TestSessionShowCommands(t *testing.T) {
	tests := []QueryTest{
		{
			Name:         "show_search_path",
			Query:        "SHOW search_path",
			DuckgresOnly: true,
		},
		{
			Name:         "show_client_encoding",
			Query:        "SHOW client_encoding",
			DuckgresOnly: true,
		},
		{
			Name:         "show_timezone",
			Query:        "SHOW timezone",
			DuckgresOnly: true,
		},
		{
			Name:         "show_datestyle",
			Query:        "SHOW datestyle",
			DuckgresOnly: true,
		},
		{
			Name:         "show_server_version",
			Query:        "SHOW server_version",
			DuckgresOnly: true,
		},
		{
			Name:         "show_server_encoding",
			Query:        "SHOW server_encoding",
			DuckgresOnly: true,
		},
		{
			Name:         "show_standard_conforming_strings",
			Query:        "SHOW standard_conforming_strings",
			DuckgresOnly: true,
		},
		{
			Name:         "show_transaction_isolation",
			Query:        "SHOW transaction_isolation",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestSessionTransactionCommands tests transaction control commands
func TestSessionTransactionCommands(t *testing.T) {
	tests := []struct {
		name    string
		queries []string
	}{
		{
			name:    "begin_commit",
			queries: []string{"BEGIN", "SELECT 1", "COMMIT"},
		},
		{
			name:    "begin_rollback",
			queries: []string{"BEGIN", "SELECT 1", "ROLLBACK"},
		},
		{
			name:    "begin_transaction",
			queries: []string{"BEGIN TRANSACTION", "SELECT 1", "COMMIT"},
		},
		{
			name:    "start_transaction",
			queries: []string{"START TRANSACTION", "SELECT 1", "COMMIT"},
		},
		{
			name:    "end_as_commit",
			queries: []string{"BEGIN", "SELECT 1", "END"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, q := range tt.queries {
				_, err := testHarness.DuckgresDB.Exec(q)
				if err != nil {
					t.Errorf("Query %q failed: %v", q, err)
				}
			}
		})
	}
}

// TestSessionTransactionModes tests transaction mode commands
func TestSessionTransactionModes(t *testing.T) {
	// These should not error, but may be effectively no-ops
	tests := []QueryTest{
		{
			Name:         "begin_read_committed",
			Query:        "BEGIN ISOLATION LEVEL READ COMMITTED",
			DuckgresOnly: true,
		},
		{
			Name:         "begin_repeatable_read",
			Query:        "BEGIN ISOLATION LEVEL REPEATABLE READ",
			DuckgresOnly: true,
		},
		{
			Name:         "begin_serializable",
			Query:        "BEGIN ISOLATION LEVEL SERIALIZABLE",
			DuckgresOnly: true,
		},
		{
			Name:         "begin_read_only",
			Query:        "BEGIN READ ONLY",
			DuckgresOnly: true,
		},
		{
			Name:         "begin_read_write",
			Query:        "BEGIN READ WRITE",
			DuckgresOnly: true,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			_, err := testHarness.DuckgresDB.Exec(test.Query)
			if err != nil {
				t.Errorf("Query failed: %v", err)
			}
			// Always rollback to clean up
			_, _ = testHarness.DuckgresDB.Exec("ROLLBACK")
		})
	}
}

// TestSessionReset tests RESET command
func TestSessionReset(t *testing.T) {
	tests := []QueryTest{
		{
			Name:         "reset_search_path",
			Query:        "RESET search_path",
			DuckgresOnly: true,
		},
		{
			Name:         "reset_all",
			Query:        "RESET ALL",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestSessionCurrentFunctions tests current_* functions
func TestSessionCurrentFunctions(t *testing.T) {
	tests := []QueryTest{
		{
			Name:         "current_database",
			Query:        "SELECT current_database() IS NOT NULL",
			DuckgresOnly: true,
		},
		{
			Name:         "current_schema",
			Query:        "SELECT current_schema() IS NOT NULL",
			DuckgresOnly: true,
		},
		{
			Name:         "current_user",
			Query:        "SELECT current_user IS NOT NULL",
			DuckgresOnly: true,
		},
		{
			Name:         "session_user",
			Query:        "SELECT session_user IS NOT NULL",
			DuckgresOnly: true,
		},
		{
			Name:         "current_setting",
			Query:        "SELECT current_setting('server_version') IS NOT NULL",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestSessionMiscCommands tests miscellaneous session commands
func TestSessionMiscCommands(t *testing.T) {
	tests := []QueryTest{
		// DISCARD should be safely handled
		{
			Name:         "discard_all",
			Query:        "DISCARD ALL",
			DuckgresOnly: true,
		},
		{
			Name:         "discard_plans",
			Query:        "DISCARD PLANS",
			DuckgresOnly: true,
		},
		{
			Name:         "discard_sequences",
			Query:        "DISCARD SEQUENCES",
			DuckgresOnly: true,
		},
		{
			Name:         "discard_temp",
			Query:        "DISCARD TEMP",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

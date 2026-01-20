package integration

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
)

var (
	// Global harness for all tests
	testHarness *TestHarness
	// Skip PostgreSQL comparison tests
	skipPostgresCompare bool
)

// knownFailures maps test names (or subtest names) to skip reasons.
// Tests matching these patterns will be skipped with the given reason.
// Use the subtest name only (e.g., "insert_returning_star") for subtests,
// or full test name (e.g., "TestFoo") for entire test functions.
var knownFailures = map[string]string{
	// DuckLake limitations - RETURNING clause not supported
	"insert_returning_star":    "DuckLake: RETURNING clause not yet supported",
	"insert_returning_columns": "DuckLake: RETURNING clause not yet supported",
	"update_returning_star":    "DuckLake: RETURNING clause not yet supported",
	"delete_returning_star":    "DuckLake: RETURNING clause not yet supported",
	"on_conflict_do_nothing":   "DuckLake: constraint detection requires explicit columns",

	// DuckDB missing functions
	"where_regex_match":                 "DuckDB: ~* operator not available",
	"where_regex_match_case_insensitive": "DuckDB: ~* operator not available",
	"where_regex_not_match":             "DuckDB: !~* operator not available",
	"octet_length":                      "DuckDB: octet_length needs explicit cast",
	"initcap":                           "DuckDB: initcap function not available",
	"lpad_space":                        "DuckDB: lpad 2-arg form not available",
	"rpad_space":                        "DuckDB: rpad 2-arg form not available",
	"overlay":                           "DuckDB: overlay function difference",
	"format_s":                          "DuckDB: format function not compatible",
	"format_I":                          "DuckDB: format function not compatible",
	"quote_literal":                     "DuckDB: quote_literal not available",
	"quote_ident":                       "DuckDB: quote_ident not available",
	"quote_nullable_null":               "DuckDB: quote_nullable not available",
	"quote_nullable_value":              "DuckDB: quote_nullable not available",
	"width_bucket":                      "DuckDB: width_bucket not available",
	"log_base":                          "DuckDB: log(base,val) not compatible",
	"regexp_matches":                    "DuckDB: regexp_matches returns different format",
	"string_to_array":                   "DuckDB: array format differs from PostgreSQL",

	// Type/precision differences
	"select_divide_integer": "DuckDB: integer division returns float",
	"exp":                   "DuckDB: floating point precision differs",
	"ln":                    "DuckDB: floating point precision differs",
	"pi":                    "DuckDB: floating point precision differs",
	"radians":               "DuckDB: floating point precision differs",
	"atan2":                 "DuckDB: floating point precision differs",

	// Extract function differences
	"extract_year":        "DuckDB: extract function syntax differs",
	"extract_month":       "DuckDB: extract function syntax differs",
	"extract_day":         "DuckDB: extract function syntax differs",
	"extract_hour":        "DuckDB: extract function syntax differs",
	"extract_minute":      "DuckDB: extract function syntax differs",
	"extract_second":      "DuckDB: extract function syntax differs",
	"extract_dow":         "DuckDB: extract function syntax differs",
	"extract_doy":         "DuckDB: extract function syntax differs",
	"extract_week":        "DuckDB: extract function syntax differs",
	"extract_quarter":     "DuckDB: extract function syntax differs",
	"extract_epoch":       "DuckDB: extract function syntax differs",
	"group_by_extract":    "DuckDB: extract in GROUP BY not compatible",

	// Information schema differences
	"info_schema_tables_all":       "DuckDB: information_schema row count differs",
	"info_schema_tables_public":    "DuckDB: information_schema row count differs",
	"info_schema_tables_base":      "DuckDB: information_schema row count differs",
	"info_schema_columns_users":    "DuckDB: is_nullable detection differs",
	"info_schema_columns_with_default": "DuckDB: column_default not populated",
	"info_schema_schemata_all":     "DuckDB: schema count differs",
	"psql_dt":                      "DuckDB: pg_class row count differs",

	// Query ordering differences (results correct but order differs)
	"distinct_multiple":   "DuckDB: default ordering differs",
	"group_by_multiple":   "DuckDB: GROUP BY expression handling differs",
	"group_by_expression": "DuckDB: GROUP BY expression handling differs",
	"grouping_sets":       "DuckDB: GROUPING SETS row count differs",
	"inner_join_on":       "DuckDB: join ordering differs",
	"inner_join_explicit": "DuckDB: join ordering differs",
	"cross_join":          "DuckDB: cross join ordering differs",
	"cross_join_implicit": "DuckDB: cross join ordering differs",
	"self_join":           "DuckDB: self join ordering differs",
	"in_subquery":         "DuckDB: subquery result ordering differs",
	"analytics_query":     "DuckDB: complex query ordering differs",

	// JSON syntax
	"create_table_types": "DuckDB: JSON type syntax differs",

	// Array formatting
	"array_select":       "DuckDB: array format differs from PostgreSQL",
	"array_access":       "DuckDB: array access syntax differs",
	"array_slice":        "DuckDB: array slice syntax differs",
	"array_concat":       "DuckDB: array concat result format differs",
	"array_append":       "DuckDB: array append result format differs",
	"array_prepend":      "DuckDB: array prepend result format differs",
	"array_length":       "DuckDB: array_length behavior differs",
	"array_dims":         "DuckDB: array_dims not available",
	"array_upper":        "DuckDB: array_upper behavior differs",
	"array_lower":        "DuckDB: array_lower behavior differs",
	"unnest":             "DuckDB: unnest behavior differs",
	"array_agg":          "DuckDB: array_agg format differs",
	"array_agg_order":    "DuckDB: array_agg ordering differs",

	// Protocol/type handling
	"extended_query_params":     "DuckDB: parameter binding differs",
	"protocol_binary_int":       "DuckDB: binary protocol differs",
	"numeric_decimal":           "DuckDB: numeric precision differs",
	"numeric_numeric":           "DuckDB: numeric handling differs",
	"timestamp_tz":              "DuckDB: timestamptz handling differs",
	"timestamp_ntz":             "DuckDB: timestamp handling differs",
	"date_type":                 "DuckDB: date handling differs",
	"time_type":                 "DuckDB: time handling differs",
	"interval_type":             "DuckDB: interval handling differs",
	"binary_bytea":              "DuckDB: bytea handling differs",
	"binary_hex":                "DuckDB: hex encoding differs",
	"uuid_type":                 "DuckDB: uuid handling differs",
	"uuid_generate":             "DuckDB: uuid generation differs",
	"json_type":                 "DuckDB: json type handling differs",
	"json_access":               "DuckDB: json access syntax differs",
	"jsonb_type":                "DuckDB: jsonb handling differs",
	"json_build_object":         "DuckDB: json_build_object differs",
	"json_agg":                  "DuckDB: json_agg format differs",

	// Additional DateTime function differences
	"age_two_args":    "DuckDB: age function not compatible",
	"to_char_date":    "DuckDB: to_char not available",
	"to_char_day_name": "DuckDB: to_char not available",
	"to_char_number":  "DuckDB: to_char not available",
	"to_date":         "DuckDB: to_date not compatible",
	"to_timestamp":    "DuckDB: to_timestamp not compatible",
	"make_time":       "DuckDB: make_time not available",

	// Additional aggregate function differences
	"array_agg_distinct": "DuckDB: array_agg DISTINCT differs",
	"json_object_agg":    "DuckDB: json_object_agg not available",

	// Additional JSON function differences
	"json_build_array":        "DuckDB: json_build_array not available",
	"to_json":                 "DuckDB: to_json not available",
	"to_jsonb":                "DuckDB: to_jsonb not available",
	"array_to_json":           "DuckDB: array_to_json not available",
	"json_extract_path":       "DuckDB: json_extract_path differs",
	"json_extract_path_text":  "DuckDB: json_extract_path_text differs",
	"json_typeof_object":      "DuckDB: json_typeof differs",
	"json_typeof_array":       "DuckDB: json_typeof differs",
	"json_typeof_string":      "DuckDB: json_typeof differs",
	"json_typeof_number":      "DuckDB: json_typeof differs",
	"json_array_length":       "DuckDB: json_array_length differs",
	"jsonb_set":               "DuckDB: jsonb_set not available",
	"jsonb_concat":            "DuckDB: jsonb concat differs",
	"jsonb_delete_key":        "DuckDB: jsonb delete differs",
	"json_each":               "DuckDB: json_each not available",
	"jsonb_each":              "DuckDB: jsonb_each not available",
	"json_each_text":          "DuckDB: json_each_text not available",
	"json_array_elements":     "DuckDB: json_array_elements differs",
	"jsonb_array_elements":    "DuckDB: jsonb_array_elements differs",
	"json_array_elements_text": "DuckDB: json_array_elements_text differs",
	"json_arrow":              "DuckDB: -> operator differs",
	"json_double_arrow":       "DuckDB: ->> operator differs",
	"json_nested":             "DuckDB: nested JSON access differs",
	"json_null":               "DuckDB: JSON null handling differs",
	"json_number":             "DuckDB: JSON number handling differs",
	"json_object":             "DuckDB: JSON object handling differs",
	"json_string":             "DuckDB: JSON string handling differs",
	"json_array":              "DuckDB: JSON array handling differs",
	"json_array_access":       "DuckDB: JSON array access differs",
	"jsonb_array":             "DuckDB: JSONB array handling differs",
	"jsonb_object":            "DuckDB: JSONB object handling differs",
	"jsonb_contains":          "DuckDB: jsonb @> operator differs",
	"jsonb_contained":         "DuckDB: jsonb <@ operator differs",
	"jsonb_exists":            "DuckDB: jsonb ? operator differs",

	// Additional array function differences
	"array_ndims":    "DuckDB: array_ndims not available",
	"cardinality":    "DuckDB: cardinality differs",
	"array_cat":      "DuckDB: array_cat differs",
	"array_remove":   "DuckDB: array_remove differs",
	"array_replace":  "DuckDB: array_replace differs",
	"nested_array":   "DuckDB: nested array handling differs",
	"int_array":      "DuckDB: int array format differs",
	"text_array":     "DuckDB: text array format differs",
	"array_cast":     "DuckDB: array casting differs",
	"select_array_column":   "DuckDB: array column format differs",
	"select_text_array":     "DuckDB: text array format differs",

	// Information schema views
	"info_schema_views_all": "DuckDB: views metadata differs",

	// Binary/bytea handling
	"bytea_hex":       "DuckDB: bytea hex format differs",
	"bytes":           "DuckDB: bytes handling differs",
	"decode_base64":   "DuckDB: decode differs",
	"decode_hex":      "DuckDB: decode differs",
	"encode_base64":   "DuckDB: encode differs",
	"encode_hex":      "DuckDB: encode differs",
	"select_bytea_column": "DuckDB: bytea column format differs",

	// UUID handling
	"uuid_literal":         "DuckDB: UUID literal handling differs",
	"select_uuid_column":   "DuckDB: UUID column format differs",

	// Time/interval handling
	"time_cast":             "DuckDB: time cast differs",
	"time_literal":          "DuckDB: time literal differs",
	"time_with_microseconds": "DuckDB: time microseconds differ",
	"interval_cast":         "DuckDB: interval cast differs",
	"interval_complex":      "DuckDB: complex interval differs",
	"interval_day":          "DuckDB: day interval differs",
	"interval_hour_minute":  "DuckDB: hour/minute interval differs",
	"select_time_column":    "DuckDB: time column format differs",
	"select_interval_column": "DuckDB: interval column format differs",

	// Numeric handling
	"integer_divide": "DuckDB: integer division differs",

	// Protocol tests
	"prepare_with_types": "DuckDB: prepared statement types differ",
	"pg_typeof":          "DuckDB: pg_typeof differs",
	"pg_typeof_text":     "DuckDB: pg_typeof differs",
}

// skipIfKnown checks if the current test is in the known failures list
// and skips it with the documented reason. Call at the start of subtests.
func skipIfKnown(t *testing.T) {
	t.Helper()
	name := t.Name()

	// Check full test name first
	if reason, ok := knownFailures[name]; ok {
		t.Skip(reason)
		return
	}

	// Extract subtest name (after last /) and check that
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '/' {
			subtest := name[i+1:]
			if reason, ok := knownFailures[subtest]; ok {
				t.Skip(reason)
				return
			}
			break
		}
	}
}

// TestMain sets up and tears down the test environment
func TestMain(m *testing.M) {
	cfg := DefaultConfig()

	// Check if PostgreSQL (for comparison) is running
	pgPort := cfg.PostgresPort
	if !IsPostgresRunning(pgPort) {
		fmt.Println("PostgreSQL container not running. Starting it...")
		if err := StartPostgresContainer(); err != nil {
			fmt.Printf("Failed to start PostgreSQL: %v\n", err)
			fmt.Println("Running tests without PostgreSQL comparison (Duckgres-only mode)")
			skipPostgresCompare = true
		}
	}

	// Check and wait for DuckLake infrastructure if DuckLake mode is enabled
	if cfg.UseDuckLake {
		if !IsDuckLakeInfraRunning(cfg.DuckLakeMetadataPort, cfg.MinIOPort) {
			fmt.Println("DuckLake infrastructure not running. Waiting for it...")
			if err := WaitForDuckLakeInfra(cfg.DuckLakeMetadataPort, cfg.MinIOPort, 30*time.Second); err != nil {
				fmt.Printf("DuckLake infrastructure not available: %v\n", err)
				fmt.Println("Falling back to vanilla DuckDB mode (set DUCKGRES_TEST_NO_DUCKLAKE=1 to suppress this)")
				cfg.UseDuckLake = false
			} else {
				fmt.Println("DuckLake infrastructure is ready")
			}
		}
	}

	cfg.SkipPostgres = skipPostgresCompare

	var err error
	testHarness, err = NewTestHarness(cfg)
	if err != nil {
		fmt.Printf("Failed to create test harness: %v\n", err)
		os.Exit(1)
	}

	// Run tests
	code := m.Run()

	// Cleanup
	if testHarness != nil {
		testHarness.Close()
	}

	os.Exit(code)
}

// runQueryTest runs a single query test
func runQueryTest(t *testing.T, test QueryTest) {
	t.Helper()

	// Check centralized skip list first
	skipIfKnown(t)

	if test.Skip != "" {
		t.Skipf("Skipping: %s", test.Skip)
		return
	}

	if test.DuckgresOnly || skipPostgresCompare {
		// Only test against Duckgres
		result, err := ExecuteQuery(testHarness.DuckgresDB, test.Query)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		if test.ExpectError {
			if result.Error == nil {
				t.Errorf("Expected error but query succeeded")
			}
		} else {
			if result.Error != nil {
				t.Errorf("Query failed: %v", result.Error)
			}
		}
		return
	}

	// Compare PostgreSQL and Duckgres
	opts := test.Options
	if opts.FloatTolerance == 0 {
		opts = DefaultCompareOptions()
	}

	result := CompareQueries(testHarness.PostgresDB, testHarness.DuckgresDB, test.Query, opts)

	if test.ExpectError {
		if result.PGError == nil && result.DGError == nil {
			t.Errorf("Expected error but both queries succeeded")
		}
		return
	}

	if !result.Match {
		t.Errorf("Query results do not match:\n  PostgreSQL rows: %d\n  Duckgres rows: %d\n  Differences:\n    %s",
			result.PGRowCount, result.DGRowCount, formatDifferences(result.Differences))
	}
}

// runQueryTests runs multiple query tests
func runQueryTests(t *testing.T, tests []QueryTest) {
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			runQueryTest(t, test)
		})
	}
}

// formatDifferences formats a list of differences for display
func formatDifferences(diffs []string) string {
	if len(diffs) == 0 {
		return "(none)"
	}
	result := ""
	for i, diff := range diffs {
		if i > 0 {
			result += "\n    "
		}
		result += diff
		if i >= 5 {
			result += fmt.Sprintf("\n    ... and %d more differences", len(diffs)-5)
			break
		}
	}
	return result
}

// mustExec executes a statement and fails the test on error
func mustExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("Failed to execute %q: %v", truncateQuery(query), err)
	}
}

// mustExecBoth executes a statement on both databases
func mustExecBoth(t *testing.T, query string) {
	t.Helper()
	if testHarness.PostgresDB != nil && !skipPostgresCompare {
		mustExec(t, testHarness.PostgresDB, query)
	}
	mustExec(t, testHarness.DuckgresDB, query)
}

// truncateQuery truncates a query for display
func truncateQuery(q string) string {
	if len(q) > 80 {
		return q[:80] + "..."
	}
	return q
}

// pgOnly returns the PostgreSQL database, or skips if not available
func pgOnly(t *testing.T) *sql.DB {
	t.Helper()
	if testHarness.PostgresDB == nil || skipPostgresCompare {
		t.Skip("PostgreSQL not available")
	}
	return testHarness.PostgresDB
}

// dgOnly returns the Duckgres database
func dgOnly(t *testing.T) *sql.DB {
	t.Helper()
	return testHarness.DuckgresDB
}

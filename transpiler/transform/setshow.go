package transform

import (
	"fmt"
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// configParamPattern matches PostgreSQL configuration parameter names
// (lowercase letters, digits, and underscores, starting with a letter)
var configParamPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// SetShowTransform handles SET and SHOW commands:
// - SET application_name = 'x' -> SET VARIABLE application_name = 'x'
// - SHOW application_name -> SELECT getvariable('application_name') AS application_name
// - PostgreSQL-specific SET parameters -> IsIgnoredSet = true
type SetShowTransform struct {
	// IgnoredParams are PostgreSQL-specific parameters that should be silently ignored
	IgnoredParams map[string]bool

	// VariableParams are parameters that need SET VARIABLE syntax in DuckDB
	VariableParams map[string]bool
}

// NewSetShowTransform creates a new SetShowTransform with default ignored parameters.
func NewSetShowTransform() *SetShowTransform {
	return &SetShowTransform{
		IgnoredParams: map[string]bool{
			// SSL/Connection settings
			"ssl_renegotiation_limit": true,

			// Statement/Lock timeouts
			"statement_timeout":                   true,
			"lock_timeout":                        true,
			"idle_in_transaction_session_timeout": true,
			"idle_session_timeout":                true,

			// Client connection settings
			"application_name":           true, // Used by clients for identification, not critical
			"client_min_messages":        true,
			"log_min_messages":           true,
			"log_min_duration_statement": true,
			"log_statement":              true,
			"extra_float_digits":         true,

			// Transaction settings
			"default_transaction_isolation":  true,
			"default_transaction_read_only":  true,
			"default_transaction_deferrable": true,
			"transaction_isolation":          true,
			"transaction_read_only":          true,
			"transaction_deferrable":         true,

			// Encoding (DuckDB is always UTF-8)
			"client_encoding": true,

			// PostgreSQL-specific features
			"row_security":             true,
			"check_function_bodies":    true,
			"default_tablespace":       true,
			"temp_tablespaces":         true,
			"session_replication_role": true,
			"vacuum_freeze_min_age":    true,
			"vacuum_freeze_table_age":  true,
			"bytea_output":             true,
			"xmlbinary":                true,
			"xmloption":                true,
			"gin_pending_list_limit":   true,
			"gin_fuzzy_search_limit":   true,

			// Locale settings
			"lc_messages": true,
			"lc_monetary": true,
			"lc_numeric":  true,
			"lc_time":     true,

			// Constraint settings
			"constraint_exclusion":  true,
			"cursor_tuple_fraction": true,
			"from_collapse_limit":   true,
			"join_collapse_limit":   true,
			"geqo":                  true,
			"geqo_threshold":        true,

			// Parallel query settings
			"max_parallel_workers_per_gather": true,
			"max_parallel_workers":            true,
			"parallel_leader_participation":   true,
			"parallel_tuple_cost":             true,
			"parallel_setup_cost":             true,
			"min_parallel_table_scan_size":    true,
			"min_parallel_index_scan_size":    true,

			// Planner settings
			"enable_bitmapscan":              true,
			"enable_hashagg":                 true,
			"enable_hashjoin":                true,
			"enable_indexscan":               true,
			"enable_indexonlyscan":           true,
			"enable_material":                true,
			"enable_mergejoin":               true,
			"enable_nestloop":                true,
			"enable_parallel_append":         true,
			"enable_parallel_hash":           true,
			"enable_partition_pruning":       true,
			"enable_partitionwise_join":      true,
			"enable_partitionwise_aggregate": true,
			"enable_seqscan":                 true,
			"enable_sort":                    true,
			"enable_tidscan":                 true,
			"enable_gathermerge":             true,

			// Cost settings
			"seq_page_cost":        true,
			"random_page_cost":     true,
			"cpu_tuple_cost":       true,
			"cpu_index_tuple_cost": true,
			"cpu_operator_cost":    true,
			"effective_cache_size": true,

			// Memory settings
			"work_mem":                  true,
			"maintenance_work_mem":      true,
			"logical_decoding_work_mem": true,
			"temp_buffers":              true,

			// Misc settings
			"synchronous_commit":      true,
			"commit_delay":            true,
			"commit_siblings":         true,
			"huge_pages":              true,
			"force_parallel_mode":     true,
			"jit":                     true,
			"jit_above_cost":          true,
			"jit_inline_above_cost":   true,
			"jit_optimize_above_cost": true,

			// Replication settings
			"synchronous_standby_names": true,
			"wal_sender_timeout":        true,
			"wal_receiver_timeout":      true,

			// Search path and session settings (silently accept)
			"search_path":                  true,
			"datestyle":                    true,
			"intervalstyle":                true,
			"standard_conforming_strings":  true,
			"escape_string_warning":        true,
			"array_nulls":                  true,
			"backslash_quote":              true,
			"default_with_oids":            true,
			"quote_all_identifiers":        true,
			"sql_inheritance":              true,
			"transform_null_equals":        true,
			"lo_compat_privileges":         true,
			"operator_precedence_warning":  true,

			// Server version settings (commonly queried)
			"server_version":     true,
			"server_version_num": true,
			"server_encoding":    true,

			// Timezone (DuckDB has its own timezone setting)
			"timezone":         true,
			"log_timezone":     true,
			"timezone_abbreviations": true,
		},
		VariableParams: map[string]bool{
			// Parameters that need SET VARIABLE syntax in DuckDB
			// (currently none - application_name is ignored instead)
		},
	}
}

func (t *SetShowTransform) Name() string {
	return "setshow"
}

func (t *SetShowTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false

	for i, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}

		switch n := stmt.Stmt.Node.(type) {
		case *pg_query.Node_VariableSetStmt:
			if n.VariableSetStmt != nil {
				// Handle RESET ALL (kind = VAR_RESET_ALL = 4)
				if n.VariableSetStmt.Kind == pg_query.VariableSetKind_VAR_RESET_ALL {
					result.IsIgnoredSet = true
					return true, nil
				}

				// Handle transaction-related SET commands (VAR_SET_MULTI kind)
				// This covers multiple PostgreSQL transaction commands:
				//   - SET SESSION CHARACTERISTICS AS TRANSACTION ... (Name: "SESSION CHARACTERISTICS")
				//   - SET TRANSACTION ISOLATION LEVEL ... (Name: "TRANSACTION")
				//   - SET TRANSACTION READ ONLY/WRITE (Name: "TRANSACTION")
				//   - SET TRANSACTION SNAPSHOT ... (Name: "TRANSACTION SNAPSHOT")
				// All are ignored since DuckDB/DuckLake use fixed isolation levels:
				//   - DuckDB: SERIALIZABLE
				//   - DuckLake: snapshot isolation (equivalent to REPEATABLE READ)
				if n.VariableSetStmt.Kind == pg_query.VariableSetKind_VAR_SET_MULTI {
					result.IsIgnoredSet = true
					return true, nil
				}

				paramName := strings.ToLower(n.VariableSetStmt.Name)

				// Check if this is an ignored parameter (including RESET single param)
				if t.IgnoredParams[paramName] {
					result.IsIgnoredSet = true
					return true, nil
				}

				// Check if this needs SET VARIABLE syntax
				// DuckDB uses SET VARIABLE for user-defined variables
				// We'll handle application_name specially
				if t.VariableParams[paramName] {
					// The pg_query deparser will output this correctly,
					// but DuckDB needs SET VARIABLE syntax.
					// Since we can't change the AST node type, we'll need to
					// handle this at a different level or use a workaround.
					// For now, let's set a flag and handle in the transpiler.
					changed = true
				}
			}

		case *pg_query.Node_DiscardStmt:
			// DISCARD ALL/PLANS/SEQUENCES/TEMP - silently ignore
			result.IsIgnoredSet = true
			return true, nil

		case *pg_query.Node_TransactionStmt:
			// Handle BEGIN/START TRANSACTION with isolation level
			// DuckDB doesn't support ISOLATION LEVEL syntax, so strip it
			if n.TransactionStmt != nil {
				// Check if this has options (like ISOLATION LEVEL)
				if len(n.TransactionStmt.Options) > 0 {
					// Clear the options to convert to simple BEGIN
					n.TransactionStmt.Options = nil
					changed = true
				}
			}

		case *pg_query.Node_VariableShowStmt:
			if n.VariableShowStmt != nil {
				paramName := strings.ToLower(n.VariableShowStmt.Name)

				// For ignored params, return a sensible default value
				if t.IgnoredParams[paramName] {
					defaultVal := t.getDefaultValue(paramName)
					selectStmt := t.createDefaultValueSelect(paramName, defaultVal)
					tree.Stmts[i].Stmt = &pg_query.Node{
						Node: &pg_query.Node_SelectStmt{SelectStmt: selectStmt},
					}
					changed = true
					continue
				}

				// Convert SHOW to SELECT getvariable() for variable params
				if t.VariableParams[paramName] {
					// Replace with a SELECT statement
					selectStmt := t.createGetVariableSelect(paramName)
					tree.Stmts[i].Stmt = &pg_query.Node{
						Node: &pg_query.Node_SelectStmt{SelectStmt: selectStmt},
					}
					changed = true
					continue
				}

				// If this looks like a PostgreSQL config parameter (e.g., "some_setting")
				// but we don't recognize it, return an error rather than letting DuckDB
				// give a confusing "table not found" error
				if configParamPattern.MatchString(paramName) {
					result.Error = fmt.Errorf("unrecognized configuration parameter %q", paramName)
					return true, nil
				}
			}
		}
	}

	return changed, nil
}

// getDefaultValue returns a sensible default value for an ignored parameter
func (t *SetShowTransform) getDefaultValue(paramName string) string {
	defaults := map[string]string{
		// Client connection settings
		"application_name":   "duckgres",
		"client_encoding":    "UTF8",
		"statement_timeout":  "0",
		"lock_timeout":       "0",
		"extra_float_digits": "1",
		"client_min_messages": "notice",

		// Transaction settings
		"transaction_isolation":         "read committed",
		"default_transaction_isolation": "read committed",
		"transaction_read_only":         "off",
		"transaction_deferrable":        "off",
		"synchronous_commit":            "on",

		// Memory settings
		"work_mem":             "4MB",
		"maintenance_work_mem": "64MB",
		"effective_cache_size": "4GB",

		// Search path and session settings
		"search_path":                 "\"$user\", public",
		"datestyle":                   "ISO, MDY",
		"intervalstyle":               "postgres",
		"standard_conforming_strings": "on",
		"escape_string_warning":       "on",
		"array_nulls":                 "on",
		"backslash_quote":             "safe_encoding",
		"bytea_output":                "hex",

		// Server version settings
		"server_version":     "15.0",
		"server_version_num": "150000",
		"server_encoding":    "UTF8",

		// Timezone
		"timezone": "UTC",

		// Other commonly queried settings
		"max_identifier_length":      "63",
		"default_tablespace":         "",
		"temp_tablespaces":           "",
		"lc_collate":                 "en_US.UTF-8",
		"lc_ctype":                   "en_US.UTF-8",
		"lc_messages":                "en_US.UTF-8",
		"lc_monetary":                "en_US.UTF-8",
		"lc_numeric":                 "en_US.UTF-8",
		"lc_time":                    "en_US.UTF-8",
		"integer_datetimes":          "on",
		"is_superuser":               "on",
		"session_authorization":      "duckdb",
	}
	if val, ok := defaults[paramName]; ok {
		return val
	}
	return "" // Empty string for unknown params
}

// createDefaultValueSelect creates a SELECT 'value' AS name statement
func (t *SetShowTransform) createDefaultValueSelect(paramName, value string) *pg_query.SelectStmt {
	return &pg_query.SelectStmt{
		TargetList: []*pg_query.Node{
			{
				Node: &pg_query.Node_ResTarget{
					ResTarget: &pg_query.ResTarget{
						Name: paramName,
						Val: &pg_query.Node{
							Node: &pg_query.Node_AConst{
								AConst: &pg_query.A_Const{
									Val: &pg_query.A_Const_Sval{
										Sval: &pg_query.String{Sval: value},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// createGetVariableSelect creates a SELECT getvariable('name') AS name statement
func (t *SetShowTransform) createGetVariableSelect(paramName string) *pg_query.SelectStmt {
	return &pg_query.SelectStmt{
		TargetList: []*pg_query.Node{
			{
				Node: &pg_query.Node_ResTarget{
					ResTarget: &pg_query.ResTarget{
						Name: paramName,
						Val: &pg_query.Node{
							Node: &pg_query.Node_FuncCall{
								FuncCall: &pg_query.FuncCall{
									Funcname: []*pg_query.Node{
										{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "getvariable"}}},
									},
									Args: []*pg_query.Node{
										{
											Node: &pg_query.Node_AConst{
												AConst: &pg_query.A_Const{
													Val: &pg_query.A_Const_Sval{
														Sval: &pg_query.String{Sval: paramName},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

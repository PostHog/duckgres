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

// querySourceParam is the duckgres-namespaced custom session GUC that carries
// the pull-based-compute-billing query source. It is intercepted here and
// stored session-side by the connection layer; it is NEVER forwarded to DuckDB
// (DuckDB rejects unknown settings). See clientConn.QuerySource in server/.
const querySourceParam = "duckgres.query_source"

// Valid values for the duckgres.query_source GUC. The value is a billing
// dimension — it lands verbatim in the compute-usage bucket key
// (duckgres_org_compute_usage.query_source) and in the billing pull API — so
// it is a closed enum: accepting arbitrary strings would hand clients
// unbounded cardinality (and arbitrary junk) in the billing table and its
// exports.
const (
	QuerySourceStandard  = "standard"
	QuerySourceEndpoints = "endpoints"
)

// NormalizeQuerySource validates a client-supplied duckgres.query_source value
// against the closed set above. Matching is case-insensitive (and ignores
// surrounding whitespace); the returned value is canonical lowercase. Empty is
// valid and means "reset to default" (the session then reports "standard").
// An invalid value returns a 22023 (invalid_parameter_value) CodedError. The
// message deliberately does NOT echo the offending value: it is arbitrary
// client input (possibly huge) and error messages flow into logs, the query
// log, and the admin recent-errors ring.
func NormalizeQuerySource(raw string) (string, error) {
	switch v := strings.ToLower(strings.TrimSpace(raw)); v {
	case "", QuerySourceStandard, QuerySourceEndpoints:
		return v, nil
	default:
		return "", errInvalidQuerySource()
	}
}

// errInvalidQuerySource is the SET-time rejection for a bad
// duckgres.query_source value: 22023 invalid_parameter_value, matching how the
// control plane rejects invalid duckgres.worker_* startup options.
func errInvalidQuerySource() *CodedError {
	return &CodedError{
		Code: "22023", // invalid_parameter_value
		Message: fmt.Sprintf("invalid value for %q: must be %q or %q",
			querySourceParam, QuerySourceStandard, QuerySourceEndpoints),
	}
}

// duckdbShowCommands are DuckDB-specific SHOW commands that should be passed
// through to DuckDB rather than treated as PostgreSQL config parameters.
var duckdbShowCommands = map[string]bool{
	"tables":    true,
	"databases": true,
	"all":       true,
}

// SetShowTransform handles SET and SHOW commands:
// - SET application_name = 'x' -> SET VARIABLE application_name = 'x'
// - SHOW application_name -> SELECT getvariable('application_name') AS application_name
// - PostgreSQL-specific SET parameters -> IsIgnoredSet = true
// - DuckDB-native SET parameters (e.g. search_path) -> passed through unchanged
type SetShowTransform struct {
	// IgnoredParams are PostgreSQL-specific parameters that should be silently ignored
	IgnoredParams map[string]bool

	// PassthroughParams are parameters that DuckDB natively supports.
	// SET is forwarded unchanged; SHOW is transformed to query duckdb_settings().
	PassthroughParams map[string]bool

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

			// Role/authorization settings
			"role":                  true,
			"session_authorization": true,

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

			// Session settings (silently accept)
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
		PassthroughParams: map[string]bool{
			// Parameters that DuckDB natively supports — forward as-is
			"search_path": true,
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

	// The duckgres.query_source custom GUC is intercepted session-side and
	// surfaced on the whole-batch Result (QuerySourceSet/QuerySourceShow), which
	// makes the transpiler return early for the ENTIRE batch. That is only safe
	// for a single-statement batch: for a multi-statement simple query
	// (e.g. `SET duckgres.query_source='x'; SHOW duckgres.query_source`) an early
	// return would swallow every statement after the GUC one. When the batch has
	// more than one statement we DON'T intercept here — the connection layer
	// splits the batch and re-transpiles each statement on its own, and the
	// single-statement transpile then intercepts the GUC correctly.
	multiStatement := len(tree.Stmts) > 1

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

				// duckgres.query_source: a duckgres-namespaced custom GUC. Intercept
				// and hand the value to the connection layer to store on the session;
				// do NOT forward to DuckDB (it would reject the unknown setting).
				// RESET / SET ... TO DEFAULT restore the default (empty value).
				// SET LOCAL is treated the same as SET here (there is no
				// transaction-scoped restore for this billing GUC). The value is a
				// billing dimension, so it is validated against the closed
				// {standard, endpoints} set (case-insensitively, normalized to
				// lowercase); anything else — including a value that is not a
				// single simple constant/identifier — is rejected with 22023 via
				// result.Error, which every protocol path (simple, split-batch,
				// extended Parse) surfaces before storing anything on the session.
				if paramName == querySourceParam && !multiStatement {
					value := ""
					if n.VariableSetStmt.Kind == pg_query.VariableSetKind_VAR_SET_VALUE {
						extracted := false
						if len(n.VariableSetStmt.Args) == 1 {
							if v, ok := searchPathValue(n.VariableSetStmt.Args[0]); ok {
								value, extracted = v, true
							}
						}
						if !extracted {
							// Multiple values / a non-string constant can never
							// name a valid query source; reject rather than
							// silently resetting to the default.
							result.Error = errInvalidQuerySource()
							return true, nil
						}
					}
					norm, err := NormalizeQuerySource(value)
					if err != nil {
						result.Error = err
						return true, nil
					}
					result.QuerySourceSet = &norm
					return true, nil
				}

				if paramName == "search_path" {
					if sql, ok := normalizeSearchPathSet(n.VariableSetStmt); ok {
						result.SQLOverride = sql
						return true, nil
					}
				}

				// Passthrough params are natively supported by DuckDB — forward unchanged
				if t.PassthroughParams[paramName] {
					return false, nil
				}

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

				// duckgres.query_source: answered from session state by the
				// connection layer (defaulting to "standard"), not DuckDB.
				if paramName == querySourceParam && !multiStatement {
					result.QuerySourceShow = true
					return true, nil
				}

				// Passthrough params: SHOW → SELECT value FROM duckdb_settings() WHERE name = '...'
				// (DuckDB's SHOW <name> describes a table, not a setting)
				if t.PassthroughParams[paramName] {
					selectStmt := t.createSettingsLookupSelect(paramName)
					tree.Stmts[i].Stmt = &pg_query.Node{
						Node: &pg_query.Node_SelectStmt{SelectStmt: selectStmt},
					}
					changed = true
					continue
				}

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

				// DuckDB SHOW commands (SHOW TABLES, SHOW DATABASES, SHOW ALL TABLES)
				// are parsed as VariableShowStmt by pg_query but should be passed
				// through to DuckDB as-is.
				if duckdbShowCommands[paramName] {
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

func normalizeSearchPathSet(stmt *pg_query.VariableSetStmt) (string, bool) {
	if stmt == nil || stmt.Kind != pg_query.VariableSetKind_VAR_SET_VALUE || len(stmt.Args) == 0 {
		return "", false
	}

	parts := make([]string, 0, len(stmt.Args))
	for _, arg := range stmt.Args {
		part, ok := searchPathValue(arg)
		if !ok {
			return "", false
		}
		parts = append(parts, part)
	}

	prefix := "SET search_path = "
	if stmt.IsLocal {
		prefix = "SET LOCAL search_path = "
	}

	value := strings.Join(parts, ",")
	value = strings.ReplaceAll(value, "'", "''")
	return prefix + "'" + value + "'", true
}

func searchPathValue(node *pg_query.Node) (string, bool) {
	if node == nil {
		return "", false
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_AConst:
		if n.AConst == nil {
			return "", false
		}
		if sval := n.AConst.GetSval(); sval != nil {
			return sval.Sval, true
		}
	case *pg_query.Node_String_:
		if n.String_ != nil {
			return n.String_.Sval, true
		}
	case *pg_query.Node_ColumnRef:
		if n.ColumnRef == nil || len(n.ColumnRef.Fields) != 1 {
			return "", false
		}
		field := n.ColumnRef.Fields[0]
		if field == nil {
			return "", false
		}
		if str := field.GetString_(); str != nil {
			return str.Sval, true
		}
	}

	return "", false
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

		// Session settings
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

// createSettingsLookupSelect creates:
//
//	SELECT value AS <name> FROM duckdb_settings() WHERE name = '<name>'
//
// Used for passthrough params where DuckDB's SHOW would try to describe a table.
func (t *SetShowTransform) createSettingsLookupSelect(paramName string) *pg_query.SelectStmt {
	return &pg_query.SelectStmt{
		TargetList: []*pg_query.Node{
			{
				Node: &pg_query.Node_ResTarget{
					ResTarget: &pg_query.ResTarget{
						Name: paramName,
						Val: &pg_query.Node{
							Node: &pg_query.Node_ColumnRef{
								ColumnRef: &pg_query.ColumnRef{
									Fields: []*pg_query.Node{
										{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "value"}}},
									},
								},
							},
						},
					},
				},
			},
		},
		FromClause: []*pg_query.Node{
			{
				Node: &pg_query.Node_RangeFunction{
					RangeFunction: &pg_query.RangeFunction{
						Functions: []*pg_query.Node{
							{
								Node: &pg_query.Node_List{
									List: &pg_query.List{
										Items: []*pg_query.Node{
											{
												Node: &pg_query.Node_FuncCall{
													FuncCall: &pg_query.FuncCall{
														Funcname: []*pg_query.Node{
															{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "duckdb_settings"}}},
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
		},
		WhereClause: &pg_query.Node{
			Node: &pg_query.Node_AExpr{
				AExpr: &pg_query.A_Expr{
					Kind: pg_query.A_Expr_Kind_AEXPR_OP,
					Name: []*pg_query.Node{
						{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "="}}},
					},
					Lexpr: &pg_query.Node{
						Node: &pg_query.Node_ColumnRef{
							ColumnRef: &pg_query.ColumnRef{
								Fields: []*pg_query.Node{
									{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "name"}}},
								},
							},
						},
					},
					Rexpr: &pg_query.Node{
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

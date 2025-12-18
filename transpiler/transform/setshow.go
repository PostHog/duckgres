package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

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
		},
		VariableParams: map[string]bool{
			"application_name": true,
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
				paramName := strings.ToLower(n.VariableSetStmt.Name)

				// Check if this is an ignored parameter
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

		case *pg_query.Node_VariableShowStmt:
			if n.VariableShowStmt != nil {
				paramName := strings.ToLower(n.VariableShowStmt.Name)

				// Convert SHOW application_name to SELECT getvariable('application_name')
				if t.VariableParams[paramName] {
					// Replace with a SELECT statement
					selectStmt := t.createGetVariableSelect(paramName)
					tree.Stmts[i].Stmt = &pg_query.Node{
						Node: &pg_query.Node_SelectStmt{SelectStmt: selectStmt},
					}
					changed = true
				}
			}
		}
	}

	return changed, nil
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

package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// PgCatalogTransform removes pg_catalog schema prefixes and redirects
// table references to our compatibility views.
type PgCatalogTransform struct {
	// ViewMappings maps pg_catalog table names to our compatibility views
	ViewMappings map[string]string

	// Functions that need pg_catalog prefix stripped
	Functions map[string]bool

	// CustomMacros are our custom macros that need memory.main. prefix in DuckLake mode
	// because they're created in the memory database
	CustomMacros map[string]bool

	// DuckLakeMode indicates whether we're running with DuckLake attached
	DuckLakeMode bool
}

// NewPgCatalogTransform creates a new PgCatalogTransform with default mappings.
func NewPgCatalogTransform() *PgCatalogTransform {
	return NewPgCatalogTransformWithConfig(false)
}

// NewPgCatalogTransformWithConfig creates a new PgCatalogTransform with configuration.
func NewPgCatalogTransformWithConfig(duckLakeMode bool) *PgCatalogTransform {
	return &PgCatalogTransform{
		DuckLakeMode: duckLakeMode,
		ViewMappings: map[string]string{
			"pg_class":              "pg_class_full",
			"pg_database":           "pg_database",
			"pg_namespace":          "pg_namespace",
			"pg_collation":          "pg_collation",
			"pg_policy":             "pg_policy",
			"pg_roles":              "pg_roles",
			"pg_statistic_ext":      "pg_statistic_ext",
			"pg_publication_tables": "pg_publication_tables",
			"pg_rules":              "pg_rules",
			"pg_publication":        "pg_publication",
			"pg_publication_rel":    "pg_publication_rel",
			"pg_inherits":           "pg_inherits",
			"pg_matviews":           "pg_matviews",
			"pg_stat_user_tables":   "pg_stat_user_tables",
			"pg_statio_user_tables": "pg_statio_user_tables",
			"pg_stat_statements":    "pg_stat_statements",
			"pg_partitioned_table":  "pg_partitioned_table",
			"pg_type":               "pg_type",
			"pg_attribute":          "pg_attribute",
		},
		Functions: map[string]bool{
			"pg_get_userbyid":                 true,
			"pg_table_is_visible":             true,
			"pg_get_expr":                     true,
			"pg_encoding_to_char":             true,
			"format_type":                     true,
			"obj_description":                 true,
			"col_description":                 true,
			"shobj_description":               true,
			"pg_get_indexdef":                 true,
			"pg_get_constraintdef":            true,
			"pg_get_partkeydef":               true,
			"pg_get_statisticsobjdef_columns": true,
			"pg_get_serial_sequence":          true,
			"pg_relation_is_publishable":      true,
			"current_setting":                 true,
			"pg_is_in_recovery":               true,
			"has_schema_privilege":            true,
			"has_table_privilege":             true,
			"array_to_string":                 true,
			"version":                         true,
		},
		// Our custom macros that are created in memory.main and need explicit qualification
		// in DuckLake mode. These are NOT built-in DuckDB pg_catalog functions.
		// IMPORTANT: Keep in sync with macros defined in server/catalog.go initPgCatalog()
		CustomMacros: map[string]bool{
			"pg_get_userbyid":                 true,
			"pg_encoding_to_char":             true,
			"pg_is_in_recovery":               true,
			"pg_relation_is_publishable":      true,
			"pg_get_statisticsobjdef_columns": true,
			"pg_get_serial_sequence":          true, // Returns NULL - DuckDB doesn't support identity columns
			"shobj_description":               true,
			"current_setting":                 true, // Override DuckDB's built-in with our PostgreSQL-compatible version
			"version":                         true, // PostgreSQL-compatible version string for SQLAlchemy
			"format_type":                     true, // Custom format_type with full PostgreSQL typemod support
		},
	}
}

func (t *PgCatalogTransform) Name() string {
	return "pgcatalog"
}

func (t *PgCatalogTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false

	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}

		// Walk the entire statement looking for nodes to transform
		t.walkAndTransform(stmt.Stmt, &changed)
	}

	return changed, nil
}

func (t *PgCatalogTransform) walkAndTransform(node *pg_query.Node, changed *bool) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		// Table references: pg_catalog.pg_class -> pg_class_full
		// In DuckLake mode, we need to fully qualify with memory.main
		if n.RangeVar != nil {
			relname := strings.ToLower(n.RangeVar.Relname)

			if strings.EqualFold(n.RangeVar.Schemaname, "pg_catalog") {
				// Handle pg_catalog prefixed references (with or without catalog qualifier)
				// e.g., pg_catalog.pg_class, memory.pg_catalog.pg_class, ducklake.pg_catalog.pg_class
				if newName, ok := t.ViewMappings[relname]; ok {
					n.RangeVar.Relname = newName
					// Views are in memory.main schema - always use explicit catalog
					// This ensures views work regardless of default catalog (DuckLake or not)
					n.RangeVar.Catalogname = "memory"
					n.RangeVar.Schemaname = "main"
				} else {
					n.RangeVar.Catalogname = ""
					n.RangeVar.Schemaname = ""
				}
				*changed = true
			} else if n.RangeVar.Schemaname == "" && n.RangeVar.Catalogname == "" {
				// Handle unqualified pg_catalog view names
				// e.g., "SELECT * FROM pg_matviews" -> "SELECT * FROM memory.main.pg_matviews"
				// Always qualify to ensure views are found regardless of default catalog
				if newName, ok := t.ViewMappings[relname]; ok {
					n.RangeVar.Relname = newName
					n.RangeVar.Catalogname = "memory"
					n.RangeVar.Schemaname = "main"
					*changed = true
				}
			}
		}

	case *pg_query.Node_FuncCall:
		// Function calls: pg_catalog.format_type() -> format_type()
		// In DuckLake mode, our custom macros need memory.main. prefix
		if n.FuncCall != nil {
			funcName := ""

			// Get the function name (last element of funcname)
			if len(n.FuncCall.Funcname) >= 1 {
				lastIdx := len(n.FuncCall.Funcname) - 1
				if last := n.FuncCall.Funcname[lastIdx]; last != nil {
					if funcStr := last.GetString_(); funcStr != nil {
						funcName = strings.ToLower(funcStr.Sval)
					}
				}
			}

			// Handle pg_catalog prefixed functions
			if len(n.FuncCall.Funcname) >= 2 {
				if first := n.FuncCall.Funcname[0]; first != nil {
					if str := first.GetString_(); str != nil && strings.EqualFold(str.Sval, "pg_catalog") {
						if t.Functions[funcName] {
							// Check if this is a custom macro that needs memory.main. prefix
							if t.DuckLakeMode && t.CustomMacros[funcName] {
								// Replace pg_catalog with memory.main
								n.FuncCall.Funcname[0] = &pg_query.Node{
									Node: &pg_query.Node_String_{
										String_: &pg_query.String{Sval: "memory"},
									},
								}
								// Insert main between memory and funcname
								n.FuncCall.Funcname = append(n.FuncCall.Funcname[:1],
									append([]*pg_query.Node{{
										Node: &pg_query.Node_String_{
											String_: &pg_query.String{Sval: "main"},
										},
									}}, n.FuncCall.Funcname[1:]...)...)
							} else {
								// Remove the pg_catalog prefix
								n.FuncCall.Funcname = n.FuncCall.Funcname[1:]
							}
							*changed = true
						}
					}
				}
			} else if len(n.FuncCall.Funcname) == 1 && t.DuckLakeMode && t.CustomMacros[funcName] {
				// Unqualified call to a custom macro in DuckLake mode
				// Add memory.main. prefix
				n.FuncCall.Funcname = []*pg_query.Node{
					{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "memory"}}},
					{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "main"}}},
					n.FuncCall.Funcname[0],
				}
				*changed = true
			}
		}
		// Recurse into function arguments
		if n.FuncCall != nil {
			for _, arg := range n.FuncCall.Args {
				t.walkAndTransform(arg, changed)
			}
		}

	case *pg_query.Node_TypeCast:
		// Recurse into type cast arguments
		if n.TypeCast != nil {
			t.walkAndTransform(n.TypeCast.Arg, changed)
		}

	case *pg_query.Node_ColumnRef:
		// Rewrite qualified column references to match renamed tables
		if n.ColumnRef != nil && len(n.ColumnRef.Fields) >= 2 {
			if first := n.ColumnRef.Fields[0]; first != nil {
				if str := first.GetString_(); str != nil {
					tableName := strings.ToLower(str.Sval)
					if newName, ok := t.ViewMappings[tableName]; ok {
						str.Sval = newName
						*changed = true
					}
				}
			}
		}

	case *pg_query.Node_AExpr:
		// Expression: check for OPERATOR(pg_catalog.~) pattern
		if n.AExpr != nil {
			// Handle the special OPERATOR(pg_catalog.xxx) syntax
			// This appears as an A_Expr with name containing pg_catalog
			if len(n.AExpr.Name) >= 2 {
				if first := n.AExpr.Name[0]; first != nil {
					if str := first.GetString_(); str != nil && strings.EqualFold(str.Sval, "pg_catalog") {
						// Remove pg_catalog from operator name
						n.AExpr.Name = n.AExpr.Name[1:]
						*changed = true
					}
				}
			}
			t.walkAndTransform(n.AExpr.Lexpr, changed)
			t.walkAndTransform(n.AExpr.Rexpr, changed)
		}

	case *pg_query.Node_SelectStmt:
		if n.SelectStmt != nil {
			t.walkSelectStmt(n.SelectStmt, changed)
		}

	case *pg_query.Node_InsertStmt:
		if n.InsertStmt != nil {
			if n.InsertStmt.Relation != nil {
				t.walkAndTransform(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: n.InsertStmt.Relation}}, changed)
			}
			for _, col := range n.InsertStmt.Cols {
				t.walkAndTransform(col, changed)
			}
			t.walkAndTransform(n.InsertStmt.SelectStmt, changed)
		}

	case *pg_query.Node_UpdateStmt:
		if n.UpdateStmt != nil {
			if n.UpdateStmt.Relation != nil {
				t.walkAndTransform(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: n.UpdateStmt.Relation}}, changed)
			}
			for _, target := range n.UpdateStmt.TargetList {
				t.walkAndTransform(target, changed)
			}
			t.walkAndTransform(n.UpdateStmt.WhereClause, changed)
			for _, from := range n.UpdateStmt.FromClause {
				t.walkAndTransform(from, changed)
			}
		}

	case *pg_query.Node_DeleteStmt:
		if n.DeleteStmt != nil {
			if n.DeleteStmt.Relation != nil {
				t.walkAndTransform(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: n.DeleteStmt.Relation}}, changed)
			}
			t.walkAndTransform(n.DeleteStmt.WhereClause, changed)
		}

	case *pg_query.Node_JoinExpr:
		if n.JoinExpr != nil {
			t.walkAndTransform(n.JoinExpr.Larg, changed)
			t.walkAndTransform(n.JoinExpr.Rarg, changed)
			t.walkAndTransform(n.JoinExpr.Quals, changed)
		}

	case *pg_query.Node_SubLink:
		if n.SubLink != nil {
			t.walkAndTransform(n.SubLink.Subselect, changed)
			t.walkAndTransform(n.SubLink.Testexpr, changed)
		}

	case *pg_query.Node_BoolExpr:
		if n.BoolExpr != nil {
			for _, arg := range n.BoolExpr.Args {
				t.walkAndTransform(arg, changed)
			}
		}

	case *pg_query.Node_ResTarget:
		if n.ResTarget != nil {
			t.walkAndTransform(n.ResTarget.Val, changed)
		}

	case *pg_query.Node_CommonTableExpr:
		if n.CommonTableExpr != nil {
			t.walkAndTransform(n.CommonTableExpr.Ctequery, changed)
		}

	case *pg_query.Node_WithClause:
		if n.WithClause != nil {
			for _, cte := range n.WithClause.Ctes {
				t.walkAndTransform(cte, changed)
			}
		}

	case *pg_query.Node_RangeSubselect:
		if n.RangeSubselect != nil {
			t.walkAndTransform(n.RangeSubselect.Subquery, changed)
		}

	case *pg_query.Node_CaseExpr:
		if n.CaseExpr != nil {
			t.walkAndTransform(n.CaseExpr.Arg, changed)
			for _, when := range n.CaseExpr.Args {
				t.walkAndTransform(when, changed)
			}
			t.walkAndTransform(n.CaseExpr.Defresult, changed)
		}

	case *pg_query.Node_CaseWhen:
		if n.CaseWhen != nil {
			t.walkAndTransform(n.CaseWhen.Expr, changed)
			t.walkAndTransform(n.CaseWhen.Result, changed)
		}

	case *pg_query.Node_CoalesceExpr:
		if n.CoalesceExpr != nil {
			for _, arg := range n.CoalesceExpr.Args {
				t.walkAndTransform(arg, changed)
			}
		}

	case *pg_query.Node_NullTest:
		if n.NullTest != nil {
			t.walkAndTransform(n.NullTest.Arg, changed)
		}

	case *pg_query.Node_List:
		if n.List != nil {
			for _, item := range n.List.Items {
				t.walkAndTransform(item, changed)
			}
		}

	case *pg_query.Node_CollateClause:
		// COLLATE pg_catalog."default" -> remove
		if n.CollateClause != nil {
			// Check if it's pg_catalog.default collation
			if len(n.CollateClause.Collname) >= 2 {
				if first := n.CollateClause.Collname[0]; first != nil {
					if str := first.GetString_(); str != nil && strings.EqualFold(str.Sval, "pg_catalog") {
						if second := n.CollateClause.Collname[1]; second != nil {
							if collStr := second.GetString_(); collStr != nil && strings.EqualFold(collStr.Sval, "default") {
								// Clear the collation - this will be handled by the parent
								n.CollateClause.Collname = nil
								*changed = true
							}
						}
					}
				}
			}
			t.walkAndTransform(n.CollateClause.Arg, changed)
		}
	}
}

func (t *PgCatalogTransform) walkSelectStmt(stmt *pg_query.SelectStmt, changed *bool) {
	if stmt == nil {
		return
	}

	for _, target := range stmt.TargetList {
		t.walkAndTransform(target, changed)
	}
	for _, from := range stmt.FromClause {
		t.walkAndTransform(from, changed)
	}
	t.walkAndTransform(stmt.WhereClause, changed)
	for _, group := range stmt.GroupClause {
		t.walkAndTransform(group, changed)
	}
	t.walkAndTransform(stmt.HavingClause, changed)
	for _, sort := range stmt.SortClause {
		t.walkAndTransform(sort, changed)
	}
	t.walkAndTransform(stmt.LimitCount, changed)
	t.walkAndTransform(stmt.LimitOffset, changed)

	if stmt.WithClause != nil {
		t.walkAndTransform(&pg_query.Node{Node: &pg_query.Node_WithClause{WithClause: stmt.WithClause}}, changed)
	}

	// Handle UNION/INTERSECT/EXCEPT
	if stmt.Larg != nil {
		t.walkSelectStmt(stmt.Larg, changed)
	}
	if stmt.Rarg != nil {
		t.walkSelectStmt(stmt.Rarg, changed)
	}
}

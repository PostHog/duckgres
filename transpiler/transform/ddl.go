package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// DDLTransform strips unsupported DDL features for DuckLake compatibility.
// DuckLake does not support: PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK constraints,
// SERIAL types, DEFAULT now(), GENERATED columns, or indexes.
type DDLTransform struct{}

// NewDDLTransform creates a new DDLTransform.
func NewDDLTransform() *DDLTransform {
	return &DDLTransform{}
}

func (t *DDLTransform) Name() string {
	return "ddl"
}

func (t *DDLTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false

	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}

		switch n := stmt.Stmt.Node.(type) {
		case *pg_query.Node_CreateStmt:
			if n.CreateStmt != nil {
				if t.transformCreateStmt(n.CreateStmt) {
					changed = true
				}
			}

		case *pg_query.Node_IndexStmt:
			// CREATE INDEX is a no-op for DuckLake
			result.IsNoOp = true
			result.NoOpTag = "CREATE INDEX"
			return true, nil

		case *pg_query.Node_DropStmt:
			if n.DropStmt != nil {
				// Check if it's DROP INDEX
				if n.DropStmt.RemoveType == pg_query.ObjectType_OBJECT_INDEX {
					result.IsNoOp = true
					result.NoOpTag = "DROP INDEX"
					return true, nil
				}
				// DuckLake doesn't support CASCADE on DROP TABLE/VIEW.
				// Strip CASCADE by converting to RESTRICT (same approach as dbt-duckdb).
				// See: https://github.com/duckdb/dbt-duckdb/pull/557
				// Note: DROP SCHEMA CASCADE is supported by DuckDB natively, so preserve it.
				if n.DropStmt.Behavior == pg_query.DropBehavior_DROP_CASCADE {
					if n.DropStmt.RemoveType == pg_query.ObjectType_OBJECT_TABLE ||
						n.DropStmt.RemoveType == pg_query.ObjectType_OBJECT_VIEW ||
						n.DropStmt.RemoveType == pg_query.ObjectType_OBJECT_MATVIEW {
						n.DropStmt.Behavior = pg_query.DropBehavior_DROP_RESTRICT
						changed = true
					}
				}
			}

		case *pg_query.Node_ReindexStmt:
			result.IsNoOp = true
			result.NoOpTag = "REINDEX"
			return true, nil

		case *pg_query.Node_ClusterStmt:
			result.IsNoOp = true
			result.NoOpTag = "CLUSTER"
			return true, nil

		case *pg_query.Node_VacuumStmt:
			result.IsNoOp = true
			result.NoOpTag = "VACUUM"
			return true, nil

		case *pg_query.Node_GrantStmt:
			// GRANT and REVOKE are no-ops
			if n.GrantStmt.IsGrant {
				result.IsNoOp = true
				result.NoOpTag = "GRANT"
			} else {
				result.IsNoOp = true
				result.NoOpTag = "REVOKE"
			}
			return true, nil

		case *pg_query.Node_CommentStmt:
			result.IsNoOp = true
			result.NoOpTag = "COMMENT"
			return true, nil

		case *pg_query.Node_AlterTableStmt:
			if n.AlterTableStmt != nil {
				for _, cmd := range n.AlterTableStmt.Cmds {
					if alterCmd := cmd.GetAlterTableCmd(); alterCmd != nil {
						// Check for unsupported ALTER TABLE commands (constraints, NOT NULL, DEFAULT, etc.)
						if t.isUnsupportedAlterCommand(alterCmd) {
							result.IsNoOp = true
							result.NoOpTag = "ALTER TABLE"
							return true, nil
						}
						// Make ADD COLUMN idempotent by adding IF NOT EXISTS.
						// DuckLake reuses physical tables across sqlmesh snapshots, so
						// columns may already exist when sqlmesh issues ALTER TABLE ADD COLUMN.
						if alterCmd.Subtype == pg_query.AlterTableType_AT_AddColumn && !alterCmd.MissingOk {
							alterCmd.MissingOk = true
							changed = true
						}
					}
				}
			}

		case *pg_query.Node_RefreshMatViewStmt:
			result.IsNoOp = true
			result.NoOpTag = "REFRESH MATERIALIZED VIEW"
			return true, nil
		}
	}

	return changed, nil
}

// transformCreateStmt modifies a CREATE TABLE statement for DuckLake compatibility
func (t *DDLTransform) transformCreateStmt(stmt *pg_query.CreateStmt) bool {
	changed := false

	// Process column definitions
	newTableElts := make([]*pg_query.Node, 0, len(stmt.TableElts))
	for _, elt := range stmt.TableElts {
		switch n := elt.Node.(type) {
		case *pg_query.Node_ColumnDef:
			if n.ColumnDef != nil {
				// Transform the column definition
				if t.transformColumnDef(n.ColumnDef) {
					changed = true
				}
				newTableElts = append(newTableElts, elt)
			}
		case *pg_query.Node_Constraint:
			// Skip table-level constraints (PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK)
			if n.Constraint != nil {
				if t.isUnsupportedConstraint(n.Constraint) {
					changed = true
					continue // Skip this constraint
				}
				newTableElts = append(newTableElts, elt)
			}
		default:
			newTableElts = append(newTableElts, elt)
		}
	}
	stmt.TableElts = newTableElts

	// Remove table-level constraints from the Constraints slice
	if len(stmt.Constraints) > 0 {
		newConstraints := make([]*pg_query.Node, 0)
		for _, c := range stmt.Constraints {
			if constraint := c.GetConstraint(); constraint != nil {
				if !t.isUnsupportedConstraint(constraint) {
					newConstraints = append(newConstraints, c)
				} else {
					changed = true
				}
			} else {
				newConstraints = append(newConstraints, c)
			}
		}
		stmt.Constraints = newConstraints
	}

	return changed
}

// transformColumnDef modifies a column definition for DuckLake compatibility
func (t *DDLTransform) transformColumnDef(col *pg_query.ColumnDef) bool {
	changed := false

	// Convert SERIAL types to INTEGER types
	if col.TypeName != nil {
		if t.convertSerialType(col.TypeName) {
			changed = true
		}
	}

	// Remove unsupported column constraints
	if len(col.Constraints) > 0 {
		newConstraints := make([]*pg_query.Node, 0)
		for _, c := range col.Constraints {
			if constraint := c.GetConstraint(); constraint != nil {
				if t.isUnsupportedColumnConstraint(constraint) {
					changed = true
					continue
				}
				// Check for DEFAULT now()/current_timestamp
				if constraint.Contype == pg_query.ConstrType_CONSTR_DEFAULT {
					if t.isUnsupportedDefault(constraint.RawExpr) {
						changed = true
						continue
					}
				}
				// Check for GENERATED columns
				if constraint.Contype == pg_query.ConstrType_CONSTR_GENERATED {
					changed = true
					continue
				}
				newConstraints = append(newConstraints, c)
			} else {
				newConstraints = append(newConstraints, c)
			}
		}
		col.Constraints = newConstraints
	}

	return changed
}

// convertSerialType converts SERIAL types to INTEGER types
func (t *DDLTransform) convertSerialType(typeName *pg_query.TypeName) bool {
	if typeName == nil || len(typeName.Names) == 0 {
		return false
	}

	// Get the type name string
	var typeStr string
	for _, name := range typeName.Names {
		if str := name.GetString_(); str != nil {
			typeStr = strings.ToLower(str.Sval)
			break
		}
	}

	// Map SERIAL types to INTEGER types
	var newType string
	switch typeStr {
	case "serial", "serial4":
		newType = "int4"
	case "bigserial", "serial8":
		newType = "int8"
	case "smallserial", "serial2":
		newType = "int2"
	default:
		return false
	}

	// Replace the type name
	typeName.Names = []*pg_query.Node{
		{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: newType}}},
	}
	return true
}

// isUnsupportedConstraint checks if a table-level constraint is unsupported
func (t *DDLTransform) isUnsupportedConstraint(c *pg_query.Constraint) bool {
	switch c.Contype {
	case pg_query.ConstrType_CONSTR_PRIMARY,
		pg_query.ConstrType_CONSTR_UNIQUE,
		pg_query.ConstrType_CONSTR_FOREIGN,
		pg_query.ConstrType_CONSTR_CHECK,
		pg_query.ConstrType_CONSTR_EXCLUSION:
		return true
	}
	return false
}

// isUnsupportedColumnConstraint checks if a column-level constraint is unsupported
func (t *DDLTransform) isUnsupportedColumnConstraint(c *pg_query.Constraint) bool {
	switch c.Contype {
	case pg_query.ConstrType_CONSTR_PRIMARY,
		pg_query.ConstrType_CONSTR_UNIQUE,
		pg_query.ConstrType_CONSTR_FOREIGN,
		pg_query.ConstrType_CONSTR_CHECK,
		pg_query.ConstrType_CONSTR_EXCLUSION:
		return true
	}
	return false
}

// isUnsupportedDefault checks if a DEFAULT expression is unsupported by DuckLake.
// DuckLake only supports simple numeric and string literals as defaults.
// Returns true if the default should be stripped.
func (t *DDLTransform) isUnsupportedDefault(expr *pg_query.Node) bool {
	if expr == nil {
		return false
	}

	// Check for function calls (e.g., now(), current_timestamp)
	if funcCall := expr.GetFuncCall(); funcCall != nil {
		return true // All function calls are unsupported
	}

	// Check for SQLValueFunction (CURRENT_TIMESTAMP, CURRENT_DATE, etc.)
	if svf := expr.GetSqlvalueFunction(); svf != nil {
		return true // All SQL value functions are unsupported
	}

	// Check for boolean constants (DEFAULT true/false)
	// DuckLake only supports numeric and string literals
	if typeCast := expr.GetTypeCast(); typeCast != nil {
		return t.isUnsupportedDefault(typeCast.Arg)
	}

	// Check for A_Const nodes - only allow Integer and String
	if aconst := expr.GetAConst(); aconst != nil {
		switch aconst.Val.(type) {
		case *pg_query.A_Const_Ival, *pg_query.A_Const_Fval, *pg_query.A_Const_Sval:
			return false // These are supported
		default:
			return true // Booleans, NULLs, etc. are not supported
		}
	}

	// Column references, expressions, etc. are not supported
	return true
}

// isUnsupportedAlterCommand checks if an ALTER TABLE command is unsupported by DuckLake
func (t *DDLTransform) isUnsupportedAlterCommand(cmd *pg_query.AlterTableCmd) bool {
	switch cmd.Subtype {
	// Constraint commands
	case pg_query.AlterTableType_AT_AddConstraint,
		pg_query.AlterTableType_AT_ValidateConstraint,
		pg_query.AlterTableType_AT_DropConstraint:
		return true
	// NOT NULL commands - DuckLake doesn't handle these well
	case pg_query.AlterTableType_AT_SetNotNull,
		pg_query.AlterTableType_AT_DropNotNull:
		return true
	// DEFAULT commands - DuckLake has limited support
	case pg_query.AlterTableType_AT_ColumnDefault:
		return true
	}
	return false
}

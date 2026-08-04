package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/posthog/duckgres/transpiler/backend"
)

// DDLTransform strips unsupported DDL features for backends (DuckLake)
// that don't support: PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK constraints,
// SERIAL types, DEFAULT now(), GENERATED columns, or indexes. Which behaviors
// apply is driven by the backend's capabilities.
type DDLTransform struct {
	policy backend.DDLPolicy
}

// NewDDLTransform creates a new DDLTransform driven by the given DDL policy.
func NewDDLTransform(policy backend.DDLPolicy) *DDLTransform {
	return &DDLTransform{policy: policy}
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
				if t.transformCreateStmt(n.CreateStmt, result) {
					changed = true
				}
				if result.Error != nil {
					return true, nil
				}
			}

		case *pg_query.Node_IndexStmt:
			// CREATE INDEX is a no-op
			if t.policy.UnsupportedDDL == backend.NoOpUnsupportedDDL {
				result.IsNoOp = true
				result.NoOpTag = "CREATE INDEX"
				return true, nil
			}

		case *pg_query.Node_DropStmt:
			if n.DropStmt != nil {
				// Check if it's DROP INDEX
				if t.policy.UnsupportedDDL == backend.NoOpUnsupportedDDL && n.DropStmt.RemoveType == pg_query.ObjectType_OBJECT_INDEX {
					result.IsNoOp = true
					result.NoOpTag = "DROP INDEX"
					return true, nil
				}
				// DuckLake doesn't support CASCADE on DROP TABLE/VIEW.
				// Strip CASCADE by converting to RESTRICT (same approach as dbt-duckdb).
				// See: https://github.com/duckdb/dbt-duckdb/pull/557
				// Note: DROP SCHEMA CASCADE is supported by DuckDB natively, so preserve it.
				if t.policy.RewriteCascadeDrop && n.DropStmt.Behavior == pg_query.DropBehavior_DROP_CASCADE {
					if n.DropStmt.RemoveType == pg_query.ObjectType_OBJECT_TABLE ||
						n.DropStmt.RemoveType == pg_query.ObjectType_OBJECT_VIEW ||
						n.DropStmt.RemoveType == pg_query.ObjectType_OBJECT_MATVIEW {
						n.DropStmt.Behavior = pg_query.DropBehavior_DROP_RESTRICT
						changed = true
					}
				}
			}

		case *pg_query.Node_ReindexStmt:
			if t.policy.UnsupportedDDL == backend.NoOpUnsupportedDDL {
				result.IsNoOp = true
				result.NoOpTag = "REINDEX"
				return true, nil
			}

		case *pg_query.Node_ClusterStmt:
			if t.policy.UnsupportedDDL == backend.NoOpUnsupportedDDL {
				result.IsNoOp = true
				result.NoOpTag = "CLUSTER"
				return true, nil
			}

		case *pg_query.Node_VacuumStmt:
			// ANALYZE and VACUUM both parse as VacuumStmt; distinguish the command
			// tag (DuckDB rejects either against a DuckLake catalog).
			if t.policy.UnsupportedDDL == backend.NoOpUnsupportedDDL {
				result.IsNoOp = true
				result.NoOpTag = "VACUUM"
				if n.VacuumStmt != nil && !n.VacuumStmt.IsVacuumcmd {
					result.NoOpTag = "ANALYZE"
				}
				return true, nil
			}

		case *pg_query.Node_GrantStmt:
			// GRANT and REVOKE are no-ops
			if t.policy.UnsupportedDDL == backend.NoOpUnsupportedDDL {
				if n.GrantStmt.IsGrant {
					result.IsNoOp = true
					result.NoOpTag = "GRANT"
				} else {
					result.IsNoOp = true
					result.NoOpTag = "REVOKE"
				}
				return true, nil
			}

		case *pg_query.Node_CommentStmt:
			if t.policy.UnsupportedDDL == backend.NoOpUnsupportedDDL {
				result.IsNoOp = true
				result.NoOpTag = "COMMENT"
				return true, nil
			}

		case *pg_query.Node_AlterTableStmt:
			if t.policy.SplitMultiAlter && n.AlterTableStmt != nil {
				if didChange, err := t.transformAlterTableStmt(n.AlterTableStmt, result); err != nil {
					return false, err
				} else if didChange {
					changed = true
				}
				if result.Error != nil || result.IsNoOp || len(result.Statements) > 0 {
					return true, nil
				}
			}

		case *pg_query.Node_RefreshMatViewStmt:
			if t.policy.UnsupportedDDL == backend.NoOpUnsupportedDDL {
				result.IsNoOp = true
				result.NoOpTag = "REFRESH MATERIALIZED VIEW"
				return true, nil
			}
		}
	}

	return changed, nil
}

// transformCreateStmt modifies a CREATE TABLE statement for DuckLake compatibility.
// Unenforceable constraints (PK/UNIQUE/CHECK/FK) and silently-NULL data features
// (SERIAL, GENERATED STORED, DEFAULT <expr>) are stripped/rewritten.
func (t *DDLTransform) transformCreateStmt(stmt *pg_query.CreateStmt, result *Result) bool {
	changed := false

	// Process column definitions
	newTableElts := make([]*pg_query.Node, 0, len(stmt.TableElts))
	for _, elt := range stmt.TableElts {
		switch n := elt.Node.(type) {
		case *pg_query.Node_ColumnDef:
			if n.ColumnDef != nil {
				// Transform the column definition
				if t.transformColumnDef(n.ColumnDef, result) {
					changed = true
				}
				if result.Error != nil {
					return changed
				}
				newTableElts = append(newTableElts, elt)
			}
		case *pg_query.Node_Constraint:
			// Skip table-level constraints (PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK)
			if n.Constraint != nil {
				if t.policy.ConstraintHandling == backend.StripConstraints && t.isUnsupportedConstraint(n.Constraint) {
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
	if t.policy.ConstraintHandling == backend.StripConstraints && len(stmt.Constraints) > 0 {
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

// transformColumnDef modifies a column definition for DuckLake compatibility.
// SERIAL is rewritten to plain integer types; GENERATED STORED and DEFAULT <expr>
// (which would silently produce NULL data on a lake catalog) are stripped.
// Unenforceable column constraints (PK/UNIQUE/CHECK/FK) are stripped.
func (t *DDLTransform) transformColumnDef(col *pg_query.ColumnDef, result *Result) bool {
	changed := false

	// SERIAL/BIGSERIAL: no backing sequence on a lake catalog -> ids silently NULL.
	if col.TypeName != nil {
		if serialTypeName(col.TypeName) != "" {
			if t.policy.RewriteSerial && t.convertSerialType(col.TypeName) {
				changed = true
			}
		}
	}

	// Remove unsupported column constraints
	if len(col.Constraints) > 0 {
		newConstraints := make([]*pg_query.Node, 0)
		for _, c := range col.Constraints {
			constraint := c.GetConstraint()
			if constraint == nil {
				newConstraints = append(newConstraints, c)
				continue
			}
			// Unenforceable constraints (PK/UNIQUE/CHECK/FK/EXCLUSION): strip.
			if t.policy.ConstraintHandling == backend.StripConstraints && t.isUnsupportedColumnConstraint(constraint) {
				changed = true
				continue
			}
			// GENERATED ALWAYS AS (...) STORED: computed value would be silently NULL.
			if constraint.Contype == pg_query.ConstrType_CONSTR_GENERATED {
				if t.policy.StripVolatileDefaults {
					changed = true
					continue
				}
			}
			// DEFAULT <expression>/now(): the default would be silently dropped (NULL).
			// Literal int/float/string and DEFAULT NULL are not "unsupported" here and
			// are passed through to the engine.
			if constraint.Contype == pg_query.ConstrType_CONSTR_DEFAULT && t.isUnsupportedDefault(constraint.RawExpr) {
				if t.policy.StripVolatileDefaults {
					changed = true
					continue
				}
			}
			newConstraints = append(newConstraints, c)
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

	// Check for A_Const nodes - allow NULL and Integer/Float/String literals.
	if aconst := expr.GetAConst(); aconst != nil {
		// DEFAULT NULL is fine: NULL is the implicit default anyway, so it is
		// neither stripped nor an error.
		if aconst.Isnull {
			return false
		}
		switch aconst.Val.(type) {
		case *pg_query.A_Const_Ival, *pg_query.A_Const_Fval, *pg_query.A_Const_Sval:
			return false // These are supported
		default:
			return true // Booleans, etc. are not supported
		}
	}

	// Column references, expressions, etc. are not supported
	return true
}

// transformAlterTableStmt handles ALTER TABLE statements for DuckLake compatibility.
// DuckDB only supports one ALTER command per statement, so multi-command ALTER TABLE
// statements (e.g., ADD COLUMN x, ADD COLUMN y) are split into individual statements
// wrapped in a transaction. Unsupported commands (constraints, NOT NULL, DEFAULT) are
// silently dropped.
func (t *DDLTransform) transformAlterTableStmt(stmt *pg_query.AlterTableStmt, result *Result) (bool, error) {
	// Partition commands into supported and unsupported
	var supported []*pg_query.Node
	for _, cmd := range stmt.Cmds {
		alterCmd := cmd.GetAlterTableCmd()
		if alterCmd == nil {
			continue
		}
		if alterColumnTypeHasUsingExpression(alterCmd) {
			result.Error = NewFeatureNotSupported(
				"ALTER COLUMN TYPE ... USING <expression> is not supported on this catalog")
			return false, nil
		}
		// ADD COLUMN carries a full ColumnDef: apply the same SERIAL/GENERATED/
		// DEFAULT-expr error and constraint-warn handling as CREATE TABLE.
		if alterCmd.Subtype == pg_query.AlterTableType_AT_AddColumn {
			if def := alterCmd.Def.GetColumnDef(); def != nil {
				t.transformColumnDef(def, result)
				if result.Error != nil {
					return false, nil
				}
			}
			// Make ADD COLUMN idempotent by adding IF NOT EXISTS.
			// DuckLake reuses physical tables across sqlmesh snapshots, so
			// columns may already exist when sqlmesh issues ALTER TABLE ADD COLUMN.
			if !alterCmd.MissingOk {
				alterCmd.MissingOk = true
			}
			supported = append(supported, cmd)
			continue
		}
		if t.isUnsupportedAlterCommand(alterCmd) {
			continue
		}
		supported = append(supported, cmd)
	}

	// All commands unsupported → no-op
	if len(supported) == 0 {
		result.IsNoOp = true
		result.NoOpTag = "ALTER TABLE"
		return true, nil
	}

	// Single supported command → modify AST in place, let normal deparse handle it
	if len(supported) == 1 {
		stmt.Cmds = supported
		return true, nil
	}

	// Multiple supported commands → split into individual ALTER TABLE statements.
	// DuckDB only supports one ALTER command per statement, so we wrap the
	// individual statements in a transaction for atomicity.
	stmts := make([]string, 0, len(supported)+1)
	stmts = append(stmts, "BEGIN")
	for _, cmd := range supported {
		singleTree := &pg_query.ParseResult{
			Stmts: []*pg_query.RawStmt{{
				Stmt: &pg_query.Node{
					Node: &pg_query.Node_AlterTableStmt{
						AlterTableStmt: &pg_query.AlterTableStmt{
							Relation:  stmt.Relation,
							Cmds:      []*pg_query.Node{cmd},
							Objtype:   stmt.Objtype,
							MissingOk: stmt.MissingOk,
						},
					},
				},
			}},
		}
		deparsed, err := pg_query.Deparse(singleTree)
		if err != nil {
			return false, err
		}
		stmts = append(stmts, deparsed)
	}
	result.Statements = stmts
	result.CleanupStatements = []string{"COMMIT"}
	return true, nil
}

func alterColumnTypeHasUsingExpression(cmd *pg_query.AlterTableCmd) bool {
	if cmd == nil || cmd.Subtype != pg_query.AlterTableType_AT_AlterColumnType {
		return false
	}
	def := cmd.Def.GetColumnDef()
	// pg_query stores ALTER COLUMN TYPE ... USING <expr> on ColumnDef.RawDefault
	// for AT_AlterColumnType commands.
	return def != nil && def.RawDefault != nil
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

// serialTypeName returns the lowercased serial type name (serial/bigserial/...)
// if the column type is a SERIAL pseudo-type, or "" otherwise.
func serialTypeName(typeName *pg_query.TypeName) string {
	if typeName == nil || len(typeName.Names) == 0 {
		return ""
	}
	var typeStr string
	for _, name := range typeName.Names {
		if str := name.GetString_(); str != nil {
			typeStr = strings.ToLower(str.Sval)
			break
		}
	}
	switch typeStr {
	case "serial", "serial4", "bigserial", "serial8", "smallserial", "serial2":
		return typeStr
	}
	return ""
}

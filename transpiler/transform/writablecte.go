package transform

import (
	"fmt"
	"math/rand"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

const maxIdentifierLength = 63 // PostgreSQL/DuckDB identifier limit

// WritableCTETransform detects and rewrites PostgreSQL writable CTEs
// (CTEs containing UPDATE/INSERT/DELETE) into equivalent multi-statement
// sequences that DuckDB can execute.
//
// PostgreSQL allows writable CTEs like:
//
//	WITH updates AS (UPDATE t SET col = 1 RETURNING *) SELECT * FROM updates
//
// DuckDB does not support this - it requires CTEs to be SELECT statements.
// This transform rewrites such queries into:
//  1. BEGIN transaction
//  2. CREATE TEMP TABLE for each CTE (preserving declaration order)
//  3. Execute the final statement
//  4. Cleanup: DROP temp tables, COMMIT
type WritableCTETransform struct{}

func NewWritableCTETransform() *WritableCTETransform {
	return &WritableCTETransform{}
}

func (t *WritableCTETransform) Name() string {
	return "writable_cte"
}

// cteInfo holds metadata about a single CTE
type cteInfo struct {
	name    string                    // CTE name
	node    *pg_query.CommonTableExpr // The CTE AST node
	isWrite bool                      // true if UPDATE/INSERT/DELETE
	deps    []string                  // CTE names this one references
	order   int                       // declaration order (0, 1, 2, ...)
}

func (t *WritableCTETransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}

		// Check for ANY statement with CTEs (SELECT, INSERT, UPDATE, DELETE)
		withClause := t.getWithClause(stmt.Stmt)
		if withClause == nil {
			continue
		}

		// Analyze CTEs (preserves declaration order)
		ctes := t.analyzeCTEs(withClause)

		// Only rewrite if there are writable CTEs
		if !t.hasWritableCTE(ctes) {
			continue // All read-only CTEs, pass through to DuckDB
		}

		// Rewrite into multi-statement sequence (processes in declaration order)
		rewrite, err := t.rewriteWritableCTE(stmt, ctes)
		if err != nil {
			result.Error = err
			return true, nil
		}

		// Populate Result with separated statements and cleanup
		result.Statements = rewrite.statements
		result.CleanupStatements = rewrite.cleanup
		return true, nil
	}

	return false, nil
}

// getWithClause extracts WITH clause from any statement type
func (t *WritableCTETransform) getWithClause(node *pg_query.Node) *pg_query.WithClause {
	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		if n.SelectStmt != nil {
			return n.SelectStmt.WithClause
		}
	case *pg_query.Node_InsertStmt:
		if n.InsertStmt != nil {
			return n.InsertStmt.WithClause
		}
	case *pg_query.Node_UpdateStmt:
		if n.UpdateStmt != nil {
			return n.UpdateStmt.WithClause
		}
	case *pg_query.Node_DeleteStmt:
		if n.DeleteStmt != nil {
			return n.DeleteStmt.WithClause
		}
	}
	return nil
}

// analyzeCTEs extracts CTE metadata while preserving declaration order
func (t *WritableCTETransform) analyzeCTEs(with *pg_query.WithClause) []*cteInfo {
	var ctes []*cteInfo
	cteNames := make(map[string]bool) // Track known CTE names for dependency detection

	for i, cteNode := range with.Ctes {
		cte := cteNode.GetCommonTableExpr()
		if cte == nil {
			continue
		}

		cteNames[cte.Ctename] = true

		info := &cteInfo{
			name:  cte.Ctename,
			node:  cte,
			deps:  t.findCTEDependencies(cte.Ctequery, cteNames),
			order: i,
		}

		// Classify as writable or read-only
		switch cte.Ctequery.Node.(type) {
		case *pg_query.Node_UpdateStmt,
			*pg_query.Node_InsertStmt,
			*pg_query.Node_DeleteStmt:
			info.isWrite = true
		}

		ctes = append(ctes, info)
	}

	return ctes // Preserves declaration order
}

// hasWritableCTE checks if any CTE contains DML
func (t *WritableCTETransform) hasWritableCTE(ctes []*cteInfo) bool {
	for _, cte := range ctes {
		if cte.isWrite {
			return true
		}
	}
	return false
}

// findCTEDependencies extracts names of CTEs referenced in a query
func (t *WritableCTETransform) findCTEDependencies(node *pg_query.Node, knownCTEs map[string]bool) []string {
	var deps []string
	seen := make(map[string]bool)

	t.walkForDependencies(node, knownCTEs, &deps, seen)

	return deps
}

// walkForDependencies recursively finds CTE references
func (t *WritableCTETransform) walkForDependencies(node *pg_query.Node, knownCTEs map[string]bool, deps *[]string, seen map[string]bool) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		if n.RangeVar != nil {
			// Only count as CTE reference if UNQUALIFIED (no catalog, no schema)
			// Schema-qualified refs like "public.mytable" are real tables, not CTEs
			if n.RangeVar.Catalogname == "" && n.RangeVar.Schemaname == "" && n.RangeVar.Relname != "" {
				name := n.RangeVar.Relname
				if knownCTEs[name] && !seen[name] {
					*deps = append(*deps, name)
					seen[name] = true
				}
			}
		}

	case *pg_query.Node_SelectStmt:
		if n.SelectStmt != nil {
			for _, from := range n.SelectStmt.FromClause {
				t.walkForDependencies(from, knownCTEs, deps, seen)
			}
			t.walkForDependencies(n.SelectStmt.WhereClause, knownCTEs, deps, seen)
			for _, target := range n.SelectStmt.TargetList {
				t.walkForDependencies(target, knownCTEs, deps, seen)
			}
			// Don't recurse into the WITH clause - those are definitions, not references
			if n.SelectStmt.Larg != nil {
				t.walkForDependencies(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: n.SelectStmt.Larg}}, knownCTEs, deps, seen)
			}
			if n.SelectStmt.Rarg != nil {
				t.walkForDependencies(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: n.SelectStmt.Rarg}}, knownCTEs, deps, seen)
			}
		}

	case *pg_query.Node_UpdateStmt:
		if n.UpdateStmt != nil {
			for _, from := range n.UpdateStmt.FromClause {
				t.walkForDependencies(from, knownCTEs, deps, seen)
			}
			t.walkForDependencies(n.UpdateStmt.WhereClause, knownCTEs, deps, seen)
			for _, target := range n.UpdateStmt.TargetList {
				t.walkForDependencies(target, knownCTEs, deps, seen)
			}
		}

	case *pg_query.Node_DeleteStmt:
		if n.DeleteStmt != nil {
			t.walkForDependencies(n.DeleteStmt.WhereClause, knownCTEs, deps, seen)
			for _, using := range n.DeleteStmt.UsingClause {
				t.walkForDependencies(using, knownCTEs, deps, seen)
			}
		}

	case *pg_query.Node_InsertStmt:
		if n.InsertStmt != nil {
			t.walkForDependencies(n.InsertStmt.SelectStmt, knownCTEs, deps, seen)
		}

	case *pg_query.Node_JoinExpr:
		if n.JoinExpr != nil {
			t.walkForDependencies(n.JoinExpr.Larg, knownCTEs, deps, seen)
			t.walkForDependencies(n.JoinExpr.Rarg, knownCTEs, deps, seen)
			t.walkForDependencies(n.JoinExpr.Quals, knownCTEs, deps, seen)
		}

	case *pg_query.Node_SubLink:
		if n.SubLink != nil {
			t.walkForDependencies(n.SubLink.Subselect, knownCTEs, deps, seen)
		}

	case *pg_query.Node_RangeSubselect:
		if n.RangeSubselect != nil {
			t.walkForDependencies(n.RangeSubselect.Subquery, knownCTEs, deps, seen)
		}

	case *pg_query.Node_FuncCall:
		if n.FuncCall != nil {
			for _, arg := range n.FuncCall.Args {
				t.walkForDependencies(arg, knownCTEs, deps, seen)
			}
		}

	case *pg_query.Node_AExpr:
		if n.AExpr != nil {
			t.walkForDependencies(n.AExpr.Lexpr, knownCTEs, deps, seen)
			t.walkForDependencies(n.AExpr.Rexpr, knownCTEs, deps, seen)
		}

	case *pg_query.Node_BoolExpr:
		if n.BoolExpr != nil {
			for _, arg := range n.BoolExpr.Args {
				t.walkForDependencies(arg, knownCTEs, deps, seen)
			}
		}

	case *pg_query.Node_ResTarget:
		if n.ResTarget != nil {
			t.walkForDependencies(n.ResTarget.Val, knownCTEs, deps, seen)
		}

	case *pg_query.Node_TypeCast:
		if n.TypeCast != nil {
			t.walkForDependencies(n.TypeCast.Arg, knownCTEs, deps, seen)
		}

	case *pg_query.Node_CaseExpr:
		if n.CaseExpr != nil {
			t.walkForDependencies(n.CaseExpr.Arg, knownCTEs, deps, seen)
			for _, when := range n.CaseExpr.Args {
				t.walkForDependencies(when, knownCTEs, deps, seen)
			}
			t.walkForDependencies(n.CaseExpr.Defresult, knownCTEs, deps, seen)
		}

	case *pg_query.Node_CaseWhen:
		if n.CaseWhen != nil {
			t.walkForDependencies(n.CaseWhen.Expr, knownCTEs, deps, seen)
			t.walkForDependencies(n.CaseWhen.Result, knownCTEs, deps, seen)
		}
	}
}

// rewriteResult holds separated main and cleanup statements
type rewriteResult struct {
	statements []string // Setup + final query
	cleanup    []string // DROP tables + COMMIT
}

func (t *WritableCTETransform) rewriteWritableCTE(
	stmt *pg_query.RawStmt,
	ctes []*cteInfo, // Already in declaration order
) (*rewriteResult, error) {
	result := &rewriteResult{}
	tempTableNames := make(map[string]string)

	// 1. Start transaction (if not already in one - handled by caller)
	result.statements = append(result.statements, "BEGIN")

	// 2. Process CTEs IN DECLARATION ORDER (critical for interleaved dependencies)
	for _, cte := range ctes {
		// Generate safe, unique temp table name
		tempName := t.generateTempTableName(cte.name)
		tempTableNames[cte.name] = tempName

		// Quote identifier for SQL safety
		quotedTempName := quoteIdentifier(tempName)

		if cte.isWrite {
			// WRITABLE CTE: capture RETURNING output, then execute DML

			// 2a. Generate SELECT that captures what RETURNING would produce
			returningSelect, err := t.generateReturningSelect(cte.node, tempTableNames)
			if err != nil {
				return nil, err
			}
			result.statements = append(result.statements,
				fmt.Sprintf("CREATE TEMP TABLE %s AS (%s)", quotedTempName, returningSelect))

			// 2b. Execute the actual DML (with CTE refs rewritten to temp tables)
			dmlSQL, err := t.deparseAndRewriteRefs(cte.node.Ctequery, tempTableNames)
			if err != nil {
				return nil, err
			}
			// Strip RETURNING clause since we pre-captured the output
			dmlSQL = t.stripReturningClause(dmlSQL)
			result.statements = append(result.statements, dmlSQL)

		} else {
			// READ-ONLY CTE: just materialize as temp table

			// Deparse and rewrite any CTE references to temp table names
			cteSQL, err := t.deparseAndRewriteRefs(cte.node.Ctequery, tempTableNames)
			if err != nil {
				return nil, fmt.Errorf("failed to deparse CTE %s: %w", cte.name, err)
			}

			result.statements = append(result.statements,
				fmt.Sprintf("CREATE TEMP TABLE %s AS (%s)", quotedTempName, cteSQL))
		}
	}

	// 3. Final statement goes LAST in main statements (will be executed and streamed)
	finalSQL, err := t.deparseFinalStatement(stmt, tempTableNames)
	if err != nil {
		return nil, err
	}
	result.statements = append(result.statements, finalSQL)

	// 4. CLEANUP STATEMENTS (executed after cursor obtained, before streaming)
	// Drop temp tables in reverse order
	for i := len(ctes) - 1; i >= 0; i-- {
		tempName := tempTableNames[ctes[i].name]
		quotedName := quoteIdentifier(tempName)
		result.cleanup = append(result.cleanup, fmt.Sprintf("DROP TABLE IF EXISTS %s", quotedName))
	}

	// 5. Commit goes in cleanup (after cursor obtained)
	result.cleanup = append(result.cleanup, "COMMIT")

	return result, nil
}

// generateReturningSelect converts a writable CTE into a SELECT that captures
// what the RETURNING clause would produce.
//
// For Airbyte's pattern, the RETURNING clause is typically used to identify
// which rows were affected, not to get the new values. We capture the rows
// that match the UPDATE/DELETE criteria BEFORE the modification.
func (t *WritableCTETransform) generateReturningSelect(
	cte *pg_query.CommonTableExpr,
	tempTableNames map[string]string,
) (string, error) {
	query := cte.Ctequery

	switch n := query.Node.(type) {
	case *pg_query.Node_UpdateStmt:
		return t.updateToSelect(n.UpdateStmt, tempTableNames)
	case *pg_query.Node_DeleteStmt:
		return t.deleteToSelect(n.DeleteStmt, tempTableNames)
	case *pg_query.Node_InsertStmt:
		return t.insertToSelect(n.InsertStmt, tempTableNames)
	default:
		return "", fmt.Errorf("unexpected CTE query type: %T", query.Node)
	}
}

// updateToSelect converts UPDATE...FROM...WHERE...RETURNING to equivalent SELECT
func (t *WritableCTETransform) updateToSelect(
	update *pg_query.UpdateStmt,
	tempTableNames map[string]string,
) (string, error) {
	// Build: SELECT <returning_cols> FROM <target> [JOIN <from>] WHERE <where>
	//
	// For RETURNING *, select all columns from target table
	// For specific RETURNING columns, select those

	if update == nil || update.Relation == nil {
		return "", fmt.Errorf("invalid UPDATE statement")
	}

	// Get target table name (alias not used in FROM clause generation)

	// Build SELECT columns from RETURNING clause
	selectCols := "*"
	if len(update.ReturningList) > 0 {
		// Deparse the RETURNING list
		var cols []string
		for _, ret := range update.ReturningList {
			colSQL, err := pg_query.Deparse(&pg_query.ParseResult{
				Stmts: []*pg_query.RawStmt{{
					Stmt: &pg_query.Node{
						Node: &pg_query.Node_SelectStmt{
							SelectStmt: &pg_query.SelectStmt{
								TargetList: []*pg_query.Node{ret},
							},
						},
					},
				}},
			})
			if err != nil {
				cols = append(cols, "*")
			} else {
				// Extract just the SELECT list part
				colSQL = strings.TrimPrefix(colSQL, "SELECT ")
				cols = append(cols, colSQL)
			}
		}
		selectCols = strings.Join(cols, ", ")
	}

	// Build FROM clause
	var fromParts []string
	fromParts = append(fromParts, t.buildTableRef(update.Relation, tempTableNames))

	// Add FROM clause tables as JOINs
	for _, from := range update.FromClause {
		fromSQL, err := t.deparseNode(from, tempTableNames)
		if err != nil {
			return "", err
		}
		fromParts = append(fromParts, fromSQL)
	}

	// Build WHERE clause
	whereSQL := ""
	if update.WhereClause != nil {
		whereDeparsed, err := t.deparseNode(update.WhereClause, tempTableNames)
		if err != nil {
			return "", err
		}
		whereSQL = " WHERE " + whereDeparsed
	}

	selectSQL := fmt.Sprintf("SELECT %s FROM %s%s",
		selectCols,
		strings.Join(fromParts, ", "),
		whereSQL)

	return selectSQL, nil
}

// deleteToSelect converts DELETE...WHERE...RETURNING to equivalent SELECT
func (t *WritableCTETransform) deleteToSelect(
	del *pg_query.DeleteStmt,
	tempTableNames map[string]string,
) (string, error) {
	// Simple: SELECT * FROM target WHERE condition

	if del == nil || del.Relation == nil {
		return "", fmt.Errorf("invalid DELETE statement")
	}

	// Build SELECT columns from RETURNING clause
	selectCols := "*"
	if len(del.ReturningList) > 0 {
		var cols []string
		for _, ret := range del.ReturningList {
			colSQL, err := pg_query.Deparse(&pg_query.ParseResult{
				Stmts: []*pg_query.RawStmt{{
					Stmt: &pg_query.Node{
						Node: &pg_query.Node_SelectStmt{
							SelectStmt: &pg_query.SelectStmt{
								TargetList: []*pg_query.Node{ret},
							},
						},
					},
				}},
			})
			if err != nil {
				cols = append(cols, "*")
			} else {
				colSQL = strings.TrimPrefix(colSQL, "SELECT ")
				cols = append(cols, colSQL)
			}
		}
		selectCols = strings.Join(cols, ", ")
	}

	// Build FROM clause
	var fromParts []string
	fromParts = append(fromParts, t.buildTableRef(del.Relation, tempTableNames))

	// Add USING clause tables
	for _, using := range del.UsingClause {
		usingSQL, err := t.deparseNode(using, tempTableNames)
		if err != nil {
			return "", err
		}
		fromParts = append(fromParts, usingSQL)
	}

	// Build WHERE clause
	whereSQL := ""
	if del.WhereClause != nil {
		whereDeparsed, err := t.deparseNode(del.WhereClause, tempTableNames)
		if err != nil {
			return "", err
		}
		whereSQL = " WHERE " + whereDeparsed
	}

	selectSQL := fmt.Sprintf("SELECT %s FROM %s%s",
		selectCols,
		strings.Join(fromParts, ", "),
		whereSQL)

	return selectSQL, nil
}

// insertToSelect converts INSERT...RETURNING to a SELECT that captures inserted rows
// For INSERT...SELECT, we just return the SELECT.
// For INSERT...VALUES, we return a SELECT with the values.
func (t *WritableCTETransform) insertToSelect(
	ins *pg_query.InsertStmt,
	tempTableNames map[string]string,
) (string, error) {
	if ins == nil {
		return "", fmt.Errorf("invalid INSERT statement")
	}

	// For INSERT ... SELECT, the RETURNING would return the inserted rows
	// which are the rows from the SELECT. Just rewrite and return the SELECT.
	if ins.SelectStmt != nil {
		selectSQL, err := t.deparseNode(ins.SelectStmt, tempTableNames)
		if err != nil {
			return "", err
		}
		return selectSQL, nil
	}

	// For INSERT VALUES, create an empty result (no rows to pre-capture)
	// since we can't know what will be inserted until it happens
	return "SELECT * FROM (SELECT 1) AS _empty WHERE false", nil
}

// buildTableRef builds a table reference string, rewriting CTE names to temp tables
func (t *WritableCTETransform) buildTableRef(rv *pg_query.RangeVar, tempTableNames map[string]string) string {
	if rv == nil {
		return ""
	}

	tableName := rv.Relname

	// Check if this is a CTE reference that needs rewriting
	if rv.Catalogname == "" && rv.Schemaname == "" {
		if tempName, ok := tempTableNames[tableName]; ok {
			tableName = quoteIdentifier(tempName)
		} else {
			tableName = quoteIdentifier(tableName)
		}
	} else {
		// Build qualified name
		if rv.Catalogname != "" {
			tableName = quoteIdentifier(rv.Catalogname) + "." + quoteIdentifier(rv.Schemaname) + "." + quoteIdentifier(rv.Relname)
		} else if rv.Schemaname != "" {
			tableName = quoteIdentifier(rv.Schemaname) + "." + quoteIdentifier(rv.Relname)
		}
	}

	if rv.Alias != nil && rv.Alias.Aliasname != "" {
		tableName = tableName + " AS " + quoteIdentifier(rv.Alias.Aliasname)
	}

	return tableName
}

// deparseNode deparses a single node with CTE references rewritten to temp table names.
// For expression nodes (like WHERE clauses), it wraps them in a SELECT to deparse,
// then extracts just the expression part.
func (t *WritableCTETransform) deparseNode(node *pg_query.Node, tempTableNames map[string]string) (string, error) {
	if node == nil {
		return "", nil
	}

	// Rewrite CTE references to temp table names
	t.rewriteRangeVars(node, tempTableNames)

	// Check if this is a statement-level node that can be deparsed directly
	switch node.Node.(type) {
	case *pg_query.Node_SelectStmt, *pg_query.Node_InsertStmt,
		*pg_query.Node_UpdateStmt, *pg_query.Node_DeleteStmt:
		// Statement-level nodes can be deparsed directly
		return pg_query.Deparse(&pg_query.ParseResult{
			Stmts: []*pg_query.RawStmt{{
				Stmt: node,
			}},
		})
	case *pg_query.Node_RangeVar:
		// RangeVar needs to be wrapped in a SELECT FROM clause
		rv := node.GetRangeVar()
		return t.buildTableRef(rv, tempTableNames), nil
	default:
		// Expression nodes need to be wrapped in SELECT to deparse
		return t.deparseExpression(node)
	}
}

// deparseExpression deparses an expression node by wrapping it in a SELECT statement
// and then extracting just the expression part.
func (t *WritableCTETransform) deparseExpression(node *pg_query.Node) (string, error) {
	// Wrap the expression in SELECT <expr>
	selectStmt := &pg_query.SelectStmt{
		TargetList: []*pg_query.Node{
			{
				Node: &pg_query.Node_ResTarget{
					ResTarget: &pg_query.ResTarget{
						Val: node,
					},
				},
			},
		},
	}

	result, err := pg_query.Deparse(&pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{
			Stmt: &pg_query.Node{
				Node: &pg_query.Node_SelectStmt{
					SelectStmt: selectStmt,
				},
			},
		}},
	})
	if err != nil {
		return "", err
	}

	// Strip the "SELECT " prefix to get just the expression
	return strings.TrimPrefix(result, "SELECT "), nil
}

// deparseAndRewriteRefs deparses a node with CTE references rewritten
func (t *WritableCTETransform) deparseAndRewriteRefs(node *pg_query.Node, tempTableNames map[string]string) (string, error) {
	// Walk and rewrite RangeVar references
	t.rewriteRangeVars(node, tempTableNames)

	// Deparse
	return pg_query.Deparse(&pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{
			Stmt: node,
		}},
	})
}

// rewriteRangeVars walks the AST and rewrites unqualified table references
// that match CTE names to use temp table names instead
func (t *WritableCTETransform) rewriteRangeVars(node *pg_query.Node, tempTableNames map[string]string) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		if n.RangeVar != nil {
			// Only rewrite unqualified references
			if n.RangeVar.Catalogname == "" && n.RangeVar.Schemaname == "" {
				if tempName, ok := tempTableNames[n.RangeVar.Relname]; ok {
					n.RangeVar.Relname = tempName
				}
			}
		}

	case *pg_query.Node_ColumnRef:
		if n.ColumnRef != nil && len(n.ColumnRef.Fields) >= 2 {
			// Column reference with table qualifier (e.g., deduped_source.id)
			// Check if first field is a string matching a CTE name
			if first := n.ColumnRef.Fields[0].GetString_(); first != nil {
				if tempName, ok := tempTableNames[first.Sval]; ok {
					first.Sval = tempName
				}
			}
		}

	case *pg_query.Node_SelectStmt:
		if n.SelectStmt != nil {
			for _, from := range n.SelectStmt.FromClause {
				t.rewriteRangeVars(from, tempTableNames)
			}
			t.rewriteRangeVars(n.SelectStmt.WhereClause, tempTableNames)
			for _, target := range n.SelectStmt.TargetList {
				t.rewriteRangeVars(target, tempTableNames)
			}
			for _, group := range n.SelectStmt.GroupClause {
				t.rewriteRangeVars(group, tempTableNames)
			}
			t.rewriteRangeVars(n.SelectStmt.HavingClause, tempTableNames)
			for _, sort := range n.SelectStmt.SortClause {
				t.rewriteRangeVars(sort, tempTableNames)
			}
			t.rewriteRangeVars(n.SelectStmt.LimitCount, tempTableNames)
			t.rewriteRangeVars(n.SelectStmt.LimitOffset, tempTableNames)
			if n.SelectStmt.Larg != nil {
				t.rewriteRangeVars(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: n.SelectStmt.Larg}}, tempTableNames)
			}
			if n.SelectStmt.Rarg != nil {
				t.rewriteRangeVars(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: n.SelectStmt.Rarg}}, tempTableNames)
			}
		}

	case *pg_query.Node_UpdateStmt:
		if n.UpdateStmt != nil {
			for _, from := range n.UpdateStmt.FromClause {
				t.rewriteRangeVars(from, tempTableNames)
			}
			t.rewriteRangeVars(n.UpdateStmt.WhereClause, tempTableNames)
			for _, target := range n.UpdateStmt.TargetList {
				t.rewriteRangeVars(target, tempTableNames)
			}
		}

	case *pg_query.Node_DeleteStmt:
		if n.DeleteStmt != nil {
			t.rewriteRangeVars(n.DeleteStmt.WhereClause, tempTableNames)
			for _, using := range n.DeleteStmt.UsingClause {
				t.rewriteRangeVars(using, tempTableNames)
			}
		}

	case *pg_query.Node_InsertStmt:
		if n.InsertStmt != nil {
			t.rewriteRangeVars(n.InsertStmt.SelectStmt, tempTableNames)
		}

	case *pg_query.Node_JoinExpr:
		if n.JoinExpr != nil {
			t.rewriteRangeVars(n.JoinExpr.Larg, tempTableNames)
			t.rewriteRangeVars(n.JoinExpr.Rarg, tempTableNames)
			t.rewriteRangeVars(n.JoinExpr.Quals, tempTableNames)
		}

	case *pg_query.Node_SubLink:
		if n.SubLink != nil {
			t.rewriteRangeVars(n.SubLink.Subselect, tempTableNames)
		}

	case *pg_query.Node_RangeSubselect:
		if n.RangeSubselect != nil {
			t.rewriteRangeVars(n.RangeSubselect.Subquery, tempTableNames)
		}

	case *pg_query.Node_FuncCall:
		if n.FuncCall != nil {
			for _, arg := range n.FuncCall.Args {
				t.rewriteRangeVars(arg, tempTableNames)
			}
		}

	case *pg_query.Node_AExpr:
		if n.AExpr != nil {
			t.rewriteRangeVars(n.AExpr.Lexpr, tempTableNames)
			t.rewriteRangeVars(n.AExpr.Rexpr, tempTableNames)
		}

	case *pg_query.Node_BoolExpr:
		if n.BoolExpr != nil {
			for _, arg := range n.BoolExpr.Args {
				t.rewriteRangeVars(arg, tempTableNames)
			}
		}

	case *pg_query.Node_ResTarget:
		if n.ResTarget != nil {
			t.rewriteRangeVars(n.ResTarget.Val, tempTableNames)
		}

	case *pg_query.Node_TypeCast:
		if n.TypeCast != nil {
			t.rewriteRangeVars(n.TypeCast.Arg, tempTableNames)
		}

	case *pg_query.Node_CaseExpr:
		if n.CaseExpr != nil {
			t.rewriteRangeVars(n.CaseExpr.Arg, tempTableNames)
			for _, when := range n.CaseExpr.Args {
				t.rewriteRangeVars(when, tempTableNames)
			}
			t.rewriteRangeVars(n.CaseExpr.Defresult, tempTableNames)
		}

	case *pg_query.Node_CaseWhen:
		if n.CaseWhen != nil {
			t.rewriteRangeVars(n.CaseWhen.Expr, tempTableNames)
			t.rewriteRangeVars(n.CaseWhen.Result, tempTableNames)
		}

	case *pg_query.Node_CoalesceExpr:
		if n.CoalesceExpr != nil {
			for _, arg := range n.CoalesceExpr.Args {
				t.rewriteRangeVars(arg, tempTableNames)
			}
		}

	case *pg_query.Node_NullTest:
		if n.NullTest != nil {
			t.rewriteRangeVars(n.NullTest.Arg, tempTableNames)
		}

	case *pg_query.Node_SortBy:
		if n.SortBy != nil {
			t.rewriteRangeVars(n.SortBy.Node, tempTableNames)
		}
	}
}

// deparseFinalStatement deparses the final statement without the WITH clause,
// rewriting CTE references to temp table names
func (t *WritableCTETransform) deparseFinalStatement(stmt *pg_query.RawStmt, tempTableNames map[string]string) (string, error) {
	if stmt.Stmt == nil {
		return "", fmt.Errorf("empty statement")
	}

	// Remove the WITH clause and rewrite references
	switch n := stmt.Stmt.Node.(type) {
	case *pg_query.Node_SelectStmt:
		if n.SelectStmt != nil {
			// Remove WITH clause
			n.SelectStmt.WithClause = nil
			// Rewrite CTE references
			t.rewriteRangeVars(stmt.Stmt, tempTableNames)
		}
	case *pg_query.Node_InsertStmt:
		if n.InsertStmt != nil {
			n.InsertStmt.WithClause = nil
			t.rewriteRangeVars(stmt.Stmt, tempTableNames)
		}
	case *pg_query.Node_UpdateStmt:
		if n.UpdateStmt != nil {
			n.UpdateStmt.WithClause = nil
			t.rewriteRangeVars(stmt.Stmt, tempTableNames)
		}
	case *pg_query.Node_DeleteStmt:
		if n.DeleteStmt != nil {
			n.DeleteStmt.WithClause = nil
			t.rewriteRangeVars(stmt.Stmt, tempTableNames)
		}
	}

	return pg_query.Deparse(&pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{stmt},
	})
}

// stripReturningClause removes the RETURNING clause from a DML statement
func (t *WritableCTETransform) stripReturningClause(sql string) string {
	// Use simple string manipulation - RETURNING should be at the end
	upper := strings.ToUpper(sql)
	idx := strings.LastIndex(upper, " RETURNING ")
	if idx == -1 {
		return sql
	}
	return sql[:idx]
}

// generateTempTableName creates a safe, unique temp table name
func (t *WritableCTETransform) generateTempTableName(cteName string) string {
	// Sanitize: remove non-alphanumeric chars, lowercase
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return r + 32 // lowercase
		}
		return '_' // replace special chars with underscore
	}, cteName)

	// Generate unique suffix (8 hex chars from random)
	suffix := fmt.Sprintf("%08x", rand.Uint32())

	// Build name with length check
	prefix := "_cte_"
	maxNameLen := maxIdentifierLength - len(prefix) - len(suffix) - 1 // -1 for underscore
	if len(safe) > maxNameLen {
		safe = safe[:maxNameLen]
	}

	return fmt.Sprintf("%s%s_%s", prefix, safe, suffix)
}

// quoteIdentifier quotes an identifier for safe use in SQL
func quoteIdentifier(name string) string {
	// Double any existing quotes and wrap in quotes
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

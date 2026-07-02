package transform

import (
	"fmt"
	"math/rand/v2"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// OnConflictTransform handles PostgreSQL ON CONFLICT (upsert) syntax.
// PostgreSQL: INSERT ... ON CONFLICT (columns) DO UPDATE SET ...
// PostgreSQL: INSERT ... ON CONFLICT (columns) DO NOTHING
// DuckDB: INSERT OR REPLACE INTO ... (for simple cases)
// DuckDB: INSERT OR IGNORE INTO ... (for DO NOTHING)
//
// Note: DuckDB's ON CONFLICT support has evolved. As of DuckDB 0.9+,
// it supports ON CONFLICT DO NOTHING and ON CONFLICT DO UPDATE.
// This transform handles cases where the syntax might differ.
//
// In DuckLake mode, PRIMARY KEY and UNIQUE constraints are stripped,
// so ON CONFLICT clauses will fail with "columns not referenced by constraint".
// Instead of stripping ON CONFLICT, we convert the INSERT to a MERGE statement
// which DuckLake supports for upserts.
type OnConflictTransform struct {
	DuckLakeMode bool
}

type onConflictMergeRewrite struct {
	merge           *pg_query.MergeStmt
	sourceSelect    *pg_query.SelectStmt
	targetRelation  *pg_query.RangeVar
	insertColumns   []string
	conflictColumns []string
	action          pg_query.OnConflictAction
}

func NewOnConflictTransform() *OnConflictTransform {
	return &OnConflictTransform{DuckLakeMode: false}
}

func NewOnConflictTransformWithConfig(duckLakeMode bool) *OnConflictTransform {
	return &OnConflictTransform{DuckLakeMode: duckLakeMode}
}

func (t *OnConflictTransform) Name() string {
	return "onconflict"
}

func (t *OnConflictTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false

	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}

		if insert := stmt.Stmt.GetInsertStmt(); insert != nil {
			// On a constraint-less backend (MERGE rewrite enabled), ON CONFLICT ON
			// CONSTRAINT <name> cannot be honored: there is no named constraint to
			// infer the conflict target from. Reject it with a clean PostgreSQL
			// error rather than letting it fail opaquely at DuckDB.
			if t.DuckLakeMode && insert.OnConflictClause != nil {
				if unsupported := t.validateDuckLakeConflictTarget(insert.OnConflictClause); unsupported != nil {
					result.Error = unsupported
					return false, nil
				}
				if unsupported := t.validateDuckLakeInsertColumns(insert); unsupported != nil {
					result.Error = unsupported
					return false, nil
				}
			}

			if rewrite := t.transformInsertToMerge(insert); rewrite != nil {
				// Replace the INSERT statement with MERGE
				stmt.Stmt = &pg_query.Node{
					Node: &pg_query.Node_MergeStmt{MergeStmt: rewrite.merge},
				}
				changed = true

				if statements, cleanup, err := t.buildGuardedMergeStatements(rewrite); err != nil {
					return false, err
				} else if len(statements) > 0 {
					result.Statements = statements
					result.CleanupStatements = cleanup
				}
			} else if t.transformInsert(insert) {
				changed = true
			}
		}
	}

	return changed, nil
}

func (t *OnConflictTransform) validateDuckLakeConflictTarget(occ *pg_query.OnConflictClause) error {
	if occ == nil {
		return nil
	}
	if occ.Infer == nil {
		return NewFeatureNotSupported(
			"ON CONFLICT without a conflict target is not supported: this catalog does not enforce unique constraints")
	}
	if strings.TrimSpace(occ.Infer.Conname) != "" {
		return NewFeatureNotSupported(
			"ON CONFLICT ON CONSTRAINT is not supported: this catalog does not enforce named constraints")
	}
	if occ.Infer.WhereClause != nil {
		return NewFeatureNotSupported(
			"ON CONFLICT partial index predicates are not supported: this catalog does not enforce partial unique indexes")
	}
	for _, elem := range occ.Infer.IndexElems {
		indexElem := elem.GetIndexElem()
		if indexElem == nil || strings.TrimSpace(indexElem.Name) == "" {
			return NewFeatureNotSupported(
				"ON CONFLICT expression targets are not supported: use column names")
		}
	}
	return nil
}

func (t *OnConflictTransform) validateDuckLakeInsertColumns(insert *pg_query.InsertStmt) error {
	if insert == nil || insert.OnConflictClause == nil {
		return nil
	}
	if len(insert.Cols) == 0 {
		return NewFeatureNotSupported(
			"ON CONFLICT without an explicit insert column list is not supported")
	}
	insertColumns := make(map[string]struct{}, len(insert.Cols))
	for _, col := range insert.Cols {
		rt := col.GetResTarget()
		if rt == nil || strings.TrimSpace(rt.Name) == "" {
			return NewFeatureNotSupported(
				"ON CONFLICT without an explicit insert column list is not supported")
		}
		insertColumns[rt.Name] = struct{}{}
	}

	conflictColumns := t.conflictColumnNames(insert.OnConflictClause.Infer.IndexElems)
	for _, conflictColumn := range conflictColumns {
		if _, ok := insertColumns[conflictColumn]; !ok {
			return NewFeatureNotSupported(
				"ON CONFLICT conflict columns must be present in the insert column list")
		}
	}
	return nil
}

// transformInsertToMerge converts INSERT ... ON CONFLICT to MERGE for DuckLake mode
func (t *OnConflictTransform) transformInsertToMerge(insert *pg_query.InsertStmt) *onConflictMergeRewrite {
	if !t.DuckLakeMode {
		return nil
	}

	if insert == nil || insert.OnConflictClause == nil {
		return nil
	}

	occ := insert.OnConflictClause

	// Need conflict columns to build the join condition
	if occ.Infer == nil || len(occ.Infer.IndexElems) == 0 {
		return nil
	}
	conflictColumns := t.conflictColumnNames(occ.Infer.IndexElems)

	// Get column names from INSERT
	colNames := make([]string, len(insert.Cols))
	for i, col := range insert.Cols {
		if rt := col.GetResTarget(); rt != nil {
			colNames[i] = rt.Name
		}
	}

	// Get the SelectStmt from INSERT
	selectStmt := insert.SelectStmt.GetSelectStmt()
	if selectStmt == nil {
		return nil
	}

	// Build source SELECT - handle both VALUES and SELECT ... FROM cases
	var sourceSelect *pg_query.SelectStmt
	if len(selectStmt.ValuesLists) > 0 {
		// INSERT ... VALUES (...) ON CONFLICT - build SELECT from values
		sourceSelect = t.buildSourceSelect(colNames, selectStmt.ValuesLists)
	} else if len(selectStmt.FromClause) > 0 || len(selectStmt.TargetList) > 0 {
		// INSERT ... SELECT ... FROM ... ON CONFLICT - use SELECT directly
		// The SELECT already has the right columns, just use it as the source
		sourceSelect = selectStmt
	}

	if sourceSelect == nil {
		return nil
	}

	// Build source relation as a subquery with alias "excluded"
	sourceRelation := &pg_query.Node{
		Node: &pg_query.Node_RangeSubselect{
			RangeSubselect: &pg_query.RangeSubselect{
				Subquery: &pg_query.Node{
					Node: &pg_query.Node_SelectStmt{SelectStmt: sourceSelect},
				},
				Alias: &pg_query.Alias{Aliasname: "excluded"},
			},
		},
	}

	// Build join condition from conflict columns
	joinCondition := t.buildJoinCondition(occ.Infer.IndexElems, insert.Relation.Relname)

	// Build MERGE WHEN clauses
	var whenClauses []*pg_query.Node

	// If DO UPDATE, add WHEN MATCHED THEN UPDATE
	if occ.Action == pg_query.OnConflictAction_ONCONFLICT_UPDATE {
		updateClause := t.buildUpdateClause(occ.TargetList, occ.WhereClause)
		whenClauses = append(whenClauses, &pg_query.Node{
			Node: &pg_query.Node_MergeWhenClause{MergeWhenClause: updateClause},
		})
	}
	// For DO NOTHING, we skip WHEN MATCHED (no action on match)

	// Always add WHEN NOT MATCHED THEN INSERT
	insertClause := t.buildInsertClause(colNames)
	whenClauses = append(whenClauses, &pg_query.Node{
		Node: &pg_query.Node_MergeWhenClause{MergeWhenClause: insertClause},
	})

	merge := &pg_query.MergeStmt{
		Relation:         insert.Relation,
		SourceRelation:   sourceRelation,
		JoinCondition:    joinCondition,
		MergeWhenClauses: whenClauses,
	}

	return &onConflictMergeRewrite{
		merge:           merge,
		sourceSelect:    sourceSelect,
		targetRelation:  insert.Relation,
		insertColumns:   colNames,
		conflictColumns: conflictColumns,
		action:          occ.Action,
	}
}

func (t *OnConflictTransform) conflictColumnNames(indexElems []*pg_query.Node) []string {
	cols := make([]string, 0, len(indexElems))
	for _, elem := range indexElems {
		indexElem := elem.GetIndexElem()
		if indexElem == nil || strings.TrimSpace(indexElem.Name) == "" {
			return nil
		}
		cols = append(cols, indexElem.Name)
	}
	return cols
}

func (t *OnConflictTransform) buildGuardedMergeStatements(rewrite *onConflictMergeRewrite) ([]string, []string, error) {
	if rewrite == nil || rewrite.merge == nil || rewrite.sourceSelect == nil || rewrite.targetRelation == nil {
		return nil, nil, nil
	}
	if len(rewrite.conflictColumns) == 0 {
		return nil, nil, nil
	}

	sourceSQL, err := deparseStatement(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: rewrite.sourceSelect}})
	if err != nil {
		return nil, nil, err
	}
	sourceTableName := t.generateOnConflictSourceTableName(rewrite.targetRelation.Relname)
	sourceRelation := quoteIdent(sourceTableName)
	sourceFrom := sourceRelation + " AS duckgres_on_conflict_source"

	mergeSQL, err := deparseMergeWithTempSource(rewrite.merge, sourceTableName)
	if err != nil {
		return nil, nil, err
	}

	statements := []string{
		"BEGIN",
		t.buildCreateSourceTableStatement(sourceRelation, sourceSQL, rewrite),
	}
	if rewrite.action == pg_query.OnConflictAction_ONCONFLICT_UPDATE {
		statements = append(statements, t.buildSourceDuplicateGuard(sourceFrom, rewrite.conflictColumns))
		statements = append(statements, t.buildTargetDuplicateGuard(sourceFrom, rewrite.targetRelation, rewrite.conflictColumns))
	}
	statements = append(statements,
		mergeSQL,
	)
	cleanup := []string{
		"DROP TABLE IF EXISTS " + sourceRelation,
		"COMMIT",
	}
	return statements, cleanup, nil
}

func (t *OnConflictTransform) buildCreateSourceTableStatement(sourceRelation string, sourceSQL string, rewrite *onConflictMergeRewrite) string {
	columnList := quotedColumnList(rewrite.insertColumns)
	if rewrite.action != pg_query.OnConflictAction_ONCONFLICT_NOTHING {
		return "CREATE TEMPORARY TABLE " + sourceRelation + " " + columnList + " AS " + sourceSQL
	}

	return "CREATE TEMPORARY TABLE " + sourceRelation + " " + columnList + " AS " +
		t.buildDoNothingSourceSQL(sourceSQL, rewrite.insertColumns, rewrite.conflictColumns)
}

func (t *OnConflictTransform) buildDoNothingSourceSQL(sourceSQL string, insertColumns []string, conflictColumns []string) string {
	rawAlias := "duckgres_on_conflict_raw"
	rankedAlias := "duckgres_on_conflict_ranked"
	rankColumn := "_duckgres_on_conflict_rank"

	insertRefs := qualifiedColumnRefs(rankedAlias, insertColumns)
	conflictRefs := qualifiedColumnRefs(rawAlias, conflictColumns)
	preservePredicates := make([]string, 0, len(conflictColumns)+1)
	for _, ref := range qualifiedColumnRefs(rankedAlias, conflictColumns) {
		preservePredicates = append(preservePredicates, ref+" IS NULL")
	}
	preservePredicates = append(preservePredicates, rankedAlias+"."+quoteIdent(rankColumn)+" = 1")

	return "SELECT " + strings.Join(insertRefs, ", ") + " FROM (" +
		"SELECT " + rawAlias + ".*, row_number() OVER (PARTITION BY " + strings.Join(conflictRefs, ", ") + ") AS " + quoteIdent(rankColumn) + " " +
		"FROM (" + sourceSQL + ") AS " + rawAlias + " " + quotedColumnList(insertColumns) +
		") AS " + rankedAlias + " " +
		"WHERE " + strings.Join(preservePredicates, " OR ")
}

func deparseStatement(stmt *pg_query.Node) (string, error) {
	sql, err := pg_query.Deparse(&pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{Stmt: stmt}},
	})
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSpace(sql), ";"), nil
}

func deparseMergeWithTempSource(merge *pg_query.MergeStmt, sourceTableName string) (string, error) {
	originalSourceRelation := merge.SourceRelation
	defer func() {
		merge.SourceRelation = originalSourceRelation
	}()
	merge.SourceRelation = &pg_query.Node{
		Node: &pg_query.Node_RangeVar{
			RangeVar: &pg_query.RangeVar{
				Relname: sourceTableName,
				Alias:   &pg_query.Alias{Aliasname: "excluded"},
			},
		},
	}
	sql, err := deparseStatement(&pg_query.Node{Node: &pg_query.Node_MergeStmt{MergeStmt: merge}})
	return sql, err
}

func (t *OnConflictTransform) buildSourceDuplicateGuard(sourceFrom string, conflictColumns []string) string {
	sourceRefs := qualifiedColumnRefs("duckgres_on_conflict_source", conflictColumns)
	notNullPredicates := notNullPredicates(sourceRefs)

	return "SELECT CASE WHEN EXISTS (" +
		"SELECT 1 FROM " + sourceFrom + " " +
		"WHERE " + notNullPredicates + " " +
		"GROUP BY " + strings.Join(sourceRefs, ", ") + " " +
		"HAVING count(*) > 1 LIMIT 1" +
		") THEN error(" + quoteStringLiteral("ON CONFLICT source rows contain duplicate conflict keys") + ") ELSE NULL END"
}

func (t *OnConflictTransform) buildTargetDuplicateGuard(sourceFrom string, target *pg_query.RangeVar, conflictColumns []string) string {
	sourceRefs := qualifiedColumnRefs("duckgres_on_conflict_source", conflictColumns)
	keyRefs := qualifiedColumnRefs("duckgres_on_conflict_keys", conflictColumns)
	targetRefs := qualifiedColumnRefs("duckgres_on_conflict_target", conflictColumns)
	joinPredicates := make([]string, 0, len(conflictColumns))
	for i := range conflictColumns {
		joinPredicates = append(joinPredicates, targetRefs[i]+" = "+keyRefs[i])
	}

	return "SELECT CASE WHEN EXISTS (" +
		"SELECT 1 FROM " + formatRangeVar(target) + " AS duckgres_on_conflict_target " +
		"JOIN (" +
		"SELECT DISTINCT " + strings.Join(sourceRefs, ", ") + " " +
		"FROM " + sourceFrom + " " +
		"WHERE " + notNullPredicates(sourceRefs) +
		") AS duckgres_on_conflict_keys " +
		"ON " + strings.Join(joinPredicates, " AND ") + " " +
		"GROUP BY " + strings.Join(targetRefs, ", ") + " " +
		"HAVING count(*) > 1 LIMIT 1" +
		") THEN error(" + quoteStringLiteral("ON CONFLICT target contains duplicate conflict keys") + ") ELSE NULL END"
}

func (t *OnConflictTransform) generateOnConflictSourceTableName(targetName string) string {
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return r + 32
		}
		return '_'
	}, targetName)
	if safe == "" {
		safe = "source"
	}

	prefix := "_duckgres_on_conflict_source_"
	suffix := fmt.Sprintf("%08x", rand.Uint32())
	maxNameLen := maxIdentifierLength - len(prefix) - len(suffix) - 1
	if len(safe) > maxNameLen {
		safe = safe[:maxNameLen]
	}
	return fmt.Sprintf("%s%s_%s", prefix, safe, suffix)
}

func qualifiedColumnRefs(alias string, columns []string) []string {
	refs := make([]string, len(columns))
	for i, col := range columns {
		refs[i] = alias + "." + quoteIdent(col)
	}
	return refs
}

func quotedColumnList(columns []string) string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = quoteIdent(col)
	}
	return "(" + strings.Join(quoted, ", ") + ")"
}

func notNullPredicates(refs []string) string {
	predicates := make([]string, len(refs))
	for i, ref := range refs {
		predicates[i] = ref + " IS NOT NULL"
	}
	return strings.Join(predicates, " AND ")
}

func formatRangeVar(rv *pg_query.RangeVar) string {
	parts := make([]string, 0, 3)
	if rv.Catalogname != "" {
		parts = append(parts, quoteIdent(rv.Catalogname))
	}
	if rv.Schemaname != "" {
		parts = append(parts, quoteIdent(rv.Schemaname))
	}
	parts = append(parts, quoteIdent(rv.Relname))
	return strings.Join(parts, ".")
}

func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func quoteStringLiteral(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}

// buildSourceSelect creates a SELECT statement from VALUES for use as MERGE source
func (t *OnConflictTransform) buildSourceSelect(colNames []string, valuesLists []*pg_query.Node) *pg_query.SelectStmt {
	if len(valuesLists) == 0 {
		return nil
	}

	if len(valuesLists) == 1 {
		// Single row: SELECT val1 AS col1, val2 AS col2, ...
		valueList := valuesLists[0].GetList()
		if valueList == nil || len(valueList.Items) != len(colNames) {
			return nil
		}

		targetList := make([]*pg_query.Node, len(colNames))
		for i, colName := range colNames {
			targetList[i] = &pg_query.Node{
				Node: &pg_query.Node_ResTarget{
					ResTarget: &pg_query.ResTarget{
						Name: colName,
						Val:  valueList.Items[i],
					},
				},
			}
		}

		return &pg_query.SelectStmt{
			TargetList:  targetList,
			LimitOption: pg_query.LimitOption_LIMIT_OPTION_DEFAULT,
			Op:          pg_query.SetOperation_SETOP_NONE,
		}
	}

	// Multiple rows: Use UNION ALL of SELECT statements
	// First row
	firstList := valuesLists[0].GetList()
	if firstList == nil || len(firstList.Items) != len(colNames) {
		return nil
	}

	targetList := make([]*pg_query.Node, len(colNames))
	for i, colName := range colNames {
		targetList[i] = &pg_query.Node{
			Node: &pg_query.Node_ResTarget{
				ResTarget: &pg_query.ResTarget{
					Name: colName,
					Val:  firstList.Items[i],
				},
			},
		}
	}

	result := &pg_query.SelectStmt{
		TargetList:  targetList,
		LimitOption: pg_query.LimitOption_LIMIT_OPTION_DEFAULT,
		Op:          pg_query.SetOperation_SETOP_NONE,
	}

	// Add remaining rows as UNION ALL
	for i := 1; i < len(valuesLists); i++ {
		valueList := valuesLists[i].GetList()
		if valueList == nil || len(valueList.Items) != len(colNames) {
			continue
		}

		rightTargetList := make([]*pg_query.Node, len(colNames))
		for j, colName := range colNames {
			rightTargetList[j] = &pg_query.Node{
				Node: &pg_query.Node_ResTarget{
					ResTarget: &pg_query.ResTarget{
						Name: colName,
						Val:  valueList.Items[j],
					},
				},
			}
		}

		rightSelect := &pg_query.SelectStmt{
			TargetList:  rightTargetList,
			LimitOption: pg_query.LimitOption_LIMIT_OPTION_DEFAULT,
			Op:          pg_query.SetOperation_SETOP_NONE,
		}

		result = &pg_query.SelectStmt{
			Op:          pg_query.SetOperation_SETOP_UNION,
			All:         true,
			Larg:        result,
			Rarg:        rightSelect,
			LimitOption: pg_query.LimitOption_LIMIT_OPTION_DEFAULT,
		}
	}

	return result
}

// buildJoinCondition creates the ON condition for MERGE
func (t *OnConflictTransform) buildJoinCondition(indexElems []*pg_query.Node, tableName string) *pg_query.Node {
	if len(indexElems) == 0 {
		return nil
	}

	// Build equality conditions for each conflict column
	var conditions []*pg_query.Node
	for _, elem := range indexElems {
		indexElem := elem.GetIndexElem()
		if indexElem == nil {
			continue
		}
		colName := indexElem.Name

		// excluded.col = table.col
		condition := &pg_query.Node{
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
									{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "excluded"}}},
									{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: colName}}},
								},
							},
						},
					},
					Rexpr: &pg_query.Node{
						Node: &pg_query.Node_ColumnRef{
							ColumnRef: &pg_query.ColumnRef{
								Fields: []*pg_query.Node{
									{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: tableName}}},
									{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: colName}}},
								},
							},
						},
					},
				},
			},
		}
		conditions = append(conditions, condition)
	}

	if len(conditions) == 1 {
		return conditions[0]
	}

	// Multiple columns: combine with AND
	result := conditions[0]
	for i := 1; i < len(conditions); i++ {
		result = &pg_query.Node{
			Node: &pg_query.Node_BoolExpr{
				BoolExpr: &pg_query.BoolExpr{
					Boolop: pg_query.BoolExprType_AND_EXPR,
					Args:   []*pg_query.Node{result, conditions[i]},
				},
			},
		}
	}

	return result
}

// buildUpdateClause creates WHEN MATCHED THEN UPDATE clause
func (t *OnConflictTransform) buildUpdateClause(targetList []*pg_query.Node, whereClause *pg_query.Node) *pg_query.MergeWhenClause {
	clause := &pg_query.MergeWhenClause{
		MatchKind:   pg_query.MergeMatchKind_MERGE_WHEN_MATCHED,
		CommandType: pg_query.CmdType_CMD_UPDATE,
	}

	if len(targetList) > 0 {
		// Specific columns to update - SET col = val, ...
		clause.TargetList = targetList
	}
	// If targetList is empty, it's a full row update (UPDATE without SET)

	if whereClause != nil {
		clause.Condition = whereClause
	}

	return clause
}

// buildInsertClause creates WHEN NOT MATCHED THEN INSERT clause
func (t *OnConflictTransform) buildInsertClause(colNames []string) *pg_query.MergeWhenClause {
	// Build target list (column names)
	targetList := make([]*pg_query.Node, len(colNames))
	for i, colName := range colNames {
		targetList[i] = &pg_query.Node{
			Node: &pg_query.Node_ResTarget{
				ResTarget: &pg_query.ResTarget{
					Name: colName,
				},
			},
		}
	}

	// Build values list (references to excluded.col)
	values := make([]*pg_query.Node, len(colNames))
	for i, colName := range colNames {
		values[i] = &pg_query.Node{
			Node: &pg_query.Node_ColumnRef{
				ColumnRef: &pg_query.ColumnRef{
					Fields: []*pg_query.Node{
						{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "excluded"}}},
						{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: colName}}},
					},
				},
			},
		}
	}

	return &pg_query.MergeWhenClause{
		MatchKind:   pg_query.MergeMatchKind_MERGE_WHEN_NOT_MATCHED_BY_TARGET,
		CommandType: pg_query.CmdType_CMD_INSERT,
		TargetList:  targetList,
		Values:      values,
	}
}

func (t *OnConflictTransform) transformInsert(insert *pg_query.InsertStmt) bool {
	if insert == nil || insert.OnConflictClause == nil {
		return false
	}

	// DuckDB now supports ON CONFLICT syntax similar to PostgreSQL
	// However, there are some differences:
	//
	// 1. PostgreSQL allows ON CONFLICT ON CONSTRAINT constraint_name
	//    DuckDB only supports ON CONFLICT (columns)
	//
	// 2. PostgreSQL has EXCLUDED pseudo-table
	//    DuckDB also supports EXCLUDED
	//
	// 3. PostgreSQL allows WHERE clause in ON CONFLICT
	//    DuckDB supports this too (as of recent versions)
	//
	// For most common cases, the syntax is compatible.
	// We mainly need to handle edge cases.

	occ := insert.OnConflictClause

	// Check for ON CONFLICT ON CONSTRAINT (not supported in DuckDB)
	// The constraint name is in the Infer clause's Conname field
	if occ.Infer != nil && occ.Infer.Conname != "" {
		// This is ON CONFLICT ON CONSTRAINT constraint_name
		// DuckDB doesn't support this syntax
		// We could try to look up the constraint columns, but that's complex
		// For now, we leave it as-is and let it error with a clear message
		return false
	}

	// ON CONFLICT (columns) DO NOTHING - supported in DuckDB
	// ON CONFLICT (columns) DO UPDATE - supported in DuckDB
	// These should work as-is

	return false
}

// OnConflictNote documents the ON CONFLICT syntax differences:
//
// PostgreSQL:
//   INSERT INTO t (a, b) VALUES (1, 2)
//   ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b
//
//   INSERT INTO t (a, b) VALUES (1, 2)
//   ON CONFLICT (a) DO NOTHING
//
//   INSERT INTO t (a, b) VALUES (1, 2)
//   ON CONFLICT ON CONSTRAINT pk_name DO UPDATE SET b = EXCLUDED.b
//
// DuckDB (supported):
//   INSERT INTO t (a, b) VALUES (1, 2)
//   ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b
//
//   INSERT INTO t (a, b) VALUES (1, 2)
//   ON CONFLICT (a) DO NOTHING
//
//   INSERT OR REPLACE INTO t (a, b) VALUES (1, 2)  -- Alternative syntax
//   INSERT OR IGNORE INTO t (a, b) VALUES (1, 2)   -- Alternative syntax
//
// DuckDB (NOT supported):
//   ON CONFLICT ON CONSTRAINT constraint_name  -- Use column list instead

package transform

import (
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
			if mergeStmt := t.transformInsertToMerge(insert); mergeStmt != nil {
				// Replace the INSERT statement with MERGE
				stmt.Stmt = &pg_query.Node{
					Node: &pg_query.Node_MergeStmt{MergeStmt: mergeStmt},
				}
				changed = true
			} else if t.transformInsert(insert) {
				changed = true
			}
		}
	}

	return changed, nil
}

// transformInsertToMerge converts INSERT ... ON CONFLICT to MERGE for DuckLake mode
func (t *OnConflictTransform) transformInsertToMerge(insert *pg_query.InsertStmt) *pg_query.MergeStmt {
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

	// Get column names from INSERT
	colNames := make([]string, len(insert.Cols))
	for i, col := range insert.Cols {
		if rt := col.GetResTarget(); rt != nil {
			colNames[i] = rt.Name
		}
	}

	// Get VALUES from INSERT's SelectStmt
	selectStmt := insert.SelectStmt.GetSelectStmt()
	if selectStmt == nil || len(selectStmt.ValuesLists) == 0 {
		return nil
	}

	// Build the source subquery: SELECT val1 AS col1, val2 AS col2, ...
	// We use a VALUES clause in a subquery for multiple rows, or SELECT for single row
	sourceSelect := t.buildSourceSelect(colNames, selectStmt.ValuesLists)
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

	return &pg_query.MergeStmt{
		Relation:         insert.Relation,
		SourceRelation:   sourceRelation,
		JoinCondition:    joinCondition,
		MergeWhenClauses: whenClauses,
	}
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

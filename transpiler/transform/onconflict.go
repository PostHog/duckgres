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
// We strip ON CONFLICT entirely in DuckLake mode since there are no constraints
// to conflict with.
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
			if t.transformInsert(insert) {
				changed = true
			}
		}
	}

	return changed, nil
}

func (t *OnConflictTransform) transformInsert(insert *pg_query.InsertStmt) bool {
	if insert == nil || insert.OnConflictClause == nil {
		return false
	}

	// In DuckLake mode, we strip PRIMARY KEY and UNIQUE constraints,
	// so ON CONFLICT clauses will fail with "columns not referenced by constraint".
	// Strip the ON CONFLICT clause entirely since there are no constraints to conflict with.
	// The data will be inserted normally. Fivetran's DELETE + INSERT pattern handles updates.
	if t.DuckLakeMode {
		insert.OnConflictClause = nil
		return true
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

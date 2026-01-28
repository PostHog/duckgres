package transform

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func TestOnConflictTransform_Name(t *testing.T) {
	tr := NewOnConflictTransform()
	if tr.Name() != "onconflict" {
		t.Errorf("Name() = %q, want %q", tr.Name(), "onconflict")
	}
}

func TestOnConflictTransform_NonDuckLakeMode(t *testing.T) {
	// In non-DuckLake mode, ON CONFLICT should pass through unchanged
	tr := NewOnConflictTransformWithConfig(false)

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "DO UPDATE passes through",
			input: "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
		},
		{
			name:  "DO NOTHING passes through",
			input: "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO NOTHING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := pg_query.Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			result := &Result{}
			changed, err := tr.Transform(tree, result)
			if err != nil {
				t.Fatalf("Transform error: %v", err)
			}

			// Should not change in non-DuckLake mode
			if changed {
				t.Error("Transform should not change SQL in non-DuckLake mode")
			}

			// Should still be an INSERT statement
			if tree.Stmts[0].Stmt.GetInsertStmt() == nil {
				t.Error("Statement should remain an INSERT")
			}
		})
	}
}

func TestOnConflictTransform_DuckLakeMode_DoUpdate(t *testing.T) {
	tr := NewOnConflictTransformWithConfig(true)

	tests := []struct {
		name            string
		input           string
		wantContains    []string
		wantNotContains []string
	}{
		{
			name:  "single row DO UPDATE",
			input: "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
			wantContains: []string{
				"MERGE INTO users",
				"USING (SELECT 1 AS id, 'test' AS name)",
				"excluded",
				"ON excluded.id = users.id",
				"WHEN MATCHED THEN UPDATE SET name = excluded.name",
				"WHEN NOT MATCHED THEN INSERT",
			},
			wantNotContains: []string{
				"ON CONFLICT",
				"EXCLUDED", // should be lowercase "excluded"
			},
		},
		{
			name:  "multiple columns in SET",
			input: "INSERT INTO users (id, name, email) VALUES (1, 'test', 'test@example.com') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email",
			wantContains: []string{
				"MERGE INTO users",
				"SELECT 1 AS id, 'test' AS name, 'test@example.com' AS email",
				"WHEN MATCHED THEN UPDATE SET name = excluded.name, email = excluded.email",
			},
		},
		{
			name:  "multiple values become UNION ALL",
			input: "INSERT INTO users (id, name) VALUES (1, 'first'), (2, 'second'), (3, 'third') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
			wantContains: []string{
				"MERGE INTO users",
				"UNION ALL",
				"WHEN MATCHED THEN UPDATE",
				"WHEN NOT MATCHED THEN INSERT",
			},
		},
		{
			name:  "composite key conflict",
			input: "INSERT INTO events (org_id, event_id, data) VALUES (1, 100, 'payload') ON CONFLICT (org_id, event_id) DO UPDATE SET data = EXCLUDED.data",
			wantContains: []string{
				"MERGE INTO events",
				"excluded.org_id = events.org_id",
				"excluded.event_id = events.event_id",
				"AND",
			},
		},
		{
			name:  "INSERT SELECT FROM staging table (Fivetran pattern)",
			input: `INSERT INTO users (id, name, email) SELECT id, name, email FROM staging_table ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email`,
			wantContains: []string{
				"MERGE INTO users",
				"USING (SELECT id, name, email FROM staging_table)",
				"excluded",
				"ON excluded.id = users.id",
				"WHEN MATCHED THEN UPDATE SET name = excluded.name, email = excluded.email",
				"WHEN NOT MATCHED THEN INSERT",
			},
			wantNotContains: []string{
				"ON CONFLICT",
			},
		},
		{
			name:  "INSERT SELECT with schema-qualified staging table",
			input: `INSERT INTO "myschema"."users" (id, name) SELECT "id", "name" FROM "myschema_staging"."temp_users" ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`,
			wantContains: []string{
				"MERGE INTO myschema.users",
				"FROM myschema_staging.temp_users",
				"WHEN MATCHED THEN UPDATE",
				"WHEN NOT MATCHED THEN INSERT",
			},
			wantNotContains: []string{
				"ON CONFLICT",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := pg_query.Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			result := &Result{}
			changed, err := tr.Transform(tree, result)
			if err != nil {
				t.Fatalf("Transform error: %v", err)
			}

			if !changed {
				t.Error("Transform should change SQL in DuckLake mode")
			}

			// Should now be a MERGE statement
			if tree.Stmts[0].Stmt.GetMergeStmt() == nil {
				t.Error("Statement should be converted to MERGE")
			}

			// Deparse and check contents
			sql, err := pg_query.Deparse(tree)
			if err != nil {
				t.Fatalf("Deparse error: %v", err)
			}

			for _, want := range tt.wantContains {
				if !strings.Contains(sql, want) {
					t.Errorf("SQL should contain %q\nGot: %s", want, sql)
				}
			}

			for _, notWant := range tt.wantNotContains {
				if strings.Contains(sql, notWant) {
					t.Errorf("SQL should NOT contain %q\nGot: %s", notWant, sql)
				}
			}
		})
	}
}

func TestOnConflictTransform_DuckLakeMode_DoNothing(t *testing.T) {
	tr := NewOnConflictTransformWithConfig(true)

	tests := []struct {
		name            string
		input           string
		wantContains    []string
		wantNotContains []string
	}{
		{
			name:  "single row DO NOTHING",
			input: "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO NOTHING",
			wantContains: []string{
				"MERGE INTO users",
				"USING (SELECT 1 AS id, 'test' AS name)",
				"ON excluded.id = users.id",
				"WHEN NOT MATCHED THEN INSERT",
			},
			wantNotContains: []string{
				"ON CONFLICT",
				"WHEN MATCHED THEN UPDATE", // DO NOTHING should not have UPDATE clause
			},
		},
		{
			name:  "multiple values DO NOTHING",
			input: "INSERT INTO users (id, name) VALUES (1, 'first'), (2, 'second') ON CONFLICT (id) DO NOTHING",
			wantContains: []string{
				"MERGE INTO users",
				"UNION ALL",
				"WHEN NOT MATCHED THEN INSERT",
			},
			wantNotContains: []string{
				"ON CONFLICT",
				"WHEN MATCHED",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := pg_query.Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			result := &Result{}
			changed, err := tr.Transform(tree, result)
			if err != nil {
				t.Fatalf("Transform error: %v", err)
			}

			if !changed {
				t.Error("Transform should change SQL in DuckLake mode")
			}

			sql, err := pg_query.Deparse(tree)
			if err != nil {
				t.Fatalf("Deparse error: %v", err)
			}

			for _, want := range tt.wantContains {
				if !strings.Contains(sql, want) {
					t.Errorf("SQL should contain %q\nGot: %s", want, sql)
				}
			}

			for _, notWant := range tt.wantNotContains {
				if strings.Contains(sql, notWant) {
					t.Errorf("SQL should NOT contain %q\nGot: %s", notWant, sql)
				}
			}
		})
	}
}

func TestOnConflictTransform_NoTransformCases(t *testing.T) {
	tr := NewOnConflictTransformWithConfig(true)

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "INSERT without ON CONFLICT",
			input: "INSERT INTO users (id, name) VALUES (1, 'test')",
		},
		{
			name:  "SELECT statement",
			input: "SELECT * FROM users",
		},
		{
			name:  "UPDATE statement",
			input: "UPDATE users SET name = 'test' WHERE id = 1",
		},
		{
			name:  "DELETE statement",
			input: "DELETE FROM users WHERE id = 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := pg_query.Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			result := &Result{}
			changed, err := tr.Transform(tree, result)
			if err != nil {
				t.Fatalf("Transform error: %v", err)
			}

			if changed {
				t.Error("Transform should not change SQL without ON CONFLICT")
			}
		})
	}
}

func TestOnConflictTransform_PreservesTableName(t *testing.T) {
	tr := NewOnConflictTransformWithConfig(true)

	tests := []struct {
		name      string
		input     string
		tableName string
	}{
		{
			name:      "simple table name",
			input:     "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
			tableName: "users",
		},
		{
			name:      "different table name",
			input:     "INSERT INTO events (id, data) VALUES (1, 'payload') ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
			tableName: "events",
		},
		{
			name:      "table name with underscore",
			input:     "INSERT INTO user_events (id, event_type) VALUES (1, 'click') ON CONFLICT (id) DO UPDATE SET event_type = EXCLUDED.event_type",
			tableName: "user_events",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := pg_query.Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			result := &Result{}
			_, err = tr.Transform(tree, result)
			if err != nil {
				t.Fatalf("Transform error: %v", err)
			}

			mergeStmt := tree.Stmts[0].Stmt.GetMergeStmt()
			if mergeStmt == nil {
				t.Fatal("Expected MERGE statement")
				return
			}

			if mergeStmt.Relation.Relname != tt.tableName {
				t.Errorf("Table name = %q, want %q", mergeStmt.Relation.Relname, tt.tableName)
			}
		})
	}
}

func TestOnConflictTransform_MergeWhenClauses(t *testing.T) {
	tr := NewOnConflictTransformWithConfig(true)

	t.Run("DO UPDATE has both WHEN clauses", func(t *testing.T) {
		input := "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
		tree, err := pg_query.Parse(input)
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}

		result := &Result{}
		_, err = tr.Transform(tree, result)
		if err != nil {
			t.Fatalf("Transform error: %v", err)
		}

		mergeStmt := tree.Stmts[0].Stmt.GetMergeStmt()
		if mergeStmt == nil {
			t.Fatal("Expected MERGE statement")
			return
		}

		// Should have 2 WHEN clauses: MATCHED (UPDATE) and NOT MATCHED (INSERT)
		if len(mergeStmt.MergeWhenClauses) != 2 {
			t.Errorf("Expected 2 WHEN clauses, got %d", len(mergeStmt.MergeWhenClauses))
		}

		// First should be WHEN MATCHED
		firstClause := mergeStmt.MergeWhenClauses[0].GetMergeWhenClause()
		if firstClause.MatchKind != pg_query.MergeMatchKind_MERGE_WHEN_MATCHED {
			t.Errorf("First clause should be WHEN MATCHED, got %v", firstClause.MatchKind)
		}
		if firstClause.CommandType != pg_query.CmdType_CMD_UPDATE {
			t.Errorf("First clause command should be UPDATE, got %v", firstClause.CommandType)
		}

		// Second should be WHEN NOT MATCHED
		secondClause := mergeStmt.MergeWhenClauses[1].GetMergeWhenClause()
		if secondClause.MatchKind != pg_query.MergeMatchKind_MERGE_WHEN_NOT_MATCHED_BY_TARGET {
			t.Errorf("Second clause should be WHEN NOT MATCHED, got %v", secondClause.MatchKind)
		}
		if secondClause.CommandType != pg_query.CmdType_CMD_INSERT {
			t.Errorf("Second clause command should be INSERT, got %v", secondClause.CommandType)
		}
	})

	t.Run("DO NOTHING has only NOT MATCHED clause", func(t *testing.T) {
		input := "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO NOTHING"
		tree, err := pg_query.Parse(input)
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}

		result := &Result{}
		_, err = tr.Transform(tree, result)
		if err != nil {
			t.Fatalf("Transform error: %v", err)
		}

		mergeStmt := tree.Stmts[0].Stmt.GetMergeStmt()
		if mergeStmt == nil {
			t.Fatal("Expected MERGE statement")
			return
		}

		// Should have only 1 WHEN clause: NOT MATCHED (INSERT)
		if len(mergeStmt.MergeWhenClauses) != 1 {
			t.Errorf("Expected 1 WHEN clause, got %d", len(mergeStmt.MergeWhenClauses))
		}

		// Should be WHEN NOT MATCHED
		clause := mergeStmt.MergeWhenClauses[0].GetMergeWhenClause()
		if clause.MatchKind != pg_query.MergeMatchKind_MERGE_WHEN_NOT_MATCHED_BY_TARGET {
			t.Errorf("Clause should be WHEN NOT MATCHED, got %v", clause.MatchKind)
		}
		if clause.CommandType != pg_query.CmdType_CMD_INSERT {
			t.Errorf("Clause command should be INSERT, got %v", clause.CommandType)
		}
	})
}

func TestOnConflictTransform_SourceSubquery(t *testing.T) {
	tr := NewOnConflictTransformWithConfig(true)

	t.Run("single row creates simple SELECT", func(t *testing.T) {
		input := "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
		tree, err := pg_query.Parse(input)
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}

		result := &Result{}
		_, err = tr.Transform(tree, result)
		if err != nil {
			t.Fatalf("Transform error: %v", err)
		}

		sql, _ := pg_query.Deparse(tree)

		// Should have a simple SELECT, not UNION ALL
		if strings.Contains(sql, "UNION ALL") {
			t.Errorf("Single row should not use UNION ALL: %s", sql)
		}
		if !strings.Contains(sql, "SELECT 1 AS id, 'test' AS name") {
			t.Errorf("Should have SELECT with aliased columns: %s", sql)
		}
	})

	t.Run("multiple rows creates UNION ALL", func(t *testing.T) {
		input := "INSERT INTO users (id, name) VALUES (1, 'first'), (2, 'second') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
		tree, err := pg_query.Parse(input)
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}

		result := &Result{}
		_, err = tr.Transform(tree, result)
		if err != nil {
			t.Fatalf("Transform error: %v", err)
		}

		sql, _ := pg_query.Deparse(tree)

		if !strings.Contains(sql, "UNION ALL") {
			t.Errorf("Multiple rows should use UNION ALL: %s", sql)
		}
	})

	t.Run("three rows creates nested UNION ALL", func(t *testing.T) {
		input := "INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
		tree, err := pg_query.Parse(input)
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}

		result := &Result{}
		_, err = tr.Transform(tree, result)
		if err != nil {
			t.Fatalf("Transform error: %v", err)
		}

		sql, _ := pg_query.Deparse(tree)

		// Should have two UNION ALLs for three rows
		count := strings.Count(sql, "UNION ALL")
		if count != 2 {
			t.Errorf("Three rows should have 2 UNION ALLs, got %d: %s", count, sql)
		}
	})
}

func TestOnConflictTransform_JoinCondition(t *testing.T) {
	tr := NewOnConflictTransformWithConfig(true)

	t.Run("single column join", func(t *testing.T) {
		input := "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
		tree, err := pg_query.Parse(input)
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}

		result := &Result{}
		_, err = tr.Transform(tree, result)
		if err != nil {
			t.Fatalf("Transform error: %v", err)
		}

		sql, _ := pg_query.Deparse(tree)

		if !strings.Contains(sql, "ON excluded.id = users.id") {
			t.Errorf("Should have single column join condition: %s", sql)
		}
		// Should NOT have AND for single column
		if strings.Contains(sql, " AND ") {
			t.Errorf("Single column should not have AND: %s", sql)
		}
	})

	t.Run("two column join", func(t *testing.T) {
		input := "INSERT INTO events (org_id, event_id, data) VALUES (1, 100, 'x') ON CONFLICT (org_id, event_id) DO UPDATE SET data = EXCLUDED.data"
		tree, err := pg_query.Parse(input)
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}

		result := &Result{}
		_, err = tr.Transform(tree, result)
		if err != nil {
			t.Fatalf("Transform error: %v", err)
		}

		sql, _ := pg_query.Deparse(tree)

		if !strings.Contains(sql, "excluded.org_id = events.org_id") {
			t.Errorf("Should have org_id condition: %s", sql)
		}
		if !strings.Contains(sql, "excluded.event_id = events.event_id") {
			t.Errorf("Should have event_id condition: %s", sql)
		}
		if !strings.Contains(sql, " AND ") {
			t.Errorf("Two columns should be joined with AND: %s", sql)
		}
	})

	t.Run("three column join", func(t *testing.T) {
		input := "INSERT INTO data (a, b, c, val) VALUES (1, 2, 3, 'x') ON CONFLICT (a, b, c) DO UPDATE SET val = EXCLUDED.val"
		tree, err := pg_query.Parse(input)
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}

		result := &Result{}
		_, err = tr.Transform(tree, result)
		if err != nil {
			t.Fatalf("Transform error: %v", err)
		}

		sql, _ := pg_query.Deparse(tree)

		// Should have 2 ANDs for 3 columns
		count := strings.Count(sql, " AND ")
		if count != 2 {
			t.Errorf("Three columns should have 2 ANDs, got %d: %s", count, sql)
		}
	})
}

func TestOnConflictTransform_ExcludedAlias(t *testing.T) {
	tr := NewOnConflictTransformWithConfig(true)

	input := "INSERT INTO users (id, name, email) VALUES (1, 'test', 'test@example.com') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email"
	tree, err := pg_query.Parse(input)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	result := &Result{}
	_, err = tr.Transform(tree, result)
	if err != nil {
		t.Fatalf("Transform error: %v", err)
	}

	sql, _ := pg_query.Deparse(tree)

	// EXCLUDED should be converted to lowercase "excluded" (the alias)
	if strings.Contains(sql, "EXCLUDED.") {
		t.Errorf("EXCLUDED should be converted to excluded: %s", sql)
	}

	// Should use excluded alias in source
	if !strings.Contains(sql, ") excluded ON") {
		t.Errorf("Source should have 'excluded' alias: %s", sql)
	}

	// Should use excluded in UPDATE SET
	if !strings.Contains(sql, "name = excluded.name") {
		t.Errorf("UPDATE should reference excluded.name: %s", sql)
	}
	if !strings.Contains(sql, "email = excluded.email") {
		t.Errorf("UPDATE should reference excluded.email: %s", sql)
	}
}

func TestOnConflictTransform_InsertClauseValues(t *testing.T) {
	tr := NewOnConflictTransformWithConfig(true)

	input := "INSERT INTO users (id, name, email) VALUES (1, 'test', 'test@example.com') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
	tree, err := pg_query.Parse(input)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	result := &Result{}
	_, err = tr.Transform(tree, result)
	if err != nil {
		t.Fatalf("Transform error: %v", err)
	}

	sql, _ := pg_query.Deparse(tree)

	// WHEN NOT MATCHED THEN INSERT should reference all columns from excluded
	if !strings.Contains(sql, "INSERT (id, name, email) VALUES (excluded.id, excluded.name, excluded.email)") {
		t.Errorf("INSERT clause should reference all columns from excluded: %s", sql)
	}
}

func TestOnConflictTransform_DataTypes(t *testing.T) {
	tr := NewOnConflictTransformWithConfig(true)

	tests := []struct {
		name  string
		input string
		check string
	}{
		{
			name:  "integer values",
			input: "INSERT INTO t (id, count) VALUES (1, 100) ON CONFLICT (id) DO UPDATE SET count = EXCLUDED.count",
			check: "SELECT 1 AS id, 100 AS count",
		},
		{
			name:  "string values",
			input: "INSERT INTO t (id, name) VALUES (1, 'hello world') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
			check: "'hello world' AS name",
		},
		{
			name:  "null values",
			input: "INSERT INTO t (id, val) VALUES (1, NULL) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val",
			check: "NULL AS val",
		},
		{
			name:  "negative numbers",
			input: "INSERT INTO t (id, val) VALUES (1, -42) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val",
			check: "-42 AS val",
		},
		{
			name:  "decimal numbers",
			input: "INSERT INTO t (id, price) VALUES (1, 19.99) ON CONFLICT (id) DO UPDATE SET price = EXCLUDED.price",
			check: "19.99 AS price",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := pg_query.Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			result := &Result{}
			_, err = tr.Transform(tree, result)
			if err != nil {
				t.Fatalf("Transform error: %v", err)
			}

			sql, _ := pg_query.Deparse(tree)

			if !strings.Contains(sql, tt.check) {
				t.Errorf("SQL should contain %q\nGot: %s", tt.check, sql)
			}
		})
	}
}

func TestOnConflictTransform_FivetranPattern(t *testing.T) {
	// Test the exact pattern Fivetran uses: INSERT INTO target SELECT FROM staging ON CONFLICT DO UPDATE
	tr := NewOnConflictTransformWithConfig(true)

	// Simplified version of the Fivetran query
	input := `INSERT INTO "stripe_test"."invoice" ("id", "status", "amount_due", "currency", "_fivetran_synced")
		SELECT "id", "status", "amount_due", "currency", "_fivetran_synced"
		FROM "stripe_test_staging"."temp_invoice"
		ON CONFLICT ("id") DO UPDATE SET
			"status" = "excluded"."status",
			"amount_due" = "excluded"."amount_due",
			"currency" = "excluded"."currency",
			"_fivetran_synced" = "excluded"."_fivetran_synced"`

	tree, err := pg_query.Parse(input)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	result := &Result{}
	changed, err := tr.Transform(tree, result)
	if err != nil {
		t.Fatalf("Transform error: %v", err)
	}

	if !changed {
		t.Error("Transform should change SQL in DuckLake mode")
	}

	// Should be converted to MERGE
	if tree.Stmts[0].Stmt.GetMergeStmt() == nil {
		t.Fatal("Statement should be converted to MERGE")
	}

	sql, err := pg_query.Deparse(tree)
	if err != nil {
		t.Fatalf("Deparse error: %v", err)
	}

	// Verify key parts of the MERGE statement
	checks := []string{
		"MERGE INTO stripe_test.invoice",
		"USING (SELECT",
		"FROM stripe_test_staging.temp_invoice",
		"excluded",
		"WHEN MATCHED THEN UPDATE SET",
		"WHEN NOT MATCHED THEN INSERT",
	}

	for _, check := range checks {
		if !strings.Contains(sql, check) {
			t.Errorf("SQL should contain %q\nGot: %s", check, sql)
		}
	}

	// Should NOT contain ON CONFLICT
	if strings.Contains(sql, "ON CONFLICT") {
		t.Errorf("SQL should NOT contain ON CONFLICT\nGot: %s", sql)
	}

	t.Logf("Transformed SQL:\n%s", sql)
}

func TestOnConflictTransform_InsertSelectDoNothing(t *testing.T) {
	tr := NewOnConflictTransformWithConfig(true)

	input := `INSERT INTO target (id, data) SELECT id, data FROM source ON CONFLICT (id) DO NOTHING`

	tree, err := pg_query.Parse(input)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	result := &Result{}
	changed, err := tr.Transform(tree, result)
	if err != nil {
		t.Fatalf("Transform error: %v", err)
	}

	if !changed {
		t.Error("Transform should change SQL in DuckLake mode")
	}

	sql, err := pg_query.Deparse(tree)
	if err != nil {
		t.Fatalf("Deparse error: %v", err)
	}

	// Should have MERGE with only WHEN NOT MATCHED (no UPDATE for DO NOTHING)
	if !strings.Contains(sql, "MERGE INTO target") {
		t.Errorf("Should have MERGE INTO target: %s", sql)
	}
	if !strings.Contains(sql, "WHEN NOT MATCHED THEN INSERT") {
		t.Errorf("Should have WHEN NOT MATCHED: %s", sql)
	}
	if strings.Contains(sql, "WHEN MATCHED") {
		t.Errorf("Should NOT have WHEN MATCHED for DO NOTHING: %s", sql)
	}
}

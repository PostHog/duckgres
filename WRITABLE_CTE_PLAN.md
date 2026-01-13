# Plan: Writable CTE Support for Duckgres

## Problem Statement

Airbyte's PostgreSQL destination connector uses **writable CTEs** (CTEs containing UPDATE/INSERT/DELETE) for deduplication. DuckDB doesn't support this PostgreSQL feature, causing:

```
ERROR: Not implemented Error: A CTE needs a SELECT
```

**Failing Pattern:**
```sql
WITH deduped_source AS (
  SELECT ... ROW_NUMBER() OVER (PARTITION BY id) ... WHERE row_number = 1
),
updates AS (
  UPDATE target SET col = src.col FROM deduped_source src WHERE target.id = src.id
)
INSERT INTO target SELECT * FROM deduped_source WHERE NOT EXISTS (...)
```

---

## Technical Analysis

### AST Structure (pg_query_go)

```
RawStmt
└── InsertStmt / UpdateStmt / DeleteStmt
    └── WithClause
        ├── Recursive: bool
        └── Ctes: []*Node
            └── CommonTableExpr
                ├── Ctename: string ("deduped_source", "updates")
                └── Ctequery: *Node (SelectStmt, UpdateStmt, etc.)
```

**Key insight:** The `Ctequery` field of a `CommonTableExpr` can contain:
- `SelectStmt` (standard CTE) - DuckDB supports this
- `UpdateStmt` / `InsertStmt` / `DeleteStmt` (writable CTE) - DuckDB does NOT support

### Current Duckgres Limitation

Duckgres executes **one statement per Query message**. Rewriting a writable CTE into multiple statements requires:
1. Adding multi-statement support to the transpiler result
2. Modifying `conn.go` to execute statements sequentially

---

## Implementation Plan

### Phase 1: Multi-Statement Transpiler Support ✅ COMPLETE

#### 1.1 Modify `transpiler/config.go` Result Structure

Separate main statements from cleanup to enable proper streaming:

```go
// Result contains the output of transpilation
type Result struct {
    SQL               string   // Primary SQL (backward compatible)
    Statements        []string // Main statements (setup + final query)
    CleanupStatements []string // Cleanup statements (DROP temp tables, COMMIT)
    ParamCount        int
    IsNoOp            bool
    NoOpTag           string
    IsIgnoredSet      bool
    Error             error
}
```

#### 1.2 Modify `server/conn.go` `handleQuery()`

**Critical: Execute cleanup AFTER obtaining cursor but BEFORE streaming**

When the final statement is SELECT/RETURNING:
1. Execute all setup statements (BEGIN, CREATE TEMP...)
2. Execute final query and **obtain cursor** (rows object)
3. Execute cleanup (DROP temp tables, COMMIT) - cursor still holds data
4. Stream rows from cursor to client

```go
func (c *clientConn) handleQuery(body []byte) error {
    // ... existing parsing ...

    result, err := tr.Transpile(query)
    if err != nil {
        return c.sendError("42601", err.Error())
    }

    // Handle multi-statement results (writable CTE rewrites)
    if len(result.Statements) > 0 {
        return c.executeMultiStatement(result.Statements, result.CleanupStatements)
    }

    // ... existing single-statement logic ...
}

func (c *clientConn) executeMultiStatement(statements []string, cleanup []string) error {
    if len(statements) == 0 {
        return nil
    }

    // Execute setup statements (all but last)
    for i := 0; i < len(statements)-1; i++ {
        _, err := c.db.Exec(statements[i])
        if err != nil {
            c.setTxError()
            // On error, still try to cleanup (best effort)
            c.executeCleanup(cleanup)
            return c.sendError("42000", err.Error())
        }
    }

    // Handle final statement
    finalStmt := statements[len(statements)-1]
    cmdType := getCommandType(finalStmt)

    if isSelectQuery(cmdType) {
        // SELECT: obtain cursor FIRST, cleanup SECOND, stream THIRD
        rows, err := c.db.Query(finalStmt)
        if err != nil {
            c.setTxError()
            c.executeCleanup(cleanup)
            return c.sendError("42000", err.Error())
        }
        defer rows.Close()

        // Execute cleanup while cursor is open (data is materialized in cursor)
        // DuckDB cursor holds result data even after source tables are dropped
        c.executeCleanup(cleanup)

        // Now stream results from cursor
        return c.streamRowsToClient(rows, cmdType)

    } else {
        // DML (INSERT/UPDATE/DELETE): execute then cleanup
        result, err := c.db.Exec(finalStmt)
        if err != nil {
            c.setTxError()
            c.executeCleanup(cleanup)
            return c.sendError("42000", err.Error())
        }

        // Execute cleanup
        c.executeCleanup(cleanup)

        // Send completion
        rowsAffected, _ := result.RowsAffected()
        tag := fmt.Sprintf("%s 0 %d", cmdType, rowsAffected)
        writeCommandComplete(c.writer, tag)
        writeReadyForQuery(c.writer, c.txStatus)
        return c.writer.Flush()
    }
}

// executeCleanup runs cleanup statements, ignoring errors (best effort)
func (c *clientConn) executeCleanup(cleanup []string) {
    for _, stmt := range cleanup {
        c.db.Exec(stmt) // Ignore errors - cleanup is best effort
    }
}

// streamRowsToClient sends result rows over the wire protocol
func (c *clientConn) streamRowsToClient(rows *sql.Rows, cmdType string) error {
    // Get column info
    cols, err := rows.ColumnTypes()
    if err != nil {
        return c.sendError("42000", err.Error())
    }

    // Send RowDescription
    writeRowDescription(c.writer, cols)

    // Stream DataRows
    rowCount := 0
    for rows.Next() {
        // ... existing row streaming logic ...
        rowCount++
    }

    if err := rows.Err(); err != nil {
        return c.sendError("42000", err.Error())
    }

    // Send completion
    tag := fmt.Sprintf("%s %d", cmdType, rowCount)
    writeCommandComplete(c.writer, tag)
    writeReadyForQuery(c.writer, c.txStatus)
    return c.writer.Flush()
}
```

---

### Phase 2: WritableCTETransform Implementation ✅ COMPLETE

#### 2.1 Create `transpiler/transform/writablecte.go`

```go
package transform

import (
    "fmt"
    pg_query "github.com/pganalyze/pg_query_go/v6"
)

// WritableCTETransform detects and rewrites PostgreSQL writable CTEs
// into equivalent multi-statement sequences that DuckDB can execute.
type WritableCTETransform struct{}

func NewWritableCTETransform() *WritableCTETransform {
    return &WritableCTETransform{}
}

func (t *WritableCTETransform) Name() string {
    return "writable_cte"
}

func (t *WritableCTETransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
    for _, stmt := range tree.Stmts {
        if stmt.Stmt == nil {
            continue
        }

        // Check for ANY statement with CTEs (SELECT, INSERT, UPDATE, DELETE)
        // PostgreSQL allows: WITH writable_cte AS (UPDATE ... RETURNING *) SELECT ...
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
```

#### 2.2 CTE Analysis and Dependency Resolution

**Critical: Preserve CTE Evaluation Order**

CTEs can have interleaved dependencies where read-only CTEs depend on writable CTEs:

```sql
WITH
  a AS (SELECT ...),                      -- read-only
  b AS (UPDATE ... FROM a RETURNING *),   -- writable, depends on 'a'
  c AS (SELECT ... FROM b),               -- read-only, depends on writable 'b'!
  d AS (DELETE ... WHERE id IN (SELECT id FROM c))  -- writable, depends on 'c'
INSERT INTO ... SELECT FROM c, d;
```

**Execution must follow declaration order**, not type grouping.

```go
type cteInfo struct {
    name     string
    node     *pg_query.CommonTableExpr
    isWrite  bool      // true if UPDATE/INSERT/DELETE
    deps     []string  // CTE names this one references
    order    int       // declaration order (0, 1, 2, ...)
}

// analyzeCTEs extracts CTE metadata while preserving declaration order
func (t *WritableCTETransform) analyzeCTEs(with *pg_query.WithClause) []*cteInfo {
    var ctes []*cteInfo

    for i, cteNode := range with.Ctes {
        cte := cteNode.GetCommonTableExpr()
        if cte == nil {
            continue
        }

        info := &cteInfo{
            name:  cte.Ctename,
            node:  cte,
            deps:  t.findCTEDependencies(cte.Ctequery),
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
func (t *WritableCTETransform) findCTEDependencies(node *pg_query.Node) []string {
    var deps []string
    seen := make(map[string]bool)

    WalkFunc(&pg_query.ParseResult{Stmts: []*pg_query.RawStmt{{Stmt: node}}},
        func(n *pg_query.Node) bool {
            if rv := n.GetRangeVar(); rv != nil {
                // Only count as CTE reference if UNQUALIFIED (no catalog, no schema)
                // Schema-qualified refs like "public.mytable" are real tables, not CTEs
                if rv.Catalogname == "" && rv.Schemaname == "" && rv.Relname != "" {
                    if !seen[rv.Relname] {
                        deps = append(deps, rv.Relname)
                        seen[rv.Relname] = true
                    }
                }
            }
            return true
        })

    return deps
}

// isCTEReference checks if a RangeVar is an unqualified reference (potential CTE)
// Schema-qualified references like "public.foo" are real tables, not CTE refs
func (t *WritableCTETransform) isCTEReference(rv *pg_query.RangeVar) bool {
    return rv.Catalogname == "" && rv.Schemaname == "" && rv.Relname != ""
}
```

#### 2.3 Rewrite Strategy

**Critical: RETURNING Clause Handling**

In PostgreSQL, writable CTEs can have `RETURNING` clauses whose output is referenced by subsequent CTEs or the final statement:

```sql
WITH updates AS (
  UPDATE target SET col = 1 WHERE id = 5 RETURNING id, col
)
INSERT INTO audit SELECT * FROM updates;  -- References RETURNING output
```

The rewrite must **materialize RETURNING output** to a temp table so it can be referenced.

**Rewrite Transformation:**

```sql
-- ORIGINAL (PostgreSQL writable CTE)
WITH deduped AS (SELECT ... WHERE row_number = 1),
     updates AS (UPDATE t SET ... FROM deduped WHERE t.id = deduped.id RETURNING *)
INSERT INTO t SELECT ... FROM deduped WHERE NOT EXISTS (SELECT 1 FROM updates WHERE updates.id = deduped.id)
```

Into:

```sql
-- REWRITTEN (DuckDB compatible multi-statement)
BEGIN;

-- 1. Materialize read-only CTEs
CREATE TEMP TABLE _cte_deduped AS (SELECT ... WHERE row_number = 1);

-- 2. Execute writable CTE and capture RETURNING output
CREATE TEMP TABLE _cte_updates AS (
  SELECT * FROM (
    UPDATE t SET ... FROM _cte_deduped WHERE t.id = _cte_deduped.id RETURNING *
  ) AS _returning
);
-- NOTE: DuckDB doesn't support UPDATE...RETURNING in subquery, so we need alternative

-- ALTERNATIVE for DuckDB (which doesn't support DML in subquery):
-- 2a. First, capture the rows that WILL be updated (before modification)
CREATE TEMP TABLE _cte_updates AS (
  SELECT t.* FROM t
  INNER JOIN _cte_deduped d ON t.id = d.id
  -- Apply same WHERE conditions as the UPDATE
);
-- 2b. Then execute the UPDATE
UPDATE t SET ... FROM _cte_deduped WHERE t.id = _cte_deduped.id;

-- 3. Execute final statement (references both CTEs via temp tables)
INSERT INTO t SELECT ... FROM _cte_deduped WHERE NOT EXISTS (
  SELECT 1 FROM _cte_updates WHERE _cte_updates.id = _cte_deduped.id
);

-- 4. Clean up
DROP TABLE IF EXISTS _cte_deduped;
DROP TABLE IF EXISTS _cte_updates;
COMMIT;
```

**Key Insight:** Since DuckDB doesn't support `UPDATE...RETURNING` inside a subquery, we must:
1. Pre-compute what rows will be affected (SELECT with same JOIN/WHERE)
2. Store that in a temp table (this becomes the writable CTE's "output")
3. Execute the actual DML
4. Let subsequent statements reference the temp table

```go
// rewriteResult holds separated main and cleanup statements
type rewriteResult struct {
    statements []string // Setup + final query
    cleanup    []string // DROP tables + COMMIT
}

func (t *WritableCTETransform) rewriteWritableCTE(
    stmt *pg_query.RawStmt,
    ctes []*cteInfo,  // Already in declaration order
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
// UPDATE target SET col = val FROM source WHERE target.id = source.id RETURNING target.*
// becomes:
// SELECT target.* FROM target INNER JOIN source ON target.id = source.id
//
// DELETE FROM target WHERE condition RETURNING *
// becomes:
// SELECT * FROM target WHERE condition
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
    // If RETURNING is *, select all columns from target
    // If RETURNING lists specific columns, select those
    //
    // The FROM clause in UPDATE becomes JOIN conditions
    // The WHERE clause transfers directly

    // Implementation details:
    // 1. Extract target table from update.Relation
    // 2. Extract RETURNING columns (or * if not specified, default to target.*)
    // 3. Convert FROM clause to JOINs
    // 4. Apply WHERE clause
    // 5. Rewrite CTE references to temp table names

    // ... (detailed AST manipulation)
    return "", nil // Placeholder - actual implementation needed
}
```

#### 2.4 Safe Identifier Handling

Temp table names must be properly quoted and length-limited:

```go
const maxIdentifierLength = 63 // PostgreSQL/DuckDB identifier limit

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
```

#### 2.5 Reference Rewriting

Replace CTE name references with temp table names throughout the query.
**Critical:** Only rewrite UNQUALIFIED references - schema-qualified refs are real tables.

```go
func (t *WritableCTETransform) deparseAndRewriteRefs(
    node *pg_query.Node,
    tempTableNames map[string]string,
) (string, error) {
    // Walk the AST and replace ONLY unqualified RangeVar references
    WalkFunc(&pg_query.ParseResult{Stmts: []*pg_query.RawStmt{{Stmt: node}}},
        func(n *pg_query.Node) bool {
            if rv := n.GetRangeVar(); rv != nil {
                // CRITICAL: Only rewrite if it's an unqualified reference
                // "public.foo" should NOT be rewritten even if "foo" is a CTE name
                if t.isCTEReference(rv) {
                    if newName, ok := tempTableNames[rv.Relname]; ok {
                        rv.Relname = newName
                        // Keep schema empty - temp tables are unqualified
                    }
                }
            }
            return true
        })

    return pg_query.Deparse(&pg_query.ParseResult{
        Stmts: []*pg_query.RawStmt{{Stmt: node}},
    })
}
```

---

### Phase 3: Transaction-Aware Execution

#### 3.1 Handle Nested Transactions

The rewrite wraps in BEGIN/COMMIT, but the client might already be in a transaction:

```go
func (c *clientConn) executeMultiStatement(statements []string) error {
    // Check if we're adding our own transaction wrapper
    hasOurTransaction := len(statements) >= 2 &&
        statements[0] == "BEGIN" &&
        statements[len(statements)-1] == "COMMIT"

    // If already in transaction, skip our BEGIN/COMMIT wrapper
    if hasOurTransaction && c.txStatus == txStatusTransaction {
        statements = statements[1 : len(statements)-1] // Strip BEGIN and COMMIT
    }

    for _, stmt := range statements {
        // Execute each statement...
    }
}
```

---

### Phase 4: Register Transform

#### 4.1 Modify `transpiler/transpiler.go`

```go
func New(cfg Config) *Transpiler {
    t := &Transpiler{
        config:     cfg,
        transforms: make([]transform.Transform, 0),
    }

    // Add writable CTE transform FIRST (before other transforms)
    t.transforms = append(t.transforms, transform.NewWritableCTETransform())

    // ... existing transforms ...
    t.transforms = append(t.transforms, transform.NewPgCatalogTransform())
    // ...
}
```

---

## Edge Cases to Handle

| Case | Handling Strategy |
|------|-------------------|
| **Result streaming vs cleanup** | Separate statements into `Statements` (setup+final) and `CleanupStatements` (DROP+COMMIT); obtain cursor from final query, then run cleanup, then stream from cursor |
| **Interleaved CTE dependencies** | Process CTEs in **declaration order**, not by type; each CTE can reference earlier temp tables |
| **Read-only CTE depending on writable** | Works correctly because we process in order: writable CTE's temp table exists when read-only runs |
| **Recursive CTEs** | If purely SELECT-based, pass through; if mixed with writable, complex (document as unsupported initially) |
| **RETURNING clause referenced** | Pre-capture affected rows via equivalent SELECT before executing DML |
| **RETURNING with SET values** | Must capture POST-update values; requires generating SELECT with new values |
| **Multiple writable CTEs** | Execute in declaration order; each gets its own temp table for RETURNING |
| **Writable CTE with no RETURNING** | Still materialize result (could be empty); needed if referenced by later CTE |
| **Error during execution** | Rollback transaction; run cleanup (best effort); temp tables auto-cleaned by rollback |
| **Already in transaction** | Skip BEGIN/COMMIT wrapper; rely on outer transaction |
| **Temp table name collision** | Use timestamp + random suffix in temp table names |
| **DELETE RETURNING** | Simple: SELECT matching rows before DELETE |
| **INSERT RETURNING** | Complex: Need to capture inserted rows; may require sequence/identity handling |
| **UPDATE FROM with complex joins** | Convert UPDATE's FROM clause to equivalent JOIN in the SELECT |
| **CTE referencing real table with same name** | Only rewrite UNQUALIFIED refs; `public.foo` stays as-is even if CTE named `foo` exists |
| **CTE name with special characters** | Sanitize name for temp table, use quoted identifiers in SQL |
| **CTE name exceeding 63 chars** | Truncate sanitized name, append random suffix for uniqueness |

### Result Streaming Design

**Problem:** If cleanup (DROP temp tables, COMMIT) comes after the final SELECT in a flat statement list, we must either:
1. Stream results and return early → cleanup never runs, temp tables leak
2. Run cleanup first → can't stream because temp tables are gone

**Solution:** Separate Result into `Statements` and `CleanupStatements`:

```
Execution order:
1. Execute setup statements (BEGIN, CREATE TEMP TABLE x3, ...)
2. Execute final statement → obtain cursor (rows object holds materialized results)
3. Execute cleanup statements (DROP, DROP, ..., COMMIT) - cursor still valid!
4. Stream rows from cursor to client
5. Send CommandComplete + ReadyForQuery
```

This works because DuckDB's cursor materializes query results in memory. Once you have the cursor, the underlying temp tables can be dropped without affecting iteration.

### RETURNING Semantics Deep Dive

**Problem:** PostgreSQL `RETURNING` gives you the row values **after** modification:

```sql
-- PostgreSQL: RETURNING gives NEW values
UPDATE users SET visits = visits + 1 WHERE id = 1 RETURNING visits;
-- Returns: visits = 101 (the incremented value)
```

**Challenge for DuckDB rewrite:** We must either:

1. **Pre-compute new values** - Generate SELECT that applies the SET expressions:
   ```sql
   -- Instead of: UPDATE users SET visits = visits + 1 RETURNING visits
   -- Generate:   SELECT visits + 1 AS visits FROM users WHERE id = 1
   ```

2. **Post-capture (race condition risk)** - Execute UPDATE, then SELECT:
   ```sql
   UPDATE users SET visits = visits + 1 WHERE id = 1;
   SELECT visits FROM users WHERE id = 1;  -- Risk: another transaction could modify
   ```

**Recommended approach:** Pre-compute is safer but complex. For Airbyte's pattern (which uses `RETURNING *` to detect what was updated, not to get new values), pre-capture of the matching rows is sufficient.

---

## Files to Modify/Create

| File | Action | Purpose |
|------|--------|---------|
| `transpiler/config.go` | Modify | Add `Statements []string` and `CleanupStatements []string` to Result |
| `transpiler/transform/writablecte.go` | Create | Main transform implementation |
| `transpiler/transpiler.go` | Modify | Register WritableCTETransform |
| `server/conn.go` | Modify | Add `executeMultiStatement()`, `executeCleanup()`, `streamRowsToClient()` |
| `transpiler/transpiler_test.go` | Modify | Add writable CTE test cases |
| `transpiler/transform/writablecte_test.go` | Create | Unit tests for transform |

---

## Testing Strategy

### Unit Tests (`writablecte_test.go`)

```go
func TestWritableCTEDetection(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        hasWrite bool
    }{
        {"simple select CTE", "WITH x AS (SELECT 1) SELECT * FROM x", false},
        {"update CTE with final SELECT", "WITH x AS (UPDATE t SET a=1 RETURNING *) SELECT * FROM x", true},
        {"update CTE with final INSERT", "WITH x AS (UPDATE t SET a=1 RETURNING *) INSERT INTO audit SELECT * FROM x", true},
        {"insert CTE", "WITH x AS (INSERT INTO t VALUES(1) RETURNING *) SELECT * FROM x", true},
        {"delete CTE", "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x", true},
        {"mixed CTEs with final SELECT", "WITH a AS (SELECT 1), b AS (UPDATE t SET x=1 RETURNING *) SELECT * FROM a, b", true},
        {"writable CTE not referenced", "WITH x AS (UPDATE t SET a=1) SELECT 1", true}, // Still detected even if not referenced
    }
    // ...
}

func TestWritableCTERewrite(t *testing.T) {
    tests := []struct {
        name       string
        input      string
        wantStmts  int
        wantFirst  string
        wantLast   string
    }{
        {
            name: "airbyte upsert pattern",
            input: `WITH deduped AS (SELECT * FROM temp WHERE rn=1),
                    updates AS (UPDATE t SET a=d.a FROM deduped d WHERE t.id=d.id)
                    INSERT INTO t SELECT * FROM deduped WHERE NOT EXISTS (...)`,
            wantStmts: 6, // BEGIN, CREATE TEMP, UPDATE, INSERT, DROP, COMMIT
            wantFirst: "BEGIN",
            wantLast:  "COMMIT",
        },
    }
    // ...
}
```

### Integration Tests

```go
func TestAirbyteUpsertPattern(t *testing.T) {
    // Setup: create target table with sample data
    // Execute: run Airbyte-style writable CTE query
    // Verify: check data was correctly upserted
}
```

### Manual Testing

```bash
# Build
go build -o duckgres .

# Start server
./duckgres --port 35437

# Test with psql
PGPASSWORD=postgres psql "host=127.0.0.1 port=35437 user=postgres sslmode=require" <<EOF
-- Create test table
CREATE TABLE test_upsert (id INT, name VARCHAR, updated_at TIMESTAMP);
INSERT INTO test_upsert VALUES (1, 'Alice', NOW()), (2, 'Bob', NOW());

-- Test writable CTE (Airbyte pattern)
WITH source AS (
  SELECT * FROM (VALUES (1, 'Alice Updated'), (3, 'Charlie')) AS t(id, name)
),
updates AS (
  UPDATE test_upsert SET name = s.name FROM source s WHERE test_upsert.id = s.id
)
INSERT INTO test_upsert (id, name)
SELECT id, name FROM source s
WHERE NOT EXISTS (SELECT 1 FROM test_upsert WHERE test_upsert.id = s.id);

-- Verify results
SELECT * FROM test_upsert ORDER BY id;
-- Expected: (1, 'Alice Updated'), (2, 'Bob'), (3, 'Charlie')
EOF
```

---

## Verification Checklist

- [ ] `go test ./transpiler/...` passes
- [ ] `go test ./server/...` passes
- [ ] Manual psql test with writable CTE works
- [ ] Airbyte sync with Dedupe mode succeeds
- [ ] Existing CTE queries still work (regression)
- [ ] Transactions work correctly (BEGIN before writable CTE)
- [ ] Error handling works (bad query rolls back)

---

## Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| **RETURNING semantic mismatch** | High | High | For Airbyte pattern: pre-capture rows is sufficient; document that SET expression evaluation in RETURNING may differ |
| **AST manipulation bugs** | Medium | High | Extensive unit tests; compare deparsed SQL with expected |
| **UPDATE→SELECT conversion errors** | Medium | High | Complex FROM/JOIN translation; test each UPDATE pattern |
| **Temp table cleanup on error** | Low | Medium | Transaction rollback auto-cleans temp tables |
| **Performance overhead** | Medium | Low | Extra SELECT + temp table per writable CTE; acceptable for correctness |
| **Breaking existing queries** | Low | High | Only activate for queries WITH writable CTEs; regression tests |
| **INSERT RETURNING with sequences** | High | Medium | Sequences/SERIAL may produce different values; document limitation |
| **Concurrent modification** | Low | Medium | Transaction isolation protects us; temp tables are session-local |

### Complexity Assessment

This feature is **significantly more complex** than initially estimated due to RETURNING semantics:

| Component | Complexity | LOC Estimate |
|-----------|------------|--------------|
| Writable CTE detection | Low | ~50 |
| Read-only CTE materialization | Low | ~50 |
| UPDATE→SELECT conversion | **High** | ~200 |
| DELETE→SELECT conversion | Medium | ~50 |
| INSERT→SELECT conversion | **High** | ~150 |
| Reference rewriting | Medium | ~100 |
| Multi-statement execution | Medium | ~100 |
| Transaction handling | Low | ~50 |
| **Total** | **High** | **~750 LOC** |

### Alternative: Airbyte-Specific Pattern Matching

Given the complexity, consider a **targeted approach** for the specific Airbyte pattern:

```sql
-- Airbyte's exact pattern:
WITH deduped AS (SELECT ... ROW_NUMBER() ... WHERE rn = 1),
     updates AS (UPDATE t SET ... FROM deduped WHERE ... RETURNING *)  -- RETURNING just for "what matched"
INSERT INTO t SELECT ... FROM deduped WHERE NOT EXISTS (SELECT 1 FROM updates ...)
```

For this pattern:
- The `updates` CTE output is only used in `NOT EXISTS` (checking if ID exists)
- We don't need full RETURNING semantics, just the primary key values
- Simpler: capture PKs of rows that will be updated, not full row data

This would reduce complexity but limit compatibility to Airbyte's specific pattern.

---

## Future Enhancements

1. **RETURNING clause support** - Capture DML results and return to client
2. **Optimization** - Skip temp table for single-use CTEs; inline instead
3. **Better error messages** - Map DuckDB errors back to original query positions
4. **Metrics** - Track writable CTE rewrites for observability

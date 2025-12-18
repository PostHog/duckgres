# PostgreSQL to DuckDB/DuckLake Query Compatibility Plan

This document outlines the query rewriting, routing, and compatibility layer needed to make PostgreSQL clients work seamlessly with DuckDB running on DuckLake.

## Progress Tracker

### Phase 1: Critical (Blocking Production) - ✅ COMPLETE

| Task | Status | PR |
|------|--------|-----|
| Fix DuckLake failover (don't silently fall back) | ✅ Done | [#31](https://github.com/PostHog/duckgres/pull/31) |
| Strip PRIMARY KEY from CREATE TABLE | ✅ Done | [#31](https://github.com/PostHog/duckgres/pull/31) |
| Strip UNIQUE from CREATE TABLE | ✅ Done | [#31](https://github.com/PostHog/duckgres/pull/31) |
| Strip FOREIGN KEY / REFERENCES | ✅ Done | [#31](https://github.com/PostHog/duckgres/pull/31) |
| Strip CHECK constraints | ✅ Done | [#31](https://github.com/PostHog/duckgres/pull/31) |
| CREATE INDEX → no-op | ✅ Done | [#31](https://github.com/PostHog/duckgres/pull/31) |
| DROP INDEX → no-op | ✅ Done | [#31](https://github.com/PostHog/duckgres/pull/31) |
| SERIAL → INTEGER | ✅ Done | [#31](https://github.com/PostHog/duckgres/pull/31) |
| BIGSERIAL → BIGINT | ✅ Done | [#31](https://github.com/PostHog/duckgres/pull/31) |
| SMALLSERIAL → SMALLINT | ✅ Done | [#31](https://github.com/PostHog/duckgres/pull/31) |
| Strip DEFAULT now() / current_timestamp | ✅ Done | [#31](https://github.com/PostHog/duckgres/pull/31) |
| ALTER TABLE ADD CONSTRAINT → no-op | ✅ Done | [#31](https://github.com/PostHog/duckgres/pull/31) |
| VACUUM / ANALYZE / CLUSTER → no-op | ✅ Done | [#31](https://github.com/PostHog/duckgres/pull/31) |
| GRANT / REVOKE / COMMENT → no-op | ✅ Done | [#31](https://github.com/PostHog/duckgres/pull/31) |

### Phase 2: ORM Compatibility - 🔲 TODO

| Task | Status | PR |
|------|--------|-----|
| Stub sequence functions (nextval, setval, currval) | 🔲 Todo | |
| CREATE SEQUENCE → no-op | 🔲 Todo | |
| Strip GENERATED columns | ✅ Done | [#31](https://github.com/PostHog/duckgres/pull/31) |

### Phase 3: Advanced ETL Compatibility - 🔲 TODO

| Task | Status | PR |
|------|--------|-----|
| ON CONFLICT → MERGE INTO rewrite | 🔲 Todo | |
| Sequence emulation with helper table | 🔲 Todo | |
| ENUM → VARCHAR rewrite | 🔲 Todo | |
| Array parameter handling (`= ANY($1::int[])`) | 🔲 Todo | |

### Phase 4: Full Compatibility - 🔲 TODO

| Task | Status | PR |
|------|--------|-----|
| Savepoints (SAVEPOINT/RELEASE/ROLLBACK TO) | 🔲 Todo | |
| Materialized views | 🔲 Todo | |
| Full text search stubs | 🔲 Todo | |

---

## Table of Contents

- [Progress Tracker](#progress-tracker)
- [Current Implementation](#current-implementation)
- [DuckLake Limitations](#ducklake-limitations)
- [Query Rewriting Requirements](#query-rewriting-requirements)
- [Query Routing Architecture](#query-routing-architecture)
- [Implementation Priority](#implementation-priority)
- [Detailed Implementation Guide](#detailed-implementation-guide)

---

## Current Implementation

The `duckgres` service (`server/catalog.go`) currently handles:

### Query Rewrites
| Pattern | Rewrite To | Purpose |
|---------|------------|---------|
| `pg_catalog.pg_class` | `pg_class_full` | Custom view with extra columns |
| `pg_catalog.pg_database` | `pg_database` | Custom view |
| `::pg_catalog.regtype` | `::VARCHAR` | Type cast compatibility |
| `OPERATOR(pg_catalog.~)` | `~` | Operator syntax |
| `COLLATE pg_catalog.default` | (removed) | Collation compatibility |
| `SET application_name = X` | `SET VARIABLE application_name = X` | DuckDB variable syntax |
| `version()` | `'PostgreSQL 15.0 ...'` | Version string compatibility |

### Function Macros (created in `initPgCatalog`)
- `pg_get_userbyid()`, `pg_table_is_visible()`, `format_type()`
- `obj_description()`, `col_description()`, `pg_get_expr()`
- `current_setting()`, `pg_is_in_recovery()`, `has_schema_privilege()`

### Ignored SET Parameters
70+ PostgreSQL-specific SET commands are silently acknowledged:
- `statement_timeout`, `lock_timeout`, `client_encoding`
- `work_mem`, `enable_*` planner settings
- `transaction_isolation`, `synchronous_commit`, etc.

---

## DuckLake Limitations

Based on [DuckLake documentation](https://ducklake.select/docs/stable/duckdb/unsupported_features):

### Constraints

| Constraint | Support | Notes |
|------------|---------|-------|
| `NOT NULL` | Supported | Only constraint that works |
| `PRIMARY KEY` | Never | Prohibitively expensive in data lakes |
| `UNIQUE` | Never | Same reason |
| `FOREIGN KEY` | Never | Same reason |
| `CHECK` | Likely future | Currently fails |

### DDL Features

| Feature | Support | Notes |
|---------|---------|-------|
| `CREATE INDEX` | Never | No index support in DuckLake |
| `CREATE SEQUENCE` | Never | No sequence support |
| `GENERATED` columns | Likely future | Currently fails |
| `DEFAULT now()` | No | Only literal defaults allowed |
| `DROP ... CASCADE` | No | Doesn't drop dependent objects |
| Macros | Workaround | Must create in catalog DB |

### Data Types

| Type | Support | Notes |
|------|---------|-------|
| `SERIAL` / `BIGSERIAL` | No | Must convert to INTEGER/BIGINT |
| `ENUM` | Likely future | Currently fails |
| `VARINT` | No | Not supported |
| `BITSTRING` | No | Not supported |
| Fixed-size `ARRAY` | Likely future | Currently fails |

### DML Features

| Feature | Support | Notes |
|---------|---------|-------|
| `INSERT ... RETURNING` | Yes | Works via DuckDB |
| `ON CONFLICT DO UPDATE` | No | Requires PRIMARY KEY; use `MERGE INTO` |
| `UPSERT` | No | Use `MERGE INTO` syntax |
| Multi-row UPDATE same row | No | Not supported |

---

## Query Rewriting Requirements

### 1. Constraint Stripping

PostgreSQL clients (ORMs, ETL tools) send constraint definitions that must be stripped:

```sql
-- Input:
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    org_id INTEGER REFERENCES orgs(id),
    status VARCHAR CHECK (status IN ('active', 'inactive')),
    created_at TIMESTAMP DEFAULT now()
);

-- Output:
CREATE TABLE users (
    id INTEGER NOT NULL,
    email VARCHAR(255) NOT NULL,
    org_id INTEGER,
    status VARCHAR,
    created_at TIMESTAMP
);
```

**Regex patterns needed:**
```go
primaryKeyRegex     = regexp.MustCompile(`(?i)\s+PRIMARY\s+KEY`)
uniqueRegex         = regexp.MustCompile(`(?i)\s+UNIQUE`)
foreignKeyRegex     = regexp.MustCompile(`(?i)\s+REFERENCES\s+\w+(?:\s*\([^)]+\))?`)
checkConstraintRegex = regexp.MustCompile(`(?i)\s+CHECK\s*\([^)]+\)`)
serialRegex         = regexp.MustCompile(`(?i)\bSERIAL\b`)
bigserialRegex      = regexp.MustCompile(`(?i)\bBIGSERIAL\b`)
defaultNowRegex     = regexp.MustCompile(`(?i)DEFAULT\s+(?:now\(\)|current_timestamp|CURRENT_TIMESTAMP)`)
```

### 2. CREATE INDEX → No-op

All index creation must be silently accepted without execution:

```sql
-- Input (all become no-ops):
CREATE INDEX idx_users_email ON users(email);
CREATE UNIQUE INDEX idx_users_id ON users(id);
CREATE INDEX CONCURRENTLY idx_orders_date ON orders(created_at);
```

**Implementation:**
```go
if cmdType == "CREATE INDEX" {
    log.Printf("[%s] Ignoring CREATE INDEX (DuckLake limitation): %s", c.username, query)
    writeCommandComplete(c.writer, "CREATE INDEX")
    return nil
}
```

### 3. SERIAL/BIGSERIAL → INTEGER/BIGINT

```sql
-- Input:
CREATE TABLE orders (id BIGSERIAL, ...);

-- Output:
CREATE TABLE orders (id BIGINT, ...);
```

**Note:** PostgreSQL SERIAL creates a sequence + DEFAULT. Without sequences, auto-increment isn't possible. Options:
- A) Accept the table creation, let application handle ID generation (UUIDs)
- B) Emulate sequences with a helper table

### 4. ON CONFLICT → MERGE INTO

This is the most complex rewrite:

```sql
-- Input (PostgreSQL upsert):
INSERT INTO users (id, name, email)
VALUES (1, 'Alice', 'alice@example.com')
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    email = EXCLUDED.email;

-- Output (DuckLake MERGE):
MERGE INTO users AS target
USING (SELECT 1 AS id, 'Alice' AS name, 'alice@example.com' AS email) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET
    name = source.name,
    email = source.email
WHEN NOT MATCHED THEN INSERT (id, name, email)
    VALUES (source.id, source.name, source.email);
```

**Complexity:** Requires SQL parsing, not just regex. Consider using a SQL parser library.

### 5. Sequence Functions → Stub/Emulate

```sql
-- These must be handled:
CREATE SEQUENCE users_id_seq;        -- No-op or emulate
SELECT nextval('users_id_seq');      -- Emulate with table
SELECT setval('users_id_seq', 100);  -- Emulate with table
SELECT currval('users_id_seq');      -- Emulate with table
```

**Emulation approach:**
```sql
-- Create helper table once:
CREATE TABLE IF NOT EXISTS _duckgres_sequences (
    name VARCHAR PRIMARY KEY,
    current_value BIGINT NOT NULL DEFAULT 0
);

-- nextval becomes:
UPDATE _duckgres_sequences SET current_value = current_value + 1 WHERE name = 'users_id_seq';
SELECT current_value FROM _duckgres_sequences WHERE name = 'users_id_seq';
```

### 6. Table Constraint Definitions

Handle `ALTER TABLE ADD CONSTRAINT`:

```sql
-- Input:
ALTER TABLE users ADD CONSTRAINT users_pkey PRIMARY KEY (id);
ALTER TABLE users ADD CONSTRAINT users_email_unique UNIQUE (email);
ALTER TABLE orders ADD CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES users(id);

-- Output: All become no-ops, return success
```

---

## Query Routing Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    PostgreSQL Wire Protocol                      │
│                         (port 5432)                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Query Classifier                            │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐   │
│  │   DDL   │ │   DML   │ │   DQL   │ │   TCL   │ │  Other  │   │
│  │ CREATE  │ │ INSERT  │ │ SELECT  │ │  BEGIN  │ │   SET   │   │
│  │  ALTER  │ │ UPDATE  │ │  WITH   │ │ COMMIT  │ │  SHOW   │   │
│  │  DROP   │ │ DELETE  │ │  TABLE  │ │ROLLBACK │ │  COPY   │   │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│    REWRITE    │     │    NO-OP      │     │ PASS-THROUGH  │
│  (transform)  │     │   (ignore)    │     │   (execute)   │
└───────────────┘     └───────────────┘     └───────────────┘
        │                     │                     │
        ▼                     ▼                     ▼
• SERIAL → INTEGER    • CREATE INDEX       • SELECT
• Strip PRIMARY KEY   • CREATE UNIQUE      • INSERT (simple)
• Strip UNIQUE          INDEX              • UPDATE
• Strip FOREIGN KEY   • GRANT / REVOKE     • DELETE
• Strip CHECK         • ANALYZE            • CREATE TABLE
• DEFAULT now() →     • VACUUM             • CREATE SCHEMA
  NULL or remove      • CLUSTER            • ALTER (supported)
• ON CONFLICT →       • REINDEX            • DROP TABLE/SCHEMA
  MERGE INTO          • REFRESH MATVIEW    • BEGIN/COMMIT
• Sequence funcs      • ALTER CONSTRAINT   • TRUNCATE
                      • COMMENT ON
```

---

## Implementation Priority

### Phase 1: Critical (Blocking Production)

| Task | Effort | Impact |
|------|--------|--------|
| Fix DuckLake failover (don't silently fall back) | Low | Critical |
| Strip PRIMARY KEY from CREATE TABLE | Low | High |
| Strip UNIQUE from CREATE TABLE | Low | High |
| Strip FOREIGN KEY / REFERENCES | Low | High |
| CREATE INDEX → no-op | Low | High |
| SERIAL/BIGSERIAL → INTEGER/BIGINT | Low | High |

### Phase 2: ORM Compatibility

| Task | Effort | Impact |
|------|--------|--------|
| Stub sequence functions (nextval, setval) | Medium | High |
| Strip CHECK constraints | Low | Medium |
| Handle DEFAULT now() | Low | Medium |
| ALTER TABLE ADD CONSTRAINT → no-op | Low | Medium |
| Strip GENERATED columns | Low | Medium |

### Phase 3: Advanced ETL Compatibility

| Task | Effort | Impact |
|------|--------|--------|
| ON CONFLICT → MERGE rewrite | High | High |
| Sequence emulation with table | Medium | Medium |
| ENUM → VARCHAR rewrite | Medium | Low |
| Array parameter handling | High | Medium |

### Phase 4: Full Compatibility

| Task | Effort | Impact |
|------|--------|--------|
| Savepoints (SAVEPOINT/RELEASE) | Medium | Low |
| Materialized views | High | Low |
| Full text search stubs | Medium | Low |

---

## Detailed Implementation Guide

### Fix DuckLake Failover

**File:** `server/server.go`

**Current behavior:** If DuckLake attachment fails, server silently falls back to local DuckDB database, causing schema/table mismatches.

**Fix:**
```go
// In createDBConnection():
if err := s.attachDuckLake(db); err != nil {
    if s.cfg.DuckLake.MetadataStore != "" {
        // DuckLake was explicitly configured - don't silently fall back
        db.Close()
        return nil, fmt.Errorf("DuckLake configured but attachment failed: %w", err)
    }
    // DuckLake not configured, this is fine
    log.Printf("DuckLake not configured, using local database")
}
```

### Constraint Stripping Function

**File:** `server/catalog.go`

```go
// rewriteCreateTable strips unsupported constraints from CREATE TABLE statements
func rewriteCreateTable(query string) string {
    // Only apply to CREATE TABLE statements
    if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "CREATE") {
        return query
    }

    // Strip PRIMARY KEY
    query = regexp.MustCompile(`(?i)\s+PRIMARY\s+KEY`).ReplaceAllString(query, "")

    // Strip UNIQUE
    query = regexp.MustCompile(`(?i)\s+UNIQUE`).ReplaceAllString(query, "")

    // Strip REFERENCES (foreign keys)
    query = regexp.MustCompile(`(?i)\s+REFERENCES\s+\w+(?:\s*\([^)]+\))?(?:\s+ON\s+(?:DELETE|UPDATE)\s+\w+)*`).ReplaceAllString(query, "")

    // Strip CHECK constraints
    query = regexp.MustCompile(`(?i)\s+CHECK\s*\([^)]+\)`).ReplaceAllString(query, "")

    // SERIAL → INTEGER
    query = regexp.MustCompile(`(?i)\bSERIAL\b`).ReplaceAllString(query, "INTEGER")
    query = regexp.MustCompile(`(?i)\bBIGSERIAL\b`).ReplaceAllString(query, "BIGINT")
    query = regexp.MustCompile(`(?i)\bSMALLSERIAL\b`).ReplaceAllString(query, "SMALLINT")

    // Strip DEFAULT now()/current_timestamp
    query = regexp.MustCompile(`(?i)\s+DEFAULT\s+(?:now\(\)|current_timestamp|CURRENT_TIMESTAMP)`).ReplaceAllString(query, "")

    // Strip GENERATED columns
    query = regexp.MustCompile(`(?i)\s+GENERATED\s+(?:ALWAYS|BY\s+DEFAULT)\s+AS\s+(?:IDENTITY|[^,)]+)`).ReplaceAllString(query, "")

    return query
}
```

### No-op Commands List

**File:** `server/conn.go`

```go
var noOpCommands = map[string]bool{
    "CREATE INDEX":        true,
    "CREATE UNIQUE INDEX": true,
    "DROP INDEX":          true,
    "REINDEX":             true,
    "CLUSTER":             true,
    "VACUUM":              true,
    "ANALYZE":             true,
    "GRANT":               true,
    "REVOKE":              true,
    "COMMENT":             true,
    "REFRESH":             true,  // REFRESH MATERIALIZED VIEW
}

// In handleQuery/handleExecute:
if noOpCommands[cmdType] {
    log.Printf("[%s] No-op command (DuckLake limitation): %s", c.username, cmdType)
    writeCommandComplete(c.writer, cmdType)
    return nil
}
```

---

## Testing Strategy

### Unit Tests

```go
func TestConstraintStripping(t *testing.T) {
    tests := []struct {
        input    string
        expected string
    }{
        {
            input:    "CREATE TABLE t (id SERIAL PRIMARY KEY)",
            expected: "CREATE TABLE t (id INTEGER)",
        },
        {
            input:    "CREATE TABLE t (email VARCHAR UNIQUE NOT NULL)",
            expected: "CREATE TABLE t (email VARCHAR NOT NULL)",
        },
        {
            input:    "CREATE TABLE t (user_id INT REFERENCES users(id) ON DELETE CASCADE)",
            expected: "CREATE TABLE t (user_id INT)",
        },
    }
    // ...
}
```

### Integration Tests

1. Connect with lib/pq
2. Run Fivetran/Airbyte test patterns
3. Run ORM migrations (Rails, Django, SQLAlchemy)
4. Verify all commands return correct command tags

---

## References

- [DuckLake Unsupported Features](https://ducklake.select/docs/stable/duckdb/unsupported_features)
- [DuckLake Constraints](https://ducklake.select/docs/stable/duckdb/advanced_features/constraints)
- [DuckLake Schema Evolution](https://ducklake.select/docs/stable/duckdb/usage/schema_evolution)
- [DuckLake Architecture Blog](https://ducklake.select/2025/05/27/ducklake-01/)
- [DuckDB DuckLake Extension](https://duckdb.org/docs/stable/core_extensions/ducklake.html)

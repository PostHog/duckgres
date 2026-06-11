# PostgreSQL Compatibility Matrix

This is the canonical, feature-by-feature record of **what PostgreSQL functionality
Duckgres supports and which tests prove it.** It exists so that "is feature X
supported?" and "where's the test / where's the gap?" have one answer instead of
being smeared across the README and test docs.

Duckgres speaks the PostgreSQL wire protocol but executes on **DuckDB**, an OLAP
engine. So the compatibility target is "PostgreSQL semantics for analytical
workloads," not full OLTP parity. Many gaps below are deliberate (DuckDB has no
SAVEPOINT, no triggers, no sequences) rather than unfinished.

## Status legend

| Symbol | Meaning |
|--------|---------|
| ✅ **Covered** | Works, and a **differential** test asserts it (same query run against real PostgreSQL 16 *and* Duckgres, results compared) — `tests/integration/`. |
| 🟡 **Partial** | Works, but only **unit/transpiler**-tested (no differential assertion), or differential-covered with notable cases skipped. |
| ⚠️ **Implemented, thin/untested** | Code path exists and is reachable by clients, but no real test exercises the happy path. **These are the real test gaps.** |
| ❌ **Unsupported** | Not implemented and/or not tested; behavior undefined for clients that rely on it. |
| ⛔ **Out of scope** | Intentionally unsupported (DuckDB limitation or OLTP feature). Often has a *skipped* test documenting the reason. |

Test citations use `file.go::TestName`. Integration (differential) tests live in
`tests/integration/`; unit tests in `server/` and `transpiler/`; real-driver tests
in `scripts/client-compat/` and `tests/integration/clients/`.

## Maintenance contract

Per [CLAUDE.md](../CLAUDE.md), every behavior change ships with a test. **When that
change adds, removes, or changes a PostgreSQL-visible feature, update the matching
row here in the same PR** — flip the status, fix the test citation, or add the row.
A row that points at a real test name rots slower than prose; keep the citations
honest. If you mark something ⛔ Out of scope, link the skipped test that documents
why.

---

## 1. Queries (DQL)

| Feature | Status | Test(s) | Notes |
|---------|--------|---------|-------|
| SELECT, projections, aliases, arithmetic, `\|\|` | ✅ | `dql_test.go::TestDQLBasicSelect` | |
| WHERE: comparisons, `IS [NOT] DISTINCT FROM`, AND/OR/NOT | ✅ | `dql_test.go::TestDQLWhere` | |
| `IN` / `NOT IN` / `BETWEEN` | ✅ | `dql_test.go::TestDQLWhere` | |
| `LIKE` / `ILIKE` / regex `~ ~* !~` | ✅ | `dql_test.go::TestDQLWhere` | |
| `SIMILAR TO` | ⛔ | `dql_test.go` (skipped) | SkipUnsupportedByDuckDB |
| `ANY` / `ALL` (array) | ✅ | `dql_test.go::TestDQLWhere`, `types_test.go::TestTypesArray` | |
| ORDER BY (position/alias/expr, ASC/DESC, NULLS FIRST/LAST) | ✅ | `dql_test.go::TestDQLOrderBy` | |
| LIMIT / OFFSET / FETCH FIRST…ONLY | ✅ | `dql_test.go::TestDQLLimitOffset` | |
| DISTINCT / DISTINCT ON | ✅ | `dql_test.go::TestDQLDistinct`; transform `wiring_ops_test.go::TestOperatorTransform_DistinctOn` | |
| GROUP BY / HAVING | ✅ | `dql_test.go::TestDQLGroupBy` | |
| GROUPING SETS / ROLLUP / CUBE | ✅ | `dql_test.go::TestDQLGroupBy` | |
| Joins: INNER/LEFT/RIGHT/FULL/CROSS/self/LATERAL | ✅ | `dql_test.go::TestDQLJoins` | |
| NATURAL JOIN / JOIN USING | 🟡 | `dql_test.go::TestDQLJoins` (skipped) | Skipped on fixture column-name mismatch, not a Duckgres gap |
| Subqueries: scalar / correlated / EXISTS / IN | ✅ | `dql_test.go::TestDQLSubqueries` | |
| CTEs / RECURSIVE / chained | ✅ | `dql_test.go::TestDQLCTEs` | |
| Writable (data-modifying) CTE | ✅ | `dml_test.go::TestDMLWithCTE`; transpiler `writablecte_returning_test.go` | |
| UNION / INTERSECT / EXCEPT (+ ALL) | ✅ | `dql_test.go::TestDQLSetOperations` | |
| Window functions (ranking, offset, value, frames, named windows, distribution) | ✅ | `dql_test.go::TestDQLWindowFunctions`; transform `wiring_ops_test.go::TestOperatorTransform_InlineWindowDef`/`_NamedWindowClause` | Thorough: ROW_NUMBER/RANK/LAG/LEAD/FIRST_VALUE/NTILE/PERCENT_RANK/frames |
| VALUES (standalone / subquery / CTE) | ✅ | `dql_test.go::TestDQLValues`; transform `wiring_ops_test.go::TestOperatorTransform_ValuesLists` | |
| Quoted identifiers / case sensitivity | ✅ | `dql_test.go::TestDQLCaseSensitivity`, `edge_cases_test.go::TestQuotedIdentifiers` | |
| Complex multi-CTE/join/recursive queries | ✅ | `dql_test.go::TestDQLComplexQueries` | |
| `TABLE t` command | ⛔ | `dql_test.go` (skipped) | SkipUnsupportedByDuckDB |
| `TABLESAMPLE` (PG syntax) | ❌ | — | DuckDB `USING SAMPLE` works via fallback (`fallback_test.go`); PG `TABLESAMPLE` not asserted |

---

## 2. Data Manipulation (DML)

| Feature | Status | Test(s) | Notes |
|---------|--------|---------|-------|
| INSERT (single / multi-row / INSERT…SELECT / DEFAULT / NULL) | ✅ | `dml_test.go::TestDMLInsert` | |
| UPDATE (incl. `UPDATE…FROM`) | ✅ | `dml_test.go::TestDMLUpdate`, `::TestDMLUpdateFromJoin` | |
| DELETE (incl. `DELETE…USING`, subquery) | ✅ | `dml_test.go::TestDMLDelete`, `::TestDMLDeleteUsing`; transform `wiring_ops_test.go::TestOperatorTransform_DeleteUsing` | |
| RETURNING (simple query protocol) | 🟡 | transform `wiring_ops_test.go::TestOperatorTransform_{Insert,Update,Delete}Returning`; `conn_test.go::TestContainsReturning`/`::TestIsDMLReturning` | Differential tests `TestDML{Insert,Update,Delete}Returning` are `skipIfKnown` (stale — passes at unit/client level) |
| RETURNING (extended query protocol) | ⛔ | `conn_test.go::TestIsDMLReturning` | Rejected at Describe time with `0A000` by design — Describe would execute the mutation. See CLAUDE.md "DML RETURNING Detection" |
| ON CONFLICT / UPSERT (DO UPDATE) | ✅ | `dml_test.go::TestDMLInsertOnConflict`; transpiler `transform/onconflict_test.go` | Also exercised via DuckLake ON CONFLICT→MERGE rewrite: `ducklake_concurrency_test.go::TestDuckLakeConcurrentTransactions` |
| ON CONFLICT DO NOTHING | 🟡 | `dml_test.go::TestDMLInsertOnConflict` (subtest skipped) | |
| MERGE (user-facing) | ⛔ | — | DuckDB has no MERGE; only internal ON CONFLICT→MERGE rewrite exists |
| TRUNCATE | ✅ | `ddl_test.go::TestDDLTruncate` | |
| COPY … FROM STDIN (text/CSV) | ✅ | `copy_test.go::TestCopyFromStdin`, `::TestCopyFromStdinWithSpecialChars`, `::TestCopyFromStdinMultilineJSON` | Escape sequences stored literally (documented DuckDB CSV-parser limitation) |
| COPY … TO STDOUT | 🟡 | `copy_test.go::TestCopyToStdout` (skipped under lib/pq); `conn_test.go::TestCopyToStdoutRegex`; client-compat `psycopg` COPY suite | Integration skip is a lib/pq driver limitation, not a Duckgres gap |
| COPY binary format | 🟡 | `conn_test.go::TestShouldHandleCopyBeforeTranspile`; `types_test.go` encode/decode | Unit-level only |

---

## 3. Data Definition (DDL)

| Feature | Status | Test(s) | Notes |
|---------|--------|---------|-------|
| CREATE/DROP TABLE (IF [NOT] EXISTS, CASCADE/RESTRICT) | ✅ | `ddl_test.go::TestDDLCreateTable`, `::TestDDLDropTable` | |
| Constraints: PK / FK / UNIQUE / CHECK / NOT NULL / DEFAULT | ✅ | `ddl_test.go::TestDDLConstraints` | Enforcement follows DuckDB semantics |
| Generated columns | 🟡 | transpiler `transform/ddl_test.go` (GENERATED detection) | No differential test |
| CREATE TABLE AS / TEMP TABLE | ✅ | `ddl_test.go::TestDDLCreateTable` | |
| ALTER TABLE add/drop/rename column, rename table | ✅ | `ddl_test.go::TestDDLAlterTable` | |
| VIEW (CREATE OR REPLACE, column aliases) | ✅ | `ddl_test.go::TestDDLViews` | |
| Materialized views | ⛔ | — | `pg_matviews` is an empty stub |
| INDEX (unique, multi-col, IF [NOT] EXISTS) | 🟡 | `ddl_test.go::TestDDLIndexes` | Accepted, but a no-op in DuckDB/DuckLake mode — not semantically asserted |
| SCHEMA (create/drop/cascade) | ✅ | `ddl_test.go::TestDDLSchemas` | |
| SEQUENCE / SERIAL / nextval/currval | ⛔ | `catalog_test.go::TestCatalogPgGetSerialSequence` | `pg_get_serial_sequence` returns NULL — not supported |
| TYPE / ENUM / DOMAIN / composite | ❌ | — | Unmapped DuckDB ENUM/STRUCT/etc. fall back to OidText |
| COMMENT ON table/column | ✅ | `ddl_test.go::TestDDLComment` | |
| Partitioning / inheritance | ⛔ | — | `pg_partitioned_table`, `pg_inherits` are empty stubs |

---

## 4. Data Types

| Type | Status | Test(s) | Notes |
|------|--------|---------|-------|
| smallint / int / bigint | ✅ | `types_test.go::TestTypesNumeric`; `types_test.go(server)::TestEncode/DecodeInt2/4/8` | |
| real / double | ✅ | `types_test.go::TestTypesNumeric`; server `TestEncode/DecodeFloat4/8` | incl. NaN/±Infinity |
| numeric / decimal | ✅ | `types_test.go::TestTypesNumeric`; server `TestEncodeNumeric`/`TestDecodeNumeric` | typmod precision/scale handled |
| char / varchar / text | ✅ | `types_test.go::TestTypesCharacter`, `::TestUnicodeAndSpecialData` | unicode, emoji, escapes |
| bytea | ✅ | `types_test.go::TestTypesBinary`; server `TestEncode/DecodeBytea` | escape-format variant skipped (SkipDifferentBehavior) |
| date / time / timestamp / timestamptz / interval | ✅ | `types_test.go::TestTypesDateTime`; server `TestEncode/DecodeDate/Timestamp/Time/Interval` | incl. ancient dates, microseconds |
| boolean | ✅ | `types_test.go::TestTypesBoolean`; server `TestEncode/DecodeBool` | all literal forms (t/f/yes/no/1/0) |
| uuid | ✅ | `types_test.go::TestTypesUUID`; server `TestEncodeDecodeUUID` | |
| json / jsonb | ✅ | `types_test.go::TestTypesJSON`; server `TestEncodeJSON`/`TestEncodeBinaryJSON` | operators `-> ->> @> ?`; JSONPath `@?` skipped |
| arrays | ✅ | `types_test.go::TestTypesArray` | subscript, slice, concat, contains, ANY/ALL |
| NULL handling (all types) | ✅ | `types_test.go::TestTypesNullHandling` | |
| Casts (`CAST`, `::`, implicit) | ✅ | `types_test.go::TestTypesCasting` | |
| money | ⛔ | `types_test.go::TestTypesUnsupported` | poorly supported in DuckDB |
| inet / cidr / macaddr | ⛔ | `types_test.go::TestTypesUnsupported` | SkipNetworkType |
| point / line / box (geometric) | ⛔ | `types_test.go::TestTypesUnsupported` | SkipGeometricType |
| int4range / daterange (range/multirange) | ⛔ | `types_test.go::TestTypesUnsupported` | SkipRangeType |
| tsvector / tsquery (full-text) | ⛔ | `types_test.go::TestTypesUnsupported` | SkipTextSearch |
| enum / domain / composite | ❌ | — | falls back to OidText |
| bit string | 🟡 | `functions_test.go::TestFunctionsString` (`bit_length` only) | no bit-type operations |
| xml | ❌ | — | not tested |
| OID types (oid / regclass / …) | 🟡 | used within `catalog_test.go` queries | not type-tested directly |

---

## 5. Functions & Operators

| Group | Status | Test(s) | Notes |
|-------|--------|---------|-------|
| String (length/case/trim/pad/substring/replace/split/format/regexp/md5/quote_*) | ✅ | `functions_test.go::TestFunctionsString` | broad |
| Math (abs/round/trunc/mod/power/sqrt/trig/log/width_bucket) | ✅ | `functions_test.go::TestFunctionsNumeric` | |
| Date/time (extract/date_part/date_trunc/age/to_char/make_*) | ✅ | `functions_test.go::TestFunctionsDateTime` | |
| Conditional (CASE/COALESCE/NULLIF/GREATEST/LEAST) | ✅ | `functions_test.go::TestFunctionsConditional` | |
| Aggregates (count/sum/avg/min/max/stddev/variance/bool_*/bit_*/array_agg/string_agg/json_agg) | ✅ | `functions_test.go::TestFunctionsAggregate` | |
| Aggregate `FILTER` and `WITHIN GROUP` (percentile/mode) | ✅ | `functions_test.go::TestFunctionsAggregate`; transform `wiring_ops_test.go::TestOperatorTransform_AggFilter`/`_AggOrder` | |
| Set-returning (generate_series, unnest, json_each, *_array_elements) | ✅ | `functions_test.go::TestFunctionsMisc`, `::TestFunctionsJSON`, `::TestFunctionsArray` | |
| JSON build/extract/modify functions | ✅ | `functions_test.go::TestFunctionsJSON` | |
| Array functions | ✅ | `functions_test.go::TestFunctionsArray` | |
| System info (current_database/schema/user, version, pg_typeof) | ✅ | `functions_test.go::TestFunctionsMisc`, `session_test.go::TestSessionCurrentFunctions` | |

---

## 6. Wire Protocol

| Feature | Status | Test(s) | Notes |
|---------|--------|---------|-------|
| Simple query | ✅ | `protocol_test.go::TestProtocolSimpleQuery` | |
| Extended query (Parse/Bind/Describe/Execute/Sync) | ✅ | `protocol_test.go::TestProtocolExtendedQuery`; server `conn_bind_test.go`, `conn_describe_test.go` | |
| Extended-query error recovery (skip-until-Sync, pipelining) | ✅ | server `conn_skip_until_sync_test.go`; clients `clients_test.go::TestExtendedQueryErrorHandling` | #718 |
| Prepared statements (PREPARE/EXECUTE, reuse, NULL, 20+ params) | ✅ | `edge_cases_test.go::TestPreparedStatementEdgeCases`; clients `::TestPreparedStatements` | |
| Binary vs text result format | ✅ | `protocol_test.go::TestProtocolDataTypes`; clients `::TestPgxBinaryFormatResults`; server `types_test.go` encode/decode | |
| Row description metadata | ✅ | `protocol_test.go::TestProtocolRowDescription` | |
| Large result sets (1000+ rows, wide rows, 100KB values) | ✅ | `protocol_test.go::TestProtocolLargeResults` | |
| Query cancellation (CancelRequest) | ✅ | `cancel_test.go::TestCancelQueryDoesNotAffectOtherSessions` | |
| Error / notice + recovery (in & out of txn) | ✅ | `protocol_test.go::TestProtocolErrors`, `edge_cases_test.go::TestErrorRecovery` | |
| Empty / comment-only queries | ✅ | `edge_cases_test.go::TestEmptyQuery`, `::TestPgxPing` | |
| Multi-statement simple query | ✅ | `protocol_test.go::TestProtocolMultipleStatements`, `edge_cases_test.go::TestMultiStatementBehavior` | |
| **Cursors: DECLARE / FETCH / MOVE** | ⚠️ | server `conn_querylog_feedback_test.go::TestHandleFetchCursorLogsMissingCursorError` (error path only) | **Gap.** Implemented in `server/conn_cursor.go` (FETCH is forward-only); no differential DECLARE→FETCH happy-path test |
| Cursor CLOSE | 🟡 | `conn_test.go` (CLOSE detection) | |
| Auth: cleartext password | ✅ | server `worker_auth_test.go`; integration via connection params | |
| Auth: MD5 | ❌ | — | Current startup path requests cleartext password auth over TLS |
| Auth: SCRAM-SHA-256 | ❌ | — | not tested |
| TLS / SSL (`sslmode=require`) | ✅ | `edge_cases_test.go::TestConnectionParameters` | |

---

## 7. Session, Config & Transactions

| Feature | Status | Test(s) | Notes |
|---------|--------|---------|-------|
| SET / SHOW / RESET / RESET ALL / DISCARD | ✅ | `session_test.go::TestSessionSetCommands`, `::TestSessionShowCommands`, `::TestSessionReset`, `::TestSessionMiscCommands` | ~20 GUCs accepted (many safely ignored) |
| search_path | ✅ | `session_test.go::TestSessionSetSearchPath` | |
| current_database / current_schema / current_user / session_user | ✅ | `session_test.go::TestSessionCurrentFunctions` | |
| BEGIN / COMMIT / ROLLBACK / START TRANSACTION / END | ✅ | `session_test.go::TestSessionTransactionCommands`, `protocol_test.go::TestProtocolTransactions` | |
| Isolation levels / READ ONLY / SESSION CHARACTERISTICS | 🟡 | `session_test.go::TestSessionTransactionModes` | Parsed & accepted; DuckDB always runs snapshot isolation (≈ serializable). `SHOW transaction_isolation` returns `read committed` for compat |
| SAVEPOINT / ROLLBACK TO / RELEASE | ⛔ | `edge_cases_test.go::TestSavepoints` (skipped) | DuckDB has no SAVEPOINT — affects Django/Rails nested-txn patterns |
| SELECT FOR UPDATE / FOR SHARE | 🟡 | transform `wiring_ops_test.go::TestLockingTransform_*`; `transpiler_test.go` (FlagLocking) | Locking clause is stripped/accepted (no-op); no differential test |
| Advisory locks (`pg_advisory_*`) | ❌ | — | not tested |
| LOCK TABLE | ❌ | — | not tested |
| Two-phase commit (PREPARE TRANSACTION) | ❌ | — | not tested |
| SET ROLE / SET SESSION AUTHORIZATION | ❌ | — | not tested |

---

## 8. System Catalog & Introspection

| Feature | Status | Test(s) | Notes |
|---------|--------|---------|-------|
| pg_class | ✅ | `catalog_test.go::TestCatalogPgClass` | DuckLake variant sources from `duckdb_tables()`/`duckdb_views()` |
| pg_namespace | ✅ | `catalog_test.go::TestCatalogPgNamespace` | maps `main`→`public` |
| pg_attribute | ✅ | `catalog_test.go::TestCatalogPgAttribute` | |
| pg_type | ✅ | `catalog_test.go::TestCatalogPgType` | synthetic entries for json/jsonb/text/array/… |
| pg_database | ✅ | `catalog_test.go::TestCatalogPgDatabase` | |
| pg_roles | ✅ | `catalog_test.go::TestCatalogPgRoles` | single hardcoded superuser |
| pg_settings | ✅ | `catalog_test.go::TestCatalogPgSettings` | |
| pg_stat_activity | ✅ | `pg_stat_activity_test.go::TestPgStatActivity`/`::TestPgStatActivityFromSecondConnection`/`::TestPgStatActivityExtendedQuery`/`::TestPgStatActivityStubView` | intercepted at query time for live data |
| information_schema (tables/columns/views/schemata) | ✅ | `catalog_test.go::TestCatalogInformationSchema{Tables,Columns,Views,Schemata}` | |
| information_schema key_column_usage / table_constraints / referential_constraints | ❌ | — | Missing; used by ORMs for FK introspection |
| System functions (format_type, pg_get_userbyid, pg_table_is_visible, has_*_privilege, pg_encoding_to_char, size fns, quote_*) | ✅ | `catalog_test.go::TestCatalogSystemFunctions`, `::TestFormatTypeTimePrecision`; server `pg_compat_macros_test.go` | many return permissive/stub values |
| psql meta-commands (`\dt`, `\dn`, `\l`, `\d`) | ✅ | `catalog_test.go::TestCatalogPsqlCommands` | |
| Qualified-name resolution | ✅ | `catalog_test.go::TestCatalogQualifiedNames`, `::TestCatalogCombinedQueries` | |
| Catalog not masked by user data | ✅ | `catalog_demask_test.go::TestCatalogIsNotMasked` | |
| Stub tables return empty (pg_policy/collation/publication/inherits/rules/matviews/stat_statements/partitioned_table/rewrite) | ✅ | `catalog_test.go::TestCatalogStubs` | intentional |
| Client/BI introspection (Metabase/Grafana/Superset/Tableau/DBeaver/Fivetran/Airbyte/dbt) | ✅ | `clients/clients_test.go::Test{Metabase,Grafana,Superset,Tableau,DBeaver,Fivetran,Airbyte,Dbt}Queries`; `jdbc_test.go::TestJDBC*` | + `scripts/client-compat/queries.yaml` (100+ catalog queries) |

---

## 9. Procedural / Server-Side — all ⛔ (DuckDB does not support)

| Feature | Status | Test(s) | Notes |
|---------|--------|---------|-------|
| PL/pgSQL, CREATE FUNCTION/PROCEDURE, CALL | ⛔ | — | no server-side procedural language |
| Triggers | ⛔ | — | |
| Rules | ⛔ | — | `pg_rules` empty stub |
| LISTEN / NOTIFY | ⛔ | — | |
| Event triggers | ⛔ | — | |

---

## 10. Security & Roles

| Feature | Status | Test(s) | Notes |
|---------|--------|---------|-------|
| GRANT / REVOKE, column/default privileges | ❌ | — | `has_*_privilege()` return permissive stubs; privilege DDL untested |
| CREATE / ALTER / DROP ROLE / USER | ❌ | — | |
| Row-level security (RLS) | ⛔ | — | `pg_policy` empty stub |

---

## 11. DuckDB-Specific Syntax (pass-through, non-PostgreSQL)

Not PostgreSQL features, but exercised because clients may send them and Duckgres
must route them to native DuckDB execution rather than the PG transpiler.

| Feature | Status | Test(s) |
|---------|--------|---------|
| `FROM`-first, `EXCLUDE`/`REPLACE`, `DESCRIBE`, `SUMMARIZE`, `QUALIFY`, lambdas, positional/ASOF joins, `COLUMNS()`, `USING SAMPLE` | ✅ | `fallback_test.go::TestFallback*` |

---

## Summary — the real gaps

Sorted by what's worth acting on first.

1. **⚠️ Cursors (DECLARE/FETCH/MOVE) — implemented but no happy-path test.** The only
   coverage is the missing-cursor *error* path
   (`conn_querylog_feedback_test.go::TestHandleFetchCursorLogsMissingCursorError`).
   This is live, client-reachable code (`server/conn_cursor.go`) with no differential
   assertion. **Highest-priority gap.**

2. **🟡 Stale `skipIfKnown` skips.** Differential RETURNING
   (`TestDML{Insert,Update,Delete}Returning`) and `COPY TO STDOUT`
   (`TestCopyToStdout`) are skipped in `tests/integration/` though the behavior is
   covered at unit/client level. Re-enable or document why they must stay skipped.

3. **🟡 Transpiler-only, no differential assertion:** generated columns, `SELECT FOR
   UPDATE/SHARE` (stripped). Add differential cases if these matter to clients.

4. **❌ Unsupported or untested but plausibly reachable — undefined behavior today:** MD5/SCRAM auth,
   enum/domain/composite/xml types, advisory locks, `LOCK TABLE`,
   GRANT/REVOKE/roles, `information_schema.{key_column_usage,table_constraints,
   referential_constraints}` (ORM FK discovery), `TABLESAMPLE`. Asserting these
   (even as "errors cleanly") would pin the compatibility boundary.

5. **⛔ Out of scope by design (correctly skipped):** SAVEPOINT, MERGE,
   sequences/SERIAL, materialized views, partitioning/inheritance,
   triggers/PL-pgSQL/rules/LISTEN-NOTIFY, RLS, and the network/geometric/range/
   text-search/money types. These are DuckDB or OLTP limitations.

---

---

## Appendix A — Catalog object, function & startup-parameter reference

This is the emulation-internals view that previously lived in the README: which
`pg_catalog`/`information_schema` objects and compatibility macros Duckgres
provides, and what each returns. "Implemented" = Duckgres-provided wrapper;
"Native (DuckDB)" = works through DuckDB's own `pg_catalog` with no Duckgres
wrapper; "Stub" = present but intentionally empty/constant; "Missing" = neither
wrapper nor native support. Behavior values (returns NULL / 0 / always true) are
deliberate stubs sized to satisfy client introspection, not real implementations.

### pg_catalog views

| View | Status | Notes |
|------|--------|-------|
| `pg_class` | Implemented | `pg_class_full` wrapper adding `relforcerowsecurity`; DuckLake variant sources from `duckdb_tables()`/`duckdb_views()` |
| `pg_namespace` | Implemented | Maps `main` → `public`; DuckLake variant derives from `duckdb_tables()`/`duckdb_views()` |
| `pg_attribute` | Implemented | Maps DuckDB internal type OIDs to PG OIDs via `duckdb_columns()` JOIN; fixes `atttypmod` for NUMERIC |
| `pg_type` | Implemented | Fixes NULLs + adds synthetic entries for missing OIDs (json, jsonb, bpchar, text, record, array types) |
| `pg_database` | Implemented | Hardcoded: postgres, template0, template1, testdb |
| `pg_stat_user_tables` | Implemented | Uses `reltuples` from pg_class; zeros for scan/tuple stats |
| `pg_roles` | Minimal view | Single hardcoded superuser row (not empty) |
| `pg_settings` | Native (DuckDB) | `pg_catalog.pg_settings` is queryable via DuckDB; the `current_setting()` macro only special-cases `server_version`/`server_encoding` |
| `pg_stat_activity` | Stub (empty) | Static view is empty; intercepted at query time for live data |
| `pg_constraint` | Stub (empty) | |
| `pg_enum` | Stub (empty) | |
| `pg_collation` | Stub (empty) | |
| `pg_policy` | Stub (empty) | |
| `pg_inherits` | Stub (empty) | |
| `pg_statistic_ext` | Stub (empty) | |
| `pg_publication` | Stub (empty) | |
| `pg_publication_rel` | Stub (empty) | |
| `pg_publication_tables` | Stub (empty) | |
| `pg_rules` | Stub (empty) | |
| `pg_matviews` | Stub (empty) | |
| `pg_partitioned_table` | Stub (empty) | |
| `pg_statio_user_tables` | Stub (empty) | |
| `pg_stat_statements` | Stub (empty) | |
| `pg_indexes` | Stub (empty) | |
| `pg_proc` | Native (DuckDB) | DuckDB has native `pg_catalog.pg_proc`; no Duckgres wrapper |
| `pg_description` | Missing | Handled via `obj_description()`/`col_description()` macros returning NULL |
| `pg_depend` | Missing | |
| `pg_am` | Missing | |
| `pg_attrdef` | Missing | |
| `pg_tablespace` | Missing | |

### information_schema views

| View | Status | Notes |
|------|--------|-------|
| `tables` | Implemented | Filters internal views, normalizes `main` → `public` |
| `columns` | Implemented | DuckDB → PG type name normalization, optional metadata overlay |
| `schemata` | Implemented | Adds synthetic entries for `pg_catalog`, `information_schema`, `pg_toast` |
| `views` | Implemented | Filters internal views |
| `key_column_usage` | Missing | Used by ORMs for relationship discovery |
| `table_constraints` | Missing | Used by ORMs for relationship discovery |
| `referential_constraints` | Missing | Used by ORMs for FK introspection |

### Functions & macros

| Function | Status | Notes |
|----------|--------|-------|
| `format_type(oid, int)` | Implemented | Comprehensive OID → name mapping |
| `pg_get_expr(text, oid)` | Implemented | Returns NULL |
| `pg_get_indexdef(oid)` | Implemented | Returns empty string |
| `pg_get_constraintdef(oid)` | Implemented | Returns empty string |
| `pg_get_serial_sequence(text, text)` | Implemented | Returns NULL (no sequence support) |
| `pg_table_is_visible(oid)` | Implemented | Always true |
| `pg_get_userbyid(oid)` | Implemented | Maps OID 10 → `postgres`, 6171 → `pg_database_owner` |
| `obj_description(oid, text)` | Implemented | Returns NULL |
| `col_description(oid, int)` | Implemented | Returns NULL |
| `shobj_description(oid, text)` | Implemented | Returns NULL |
| `has_table_privilege(text, text)` | Implemented | Always true |
| `has_schema_privilege(text, text)` | Implemented | Always true |
| `pg_encoding_to_char(int)` | Implemented | Always `UTF8` |
| `version()` | Implemented | Returns `PostgreSQL 15.0 … (Duckgres/DuckDB)` |
| `current_setting(text)` | Implemented | Special-cases `server_version`, `server_encoding` |
| `current_schema()` | Native (DuckDB) | Works via DuckDB; no Duckgres wrapper |
| `current_schemas(bool)` | Missing | |
| `pg_is_in_recovery()` | Implemented | Always false |
| `pg_backend_pid()` | Implemented | Returns 0 |
| `pg_size_pretty(bigint)` | Implemented | Full human-readable formatting |
| `pg_total_relation_size(oid)` | Implemented | Returns 0 |
| `pg_relation_size(oid)` | Implemented | Returns 0 |
| `pg_table_size(oid)` | Implemented | Returns 0 |
| `pg_indexes_size(oid)` | Implemented | Returns 0 |
| `pg_database_size(text)` | Implemented | Returns 0 |
| `quote_ident(text)` | Implemented | |
| `quote_literal(text)` | Implemented | |
| `quote_nullable(text)` | Implemented | |
| `txid_current()` | Implemented | Epoch-based pseudo ID |

### Startup parameters

| Parameter | Value |
|-----------|-------|
| `server_version` | `15.0 (Duckgres)` |
| `server_encoding` | `UTF8` |
| `client_encoding` | `UTF8` |
| `DateStyle` | `ISO, MDY` |
| `TimeZone` | `UTC` |
| `integer_datetimes` | `on` |
| `standard_conforming_strings` | `on` |
| `IntervalStyle` | Missing |

> Duckgres advertises **PostgreSQL 15.0** on the wire (`server/catalog.go`,
> `server/conn.go`). The differential test suite compares results against a real
> PostgreSQL 16 server, but the emulated version string is intentionally 15.0.

---

## Related docs

- [README.md](../README.md) → "SQL Client Compatibility" — short user-facing summary that links here.
- [tests/integration/README.md](../tests/integration/README.md) — test-suite architecture, category counts, and the skip-reason table.
- [TODO.md](../TODO.md) — lightweight backlog for project ideas that do not yet have a better home.
- [scripts/client-compat/README.md](../scripts/client-compat/README.md) — real-driver compatibility harness and `queries.yaml`.

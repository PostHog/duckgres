# Duckgres + DuckLake QA Compatibility Summary

This document summarizes PostgreSQL SQL compatibility when running Duckgres with DuckLake mode enabled.

**Test Date:** 2026-02-03
**Test Environment:** Duckgres with DuckLake extension, S3/MinIO object store, PostgreSQL metadata store

---

## Working Well

### Core SQL

| Feature | Status |
|---------|--------|
| SELECT, INSERT, UPDATE, DELETE | ✅ |
| JOINs (INNER, LEFT, RIGHT, CROSS, self-join) | ✅ |
| CTEs (including RECURSIVE) | ✅ |
| Subqueries (scalar, IN, EXISTS, NOT EXISTS, correlated, LATERAL) | ✅ |
| UNION, UNION ALL, INTERSECT, EXCEPT | ✅ |
| CASE expressions (simple, searched, nested) | ✅ |
| GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET, FETCH FIRST | ✅ |
| DISTINCT, DISTINCT ON | ✅ |
| Window functions (row_number, rank, dense_rank, ntile, lead, lag, first_value, last_value) | ✅ |
| GROUPING SETS, ROLLUP, CUBE | ✅ |
| Named windows (WINDOW clause) | ✅ |
| FILTER clause on aggregates | ✅ |
| `percentile_cont` WITHIN GROUP | ✅ |

### Aggregate Functions

All standard aggregates work: `count`, `sum`, `avg`, `min`, `max`, `bool_and`, `bool_or`, `string_agg`, `array_agg`

### Math Functions

All basic math functions work: `abs`, `ceil`, `floor`, `round`, `trunc`, `mod`, `power`, `sqrt`, `cbrt`, `exp`, `ln`, `log`, `log10`, `sign`, `greatest`, `least`, `pi`, `random`, `degrees`, `radians`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `div`

### String Functions

| Function | Status |
|----------|--------|
| `length`, `upper`, `lower` | ✅ |
| `concat`, `concat_ws` | ✅ |
| `substring`, `substr` | ✅ |
| `position`, `strpos` | ✅ |
| `replace`, `reverse`, `repeat` | ✅ |
| `lpad`, `rpad` (3-arg only) | ✅ |
| `ltrim`, `rtrim`, `trim` | ✅ |
| `left`, `right` | ✅ |
| `chr`, `ascii` | ✅ |
| `split_part` | ✅ |
| `translate` | ✅ |
| `md5` | ✅ |
| `starts_with`, `ends_with` | ✅ |
| `char_length`, `character_length` | ✅ |
| `bit_length` | ✅ |

### Regex Operators

| Operator | Status |
|----------|--------|
| `~` (case-sensitive match) | ✅ |
| `~*` (case-insensitive match) | ✅ |
| `!~` (negated case-sensitive) | ✅ |
| `!~*` (negated case-insensitive) | ✅ |
| `regexp_matches`, `regexp_replace`, `regexp_match` | ✅ |
| `regexp_split_to_array`, `regexp_split_to_table` | ✅ |

### Date/Time Functions

| Function | Status |
|----------|--------|
| `extract(field FROM source)` | ✅ |
| `date_part` | ✅ |
| `date_trunc` | ✅ |
| Date arithmetic (`+`, `-` with intervals) | ✅ |
| `current_date`, `current_timestamp`, `now()` | ✅ |
| `make_date`, `make_timestamp` | ✅ |
| `age()` | ✅ (different output format) |
| `AT TIME ZONE` | ✅ |
| `timezone()` | ✅ |

### Array Functions

| Feature | Status |
|---------|--------|
| Array literals `ARRAY[1,2,3]` | ✅ |
| Array indexing `arr[i]` | ✅ |
| Array slicing `arr[2:4]` | ✅ |
| `array_cat`, `array_append`, `array_prepend` | ✅ |
| `array_position` | ✅ |
| `unnest` | ✅ |
| `array_to_string`, `string_to_array` | ✅ |
| `@>`, `<@`, `&&` operators | ✅ |
| `ANY(array)`, `ALL(array)` | ✅ |

### JSON Functions

| Feature | Status |
|---------|--------|
| JSON literals | ✅ |
| `->` (get object field) | ✅ |
| `->>` (get field as text) | ✅ |
| `json_build_object`, `json_build_array` | ✅ |
| `json_array_length` | ✅ |
| `json_typeof` | ✅ |
| `json_agg`, `json_object_agg` | ✅ |
| `json_each` | ✅ |
| `to_json` | ✅ |

### DDL

| Feature | Status |
|---------|--------|
| CREATE/DROP TABLE | ✅ |
| ALTER TABLE (add/drop/rename column) | ✅ |
| ALTER TABLE RENAME | ✅ |
| CREATE/DROP VIEW | ✅ |
| CREATE TABLE AS SELECT | ✅ |
| TRUNCATE | ✅ |

### Transactions

| Feature | Status |
|---------|--------|
| BEGIN, COMMIT, ROLLBACK | ✅ |

### Other Working Features

- COPY TO/FROM STDOUT
- EXPLAIN, EXPLAIN ANALYZE
- VACUUM, ANALYZE
- Prepared statements (PREPARE/EXECUTE/DEALLOCATE)
- COMMENT ON
- IS DISTINCT FROM / IS NOT DISTINCT FROM
- Dollar-quoted strings (`$$...$$`, `$tag$...$tag$`)
- Escape strings (`E'...\n...'`)
- Unicode escape strings (`U&'...'`)
- ISNULL / NOTNULL
- UPDATE ... FROM
- DELETE ... USING
- Bitwise operators (`&`, `|`, `~`, `<<`, `>>`)
- information_schema queries
- pg_catalog compatibility views

---

## Not Working / Known Issues

### Critical DuckLake Limitations

| Feature | Error/Issue |
|---------|-------------|
| **RETURNING clause** | `RETURNING clause not yet supported for insertion/update/deletion of a DuckLake table` |
| **ON CONFLICT (UPSERT)** | `The specified columns as conflict target are not referenced by a UNIQUE/PRIMARY KEY CONSTRAINT` |
| **Sequences** | `DuckLake does not support sequences` |
| **SELECT INTO** | `SELECT INTO not supported!` |
| **SAVEPOINT** | `syntax error at or near "SAVEPOINT"` |

### Missing Functions

| Function | Status |
|----------|--------|
| `initcap()` | Not exists |
| `to_char(date, format)` | Returns format string literally (no substitution) |
| `format(fmt, args...)` | No substitution performed |
| `overlay()` | Not exists |
| `quote_literal()` | Not exists |
| `quote_ident()` | Not exists |
| `quote_nullable()` | Not exists |
| `encode(data, format)` | Different signature in DuckDB |
| `decode(data, format)` | Different signature in DuckDB |
| `octet_length()` | Requires explicit type cast |
| `get_byte()` | Not exists |
| `set_byte()` | Not exists |
| `lpad(str, len)` | Requires 3 arguments (fill char required) |
| `rpad(str, len)` | Requires 3 arguments (fill char required) |
| `pg_table_size()` | Not exists |
| `pg_relation_size()` | Not exists |
| `pg_total_relation_size()` | Not exists |
| `array_length(arr, dim)` | DuckDB's `len()` only takes 1 argument |
| `array_dims()` | Not exists |
| `array_ndims()` | Not exists |
| `cardinality()` for arrays | Only works on MAPs |

### Regex Issues

| Feature | Issue |
|---------|-------|
| `SIMILAR TO` | Returns false unexpectedly |
| `LIKE ... ESCAPE` | pg_catalog reference error |

### JSON Issues

| Feature | Issue |
|---------|-------|
| `#>` (path operator) | Syntax error |
| `#>>` (path operator as text) | Syntax error |
| `json_extract_path(json, VARIADIC keys)` | Wrong signature - doesn't accept multiple path args |
| `json_extract_path_text()` | Wrong signature |
| `json_array_elements()` | Not exists |
| `json_each_text()` | Not exists |
| JSON output format | Shows `map[key:value]` instead of proper JSON |

### PostgreSQL-Specific Operators Not Supported

| Operator | Description | Issue |
|----------|-------------|-------|
| `/` | Integer division | Returns float instead of integer |
| `\|/` | Square root | Not exists (use `sqrt()`) |
| `\|\|/` | Cube root | Not exists (use `cbrt()`) |
| `!` | Factorial | Not exists |
| `#` | Bitwise XOR | Not exists (use `xor()`) |

### Other Missing Features

| Feature | Status |
|---------|--------|
| Full-text search (tsvector, tsquery, `@@`) | Not supported |
| Range types (int4range, daterange, etc.) | Not supported |
| DOMAIN | Not supported |
| OVERLAPS | Not supported |
| GROUP BY with ordinal position | Doesn't resolve correctly |
| Row field access `(ROW(...)).field` | Fails on unnamed structs |

### Output Format Differences

These features work but produce different output format than PostgreSQL:

| Feature | PostgreSQL Format | DuckDB/Duckgres Format |
|---------|-------------------|------------------------|
| Intervals | `1 day 02:00:00` | `{1 0 7200000000}` |
| Arrays | `{1,2,3}` | `[1 2 3]` |
| `age()` | `5 mons 14 days` | `{14 5 0}` |
| Table functions | Rows | `map[function_name:value]` |
| `generate_series` (integers) | Returns rows | Returns array |
| JSON objects | `{"key": "value"}` | `map[key:value]` |

---

## Recommendations

### For Application Developers

1. **Avoid RETURNING** - Use separate SELECT after INSERT/UPDATE/DELETE
2. **Avoid UPSERT** - Use explicit SELECT + INSERT/UPDATE logic
3. **Avoid Sequences** - Use application-generated IDs (UUID, etc.)
4. **Use function alternatives:**
   - Instead of `initcap()`: Use `upper(left(str,1)) || lower(substring(str from 2))`
   - Instead of `lpad(str, n)`: Use `lpad(str, n, ' ')`
   - Instead of `to_char(date, fmt)`: Use `strftime(date, fmt)` (DuckDB format)
   - Instead of `|/`: Use `sqrt()`
   - Instead of `||/`: Use `cbrt()`

### For Duckgres Development

Priority items to consider implementing:

1. **High Priority:**
   - `to_char()` date formatting
   - `~` regex operator fix
   - `initcap()`
   - 2-argument `lpad`/`rpad`

2. **Medium Priority:**
   - `json_array_elements()`
   - `json_each_text()`
   - `#>` / `#>>` JSON path operators
   - `format()` string formatting
   - `overlay()`

3. **DuckLake-Specific:**
   - RETURNING clause support
   - Sequence support (or explicit error guidance)

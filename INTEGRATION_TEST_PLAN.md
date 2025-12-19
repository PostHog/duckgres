# Duckgres PostgreSQL Compatibility Integration Test Plan

## Overview

This document outlines a comprehensive integration test suite to verify Duckgres compatibility with PostgreSQL 16 for OLAP workloads. The goal is semantic equivalence: same data and structure, allowing minor formatting differences.

## Test Infrastructure

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Test Runner (Go)                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────┐              ┌─────────────────────┐           │
│  │   PostgreSQL 16     │              │      Duckgres       │           │
│  │   (Docker)          │              │   (In-Process)      │           │
│  │   Port: 35432       │              │   Port: 35433       │           │
│  └─────────────────────┘              └─────────────────────┘           │
│           │                                    │                         │
│           └────────────┬───────────────────────┘                         │
│                        │                                                 │
│                        ▼                                                 │
│             ┌──────────────────────┐                                     │
│             │  Result Comparator   │                                     │
│             │  - Column names      │                                     │
│             │  - Row data          │                                     │
│             │  - Error codes       │                                     │
│             └──────────────────────┘                                     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Side-by-Side Testing Strategy

1. **Docker PostgreSQL 16**: Ground truth reference
2. **Duckgres Server**: System under test
3. **Result Comparator**: Semantic comparison of results

### Comparison Rules

- **Column Names**: Case-insensitive comparison, same count
- **Row Data**: Same values after type normalization
- **Ordering**: Respect ORDER BY; otherwise treat as sets
- **NULL Handling**: NULL == NULL for comparison purposes
- **Numeric Precision**: Float tolerance of 1e-9
- **Timestamps**: Tolerance of 1 microsecond
- **Error Codes**: Same SQLSTATE when errors expected

---

## Test Categories

### 1. Data Query Language (DQL)

#### 1.1 Basic SELECT
```sql
-- Literal values
SELECT 1;
SELECT 1, 2, 3;
SELECT 'hello';
SELECT NULL;
SELECT TRUE, FALSE;

-- Expressions
SELECT 1 + 1;
SELECT 10 / 3;
SELECT 10.0 / 3;
SELECT 'hello' || ' ' || 'world';

-- Aliases
SELECT 1 AS num;
SELECT 1 num;
SELECT 1 AS "Column Name";

-- FROM clause
SELECT * FROM table_name;
SELECT col1, col2 FROM table_name;
SELECT t.* FROM table_name t;
SELECT t.col1 FROM table_name AS t;
```

#### 1.2 WHERE Clause
```sql
-- Comparison operators
SELECT * FROM t WHERE col = 1;
SELECT * FROM t WHERE col != 1;
SELECT * FROM t WHERE col <> 1;
SELECT * FROM t WHERE col < 1;
SELECT * FROM t WHERE col <= 1;
SELECT * FROM t WHERE col > 1;
SELECT * FROM t WHERE col >= 1;

-- NULL handling
SELECT * FROM t WHERE col IS NULL;
SELECT * FROM t WHERE col IS NOT NULL;
SELECT * FROM t WHERE col IS DISTINCT FROM 1;
SELECT * FROM t WHERE col IS NOT DISTINCT FROM 1;

-- Boolean operators
SELECT * FROM t WHERE a = 1 AND b = 2;
SELECT * FROM t WHERE a = 1 OR b = 2;
SELECT * FROM t WHERE NOT (a = 1);
SELECT * FROM t WHERE a = 1 AND (b = 2 OR c = 3);

-- IN operator
SELECT * FROM t WHERE col IN (1, 2, 3);
SELECT * FROM t WHERE col NOT IN (1, 2, 3);
SELECT * FROM t WHERE col IN (SELECT id FROM other);

-- BETWEEN
SELECT * FROM t WHERE col BETWEEN 1 AND 10;
SELECT * FROM t WHERE col NOT BETWEEN 1 AND 10;

-- LIKE and pattern matching
SELECT * FROM t WHERE col LIKE 'foo%';
SELECT * FROM t WHERE col LIKE '%bar';
SELECT * FROM t WHERE col LIKE '%baz%';
SELECT * FROM t WHERE col LIKE 'a_c';
SELECT * FROM t WHERE col ILIKE 'FOO%';
SELECT * FROM t WHERE col NOT LIKE 'foo%';
SELECT * FROM t WHERE col SIMILAR TO '%(foo|bar)%';

-- Regex (PostgreSQL specific)
SELECT * FROM t WHERE col ~ '^foo';
SELECT * FROM t WHERE col ~* '^FOO';
SELECT * FROM t WHERE col !~ '^foo';
SELECT * FROM t WHERE col !~* '^FOO';

-- ANY/ALL
SELECT * FROM t WHERE col = ANY(ARRAY[1, 2, 3]);
SELECT * FROM t WHERE col = ALL(ARRAY[1]);
SELECT * FROM t WHERE col > ANY(SELECT val FROM other);
```

#### 1.3 ORDER BY
```sql
SELECT * FROM t ORDER BY col;
SELECT * FROM t ORDER BY col ASC;
SELECT * FROM t ORDER BY col DESC;
SELECT * FROM t ORDER BY col NULLS FIRST;
SELECT * FROM t ORDER BY col NULLS LAST;
SELECT * FROM t ORDER BY col1, col2;
SELECT * FROM t ORDER BY col1 ASC, col2 DESC;
SELECT * FROM t ORDER BY 1;
SELECT * FROM t ORDER BY 1, 2;
SELECT a + b AS sum FROM t ORDER BY sum;
SELECT * FROM t ORDER BY col COLLATE "C";
```

#### 1.4 LIMIT and OFFSET
```sql
SELECT * FROM t LIMIT 10;
SELECT * FROM t LIMIT 10 OFFSET 5;
SELECT * FROM t OFFSET 5;
SELECT * FROM t LIMIT ALL;
SELECT * FROM t FETCH FIRST 10 ROWS ONLY;
SELECT * FROM t FETCH FIRST 10 ROWS WITH TIES;
SELECT * FROM t OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY;
```

#### 1.5 DISTINCT
```sql
SELECT DISTINCT col FROM t;
SELECT DISTINCT col1, col2 FROM t;
SELECT DISTINCT ON (col1) col1, col2, col3 FROM t ORDER BY col1, col2;
SELECT ALL col FROM t;
```

#### 1.6 GROUP BY and HAVING
```sql
SELECT col, COUNT(*) FROM t GROUP BY col;
SELECT col, SUM(val) FROM t GROUP BY col;
SELECT col, AVG(val) FROM t GROUP BY col;
SELECT col, MIN(val), MAX(val) FROM t GROUP BY col;
SELECT col1, col2, COUNT(*) FROM t GROUP BY col1, col2;
SELECT col, COUNT(*) FROM t GROUP BY col HAVING COUNT(*) > 5;
SELECT col, SUM(val) FROM t GROUP BY col HAVING SUM(val) > 100;

-- GROUP BY with expressions
SELECT EXTRACT(year FROM date_col), COUNT(*) FROM t GROUP BY EXTRACT(year FROM date_col);
SELECT col / 10 * 10 AS bucket, COUNT(*) FROM t GROUP BY col / 10 * 10;

-- GROUPING SETS, CUBE, ROLLUP
SELECT col1, col2, SUM(val) FROM t GROUP BY GROUPING SETS ((col1), (col2), ());
SELECT col1, col2, SUM(val) FROM t GROUP BY ROLLUP (col1, col2);
SELECT col1, col2, SUM(val) FROM t GROUP BY CUBE (col1, col2);
SELECT col1, col2, SUM(val), GROUPING(col1, col2) FROM t GROUP BY ROLLUP (col1, col2);
```

#### 1.7 JOINs
```sql
-- INNER JOIN
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;
SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id;
SELECT * FROM t1 JOIN t2 USING (id);
SELECT * FROM t1 NATURAL JOIN t2;

-- LEFT JOIN
SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id;
SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;

-- RIGHT JOIN
SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id;
SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id;

-- FULL OUTER JOIN
SELECT * FROM t1 FULL JOIN t2 ON t1.id = t2.id;
SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;

-- CROSS JOIN
SELECT * FROM t1 CROSS JOIN t2;
SELECT * FROM t1, t2;

-- Multiple JOINs
SELECT * FROM t1
  JOIN t2 ON t1.id = t2.t1_id
  JOIN t3 ON t2.id = t3.t2_id;

-- Self JOIN
SELECT a.*, b.* FROM t a JOIN t b ON a.parent_id = b.id;

-- LATERAL JOIN
SELECT * FROM t1
  CROSS JOIN LATERAL (SELECT * FROM t2 WHERE t2.id = t1.id LIMIT 1) sub;
SELECT * FROM t1,
  LATERAL (SELECT * FROM t2 WHERE t2.id = t1.id) sub;
```

#### 1.8 Subqueries
```sql
-- Scalar subquery
SELECT *, (SELECT MAX(val) FROM t2) AS max_val FROM t1;
SELECT * FROM t WHERE val = (SELECT MAX(val) FROM t);

-- Table subquery
SELECT * FROM (SELECT * FROM t WHERE active = true) sub;
SELECT * FROM (SELECT col1, SUM(val) AS total FROM t GROUP BY col1) sub WHERE total > 100;

-- Correlated subquery
SELECT * FROM t1 WHERE val > (SELECT AVG(val) FROM t2 WHERE t2.category = t1.category);
SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id);
SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id);
SELECT * FROM t1 WHERE id IN (SELECT t1_id FROM t2);
SELECT * FROM t1 WHERE id NOT IN (SELECT t1_id FROM t2 WHERE t1_id IS NOT NULL);
```

#### 1.9 Common Table Expressions (CTEs)
```sql
-- Simple CTE
WITH cte AS (SELECT * FROM t WHERE active = true)
SELECT * FROM cte;

-- Multiple CTEs
WITH
  cte1 AS (SELECT * FROM t1),
  cte2 AS (SELECT * FROM t2)
SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id;

-- CTE referencing another CTE
WITH
  raw AS (SELECT * FROM t),
  filtered AS (SELECT * FROM raw WHERE active = true)
SELECT * FROM filtered;

-- Recursive CTE
WITH RECURSIVE tree AS (
  SELECT id, parent_id, name, 1 AS level
  FROM categories WHERE parent_id IS NULL
  UNION ALL
  SELECT c.id, c.parent_id, c.name, t.level + 1
  FROM categories c JOIN tree t ON c.parent_id = t.id
)
SELECT * FROM tree;

-- CTE with column aliases
WITH cte(a, b, c) AS (SELECT 1, 2, 3)
SELECT * FROM cte;
```

#### 1.10 Set Operations
```sql
-- UNION
SELECT col FROM t1 UNION SELECT col FROM t2;
SELECT col FROM t1 UNION ALL SELECT col FROM t2;
SELECT col FROM t1 UNION DISTINCT SELECT col FROM t2;

-- INTERSECT
SELECT col FROM t1 INTERSECT SELECT col FROM t2;
SELECT col FROM t1 INTERSECT ALL SELECT col FROM t2;

-- EXCEPT
SELECT col FROM t1 EXCEPT SELECT col FROM t2;
SELECT col FROM t1 EXCEPT ALL SELECT col FROM t2;

-- Multiple set operations
SELECT col FROM t1 UNION SELECT col FROM t2 UNION SELECT col FROM t3;
(SELECT col FROM t1 UNION SELECT col FROM t2) INTERSECT SELECT col FROM t3;
```

#### 1.11 Window Functions
```sql
-- ROW_NUMBER, RANK, DENSE_RANK
SELECT *, ROW_NUMBER() OVER (ORDER BY col) FROM t;
SELECT *, RANK() OVER (ORDER BY col) FROM t;
SELECT *, DENSE_RANK() OVER (ORDER BY col) FROM t;
SELECT *, NTILE(4) OVER (ORDER BY col) FROM t;

-- Partitioned windows
SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY val DESC) FROM t;
SELECT *, RANK() OVER (PARTITION BY category ORDER BY val DESC) FROM t;

-- Aggregate window functions
SELECT *, SUM(val) OVER (ORDER BY date_col) AS running_sum FROM t;
SELECT *, AVG(val) OVER (ORDER BY date_col) AS running_avg FROM t;
SELECT *, COUNT(*) OVER (PARTITION BY category) AS category_count FROM t;

-- Window frame specification
SELECT *, SUM(val) OVER (ORDER BY date_col ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t;
SELECT *, AVG(val) OVER (ORDER BY date_col ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t;
SELECT *, SUM(val) OVER (ORDER BY date_col RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) FROM t;
SELECT *, SUM(val) OVER (ORDER BY date_col ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;

-- LAG, LEAD
SELECT *, LAG(val) OVER (ORDER BY date_col) AS prev_val FROM t;
SELECT *, LAG(val, 2) OVER (ORDER BY date_col) AS prev_2_val FROM t;
SELECT *, LAG(val, 1, 0) OVER (ORDER BY date_col) AS prev_val_default FROM t;
SELECT *, LEAD(val) OVER (ORDER BY date_col) AS next_val FROM t;

-- FIRST_VALUE, LAST_VALUE, NTH_VALUE
SELECT *, FIRST_VALUE(val) OVER (PARTITION BY category ORDER BY date_col) FROM t;
SELECT *, LAST_VALUE(val) OVER (PARTITION BY category ORDER BY date_col
  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;
SELECT *, NTH_VALUE(val, 2) OVER (ORDER BY date_col) FROM t;

-- Named windows
SELECT *,
  SUM(val) OVER w AS sum,
  AVG(val) OVER w AS avg,
  COUNT(*) OVER w AS cnt
FROM t
WINDOW w AS (PARTITION BY category ORDER BY date_col);

-- PERCENT_RANK, CUME_DIST
SELECT *, PERCENT_RANK() OVER (ORDER BY val) FROM t;
SELECT *, CUME_DIST() OVER (ORDER BY val) FROM t;
```

#### 1.12 VALUES and TABLE
```sql
VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name);
TABLE t; -- Equivalent to SELECT * FROM t
```

---

### 2. Data Definition Language (DDL)

#### 2.1 CREATE TABLE
```sql
-- Basic table
CREATE TABLE t (id INTEGER, name TEXT);

-- With constraints (note: may be stripped in DuckLake mode)
CREATE TABLE t (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE
);

-- With column defaults
CREATE TABLE t (
  id INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  status TEXT DEFAULT 'pending'
);

-- Various data types
CREATE TABLE types_test (
  bool_col BOOLEAN,
  int2_col SMALLINT,
  int4_col INTEGER,
  int8_col BIGINT,
  float4_col REAL,
  float8_col DOUBLE PRECISION,
  numeric_col NUMERIC(10, 2),
  text_col TEXT,
  varchar_col VARCHAR(255),
  char_col CHAR(10),
  bytea_col BYTEA,
  date_col DATE,
  time_col TIME,
  timestamp_col TIMESTAMP,
  timestamptz_col TIMESTAMPTZ,
  interval_col INTERVAL,
  uuid_col UUID,
  json_col JSON,
  jsonb_col JSONB
);

-- CREATE TABLE AS
CREATE TABLE t2 AS SELECT * FROM t1 WHERE active = true;
CREATE TABLE t2 AS SELECT col1, SUM(val) FROM t1 GROUP BY col1;

-- CREATE TABLE LIKE
CREATE TABLE t2 (LIKE t1);
CREATE TABLE t2 (LIKE t1 INCLUDING ALL);

-- IF NOT EXISTS
CREATE TABLE IF NOT EXISTS t (id INTEGER);

-- Temporary tables
CREATE TEMP TABLE t (id INTEGER);
CREATE TEMPORARY TABLE t (id INTEGER);

-- Unlogged tables (may be treated as regular)
CREATE UNLOGGED TABLE t (id INTEGER);
```

#### 2.2 ALTER TABLE
```sql
-- Add column
ALTER TABLE t ADD COLUMN new_col TEXT;
ALTER TABLE t ADD COLUMN new_col INTEGER DEFAULT 0;

-- Drop column
ALTER TABLE t DROP COLUMN old_col;
ALTER TABLE t DROP COLUMN IF EXISTS old_col;

-- Rename column
ALTER TABLE t RENAME COLUMN old_name TO new_name;

-- Rename table
ALTER TABLE t RENAME TO new_name;

-- Set/drop default
ALTER TABLE t ALTER COLUMN col SET DEFAULT 'value';
ALTER TABLE t ALTER COLUMN col DROP DEFAULT;

-- Set/drop not null
ALTER TABLE t ALTER COLUMN col SET NOT NULL;
ALTER TABLE t ALTER COLUMN col DROP NOT NULL;

-- Change column type
ALTER TABLE t ALTER COLUMN col TYPE VARCHAR(100);
ALTER TABLE t ALTER COLUMN col TYPE INTEGER USING col::INTEGER;

-- Add constraint (may be no-op in DuckLake mode)
ALTER TABLE t ADD CONSTRAINT unique_name UNIQUE (col);
ALTER TABLE t ADD CONSTRAINT check_positive CHECK (col > 0);
```

#### 2.3 DROP TABLE
```sql
DROP TABLE t;
DROP TABLE IF EXISTS t;
DROP TABLE t CASCADE;
DROP TABLE t RESTRICT;
DROP TABLE t1, t2, t3;
```

#### 2.4 CREATE/DROP VIEW
```sql
CREATE VIEW v AS SELECT * FROM t WHERE active = true;
CREATE OR REPLACE VIEW v AS SELECT * FROM t;
CREATE VIEW v (a, b) AS SELECT col1, col2 FROM t;
CREATE TEMP VIEW v AS SELECT * FROM t;
DROP VIEW v;
DROP VIEW IF EXISTS v;
DROP VIEW v CASCADE;
```

#### 2.5 CREATE/DROP INDEX
```sql
-- Note: May be no-op in DuckLake mode
CREATE INDEX idx ON t (col);
CREATE INDEX idx ON t (col1, col2);
CREATE INDEX idx ON t (col DESC);
CREATE INDEX idx ON t (col NULLS FIRST);
CREATE UNIQUE INDEX idx ON t (col);
CREATE INDEX IF NOT EXISTS idx ON t (col);
CREATE INDEX idx ON t (col) WHERE active = true;
CREATE INDEX idx ON t USING btree (col);
CREATE INDEX idx ON t USING hash (col);
DROP INDEX idx;
DROP INDEX IF EXISTS idx;
```

#### 2.6 CREATE/DROP SCHEMA
```sql
CREATE SCHEMA s;
CREATE SCHEMA IF NOT EXISTS s;
CREATE SCHEMA s AUTHORIZATION user;
DROP SCHEMA s;
DROP SCHEMA IF EXISTS s;
DROP SCHEMA s CASCADE;
```

#### 2.7 TRUNCATE
```sql
TRUNCATE TABLE t;
TRUNCATE t;
TRUNCATE t1, t2, t3;
TRUNCATE t CASCADE;
TRUNCATE t RESTART IDENTITY;
```

#### 2.8 COMMENT
```sql
COMMENT ON TABLE t IS 'Table description';
COMMENT ON COLUMN t.col IS 'Column description';
COMMENT ON VIEW v IS 'View description';
```

---

### 3. Data Manipulation Language (DML)

#### 3.1 INSERT
```sql
-- Single row
INSERT INTO t (col1, col2) VALUES (1, 'a');
INSERT INTO t VALUES (1, 'a');

-- Multiple rows
INSERT INTO t (col1, col2) VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- INSERT ... SELECT
INSERT INTO t2 SELECT * FROM t1;
INSERT INTO t2 (col1, col2) SELECT a, b FROM t1;

-- WITH clause
WITH src AS (SELECT * FROM t1 WHERE active = true)
INSERT INTO t2 SELECT * FROM src;

-- DEFAULT VALUES
INSERT INTO t DEFAULT VALUES;
INSERT INTO t (col1) VALUES (DEFAULT);

-- RETURNING
INSERT INTO t (col1, col2) VALUES (1, 'a') RETURNING *;
INSERT INTO t (col1, col2) VALUES (1, 'a') RETURNING id;
INSERT INTO t (col1, col2) VALUES (1, 'a') RETURNING id, col1;

-- ON CONFLICT (UPSERT)
INSERT INTO t (id, val) VALUES (1, 'a') ON CONFLICT DO NOTHING;
INSERT INTO t (id, val) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING;
INSERT INTO t (id, val) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;
INSERT INTO t (id, val) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE t.val != EXCLUDED.val;
INSERT INTO t (id, val) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT t_pkey DO UPDATE SET val = EXCLUDED.val;
```

#### 3.2 UPDATE
```sql
-- Basic update
UPDATE t SET col = 'new_value';
UPDATE t SET col = 'new_value' WHERE id = 1;
UPDATE t SET col1 = 'a', col2 = 'b' WHERE id = 1;

-- Update with expressions
UPDATE t SET counter = counter + 1;
UPDATE t SET val = val * 1.1 WHERE category = 'premium';

-- Update with subquery
UPDATE t SET val = (SELECT MAX(val) FROM other) WHERE id = 1;
UPDATE t1 SET val = t2.val FROM t2 WHERE t1.id = t2.id;

-- Update with JOIN (FROM clause)
UPDATE t1 SET val = t2.val FROM t2 WHERE t1.id = t2.id AND t2.active = true;

-- RETURNING
UPDATE t SET val = 'new' WHERE id = 1 RETURNING *;
UPDATE t SET val = 'new' WHERE id = 1 RETURNING id, val;
```

#### 3.3 DELETE
```sql
-- Basic delete
DELETE FROM t WHERE id = 1;
DELETE FROM t WHERE active = false;
DELETE FROM t; -- Delete all rows

-- Delete with subquery
DELETE FROM t WHERE id IN (SELECT id FROM other WHERE expired = true);
DELETE FROM t1 USING t2 WHERE t1.id = t2.id AND t2.obsolete = true;

-- RETURNING
DELETE FROM t WHERE id = 1 RETURNING *;
DELETE FROM t WHERE id = 1 RETURNING id, name;
```

#### 3.4 COPY
```sql
-- COPY TO
COPY t TO STDOUT;
COPY t TO STDOUT WITH (FORMAT CSV);
COPY t TO STDOUT WITH (FORMAT CSV, HEADER);
COPY (SELECT * FROM t WHERE active = true) TO STDOUT;

-- COPY FROM
COPY t FROM STDIN;
COPY t FROM STDIN WITH (FORMAT CSV);
COPY t FROM STDIN WITH (FORMAT CSV, HEADER);
COPY t (col1, col2) FROM STDIN;
```

---

### 4. Data Types

#### 4.1 Numeric Types
```sql
-- Integer types
SELECT 1::SMALLINT, 1::INTEGER, 1::BIGINT;
SELECT 1::INT2, 1::INT4, 1::INT8;

-- Floating point
SELECT 1.5::REAL, 1.5::DOUBLE PRECISION;
SELECT 1.5::FLOAT4, 1.5::FLOAT8;

-- Numeric/Decimal
SELECT 1.23456::NUMERIC;
SELECT 1.23456::NUMERIC(10, 2);
SELECT 1.23456::DECIMAL(10, 2);

-- Special values
SELECT 'NaN'::FLOAT, 'Infinity'::FLOAT, '-Infinity'::FLOAT;
```

#### 4.2 Character Types
```sql
SELECT 'hello'::TEXT;
SELECT 'hello'::VARCHAR;
SELECT 'hello'::VARCHAR(10);
SELECT 'hello'::CHAR(10);
SELECT 'hello'::CHARACTER VARYING(10);
```

#### 4.3 Date/Time Types
```sql
-- Date
SELECT DATE '2024-01-15';
SELECT '2024-01-15'::DATE;
SELECT CURRENT_DATE;

-- Time
SELECT TIME '12:30:45';
SELECT '12:30:45'::TIME;
SELECT CURRENT_TIME;
SELECT LOCALTIME;

-- Timestamp
SELECT TIMESTAMP '2024-01-15 12:30:45';
SELECT '2024-01-15 12:30:45'::TIMESTAMP;
SELECT CURRENT_TIMESTAMP;
SELECT LOCALTIMESTAMP;
SELECT NOW();

-- Timestamp with timezone
SELECT TIMESTAMPTZ '2024-01-15 12:30:45+00';
SELECT '2024-01-15 12:30:45+00'::TIMESTAMPTZ;

-- Interval
SELECT INTERVAL '1 day';
SELECT INTERVAL '1 hour 30 minutes';
SELECT INTERVAL '1 year 2 months 3 days';
SELECT '1 day'::INTERVAL;
```

#### 4.4 Boolean Type
```sql
SELECT TRUE, FALSE;
SELECT 'true'::BOOLEAN, 'false'::BOOLEAN;
SELECT 't'::BOOLEAN, 'f'::BOOLEAN;
SELECT '1'::BOOLEAN, '0'::BOOLEAN;
SELECT 'yes'::BOOLEAN, 'no'::BOOLEAN;
```

#### 4.5 Binary Type
```sql
SELECT '\x48656c6c6f'::BYTEA;
SELECT 'hello'::BYTEA;
SELECT decode('48656c6c6f', 'hex');
SELECT encode('hello'::BYTEA, 'hex');
```

#### 4.6 UUID Type
```sql
SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID;
SELECT gen_random_uuid();
```

#### 4.7 JSON Types
```sql
-- JSON
SELECT '{"a": 1}'::JSON;
SELECT json_build_object('a', 1, 'b', 2);

-- JSONB
SELECT '{"a": 1}'::JSONB;
SELECT jsonb_build_object('a', 1, 'b', 2);

-- JSON access
SELECT '{"a": 1}'::JSON -> 'a';
SELECT '{"a": 1}'::JSON ->> 'a';
SELECT '{"a": {"b": 1}}'::JSON #> '{a,b}';
SELECT '{"a": {"b": 1}}'::JSON #>> '{a,b}';
SELECT '[1,2,3]'::JSON -> 0;

-- JSONB operators
SELECT '{"a": 1}'::JSONB @> '{"a": 1}'::JSONB;
SELECT '{"a": 1}'::JSONB <@ '{"a": 1, "b": 2}'::JSONB;
SELECT '{"a": 1}'::JSONB ? 'a';
SELECT '{"a": 1}'::JSONB ?| array['a', 'b'];
SELECT '{"a": 1}'::JSONB ?& array['a'];
```

#### 4.8 Array Types
```sql
SELECT ARRAY[1, 2, 3];
SELECT ARRAY['a', 'b', 'c'];
SELECT ARRAY[[1, 2], [3, 4]];
SELECT '{1,2,3}'::INTEGER[];

-- Array access
SELECT (ARRAY[1, 2, 3])[1];
SELECT (ARRAY[1, 2, 3])[1:2];

-- Array operators
SELECT ARRAY[1, 2] || ARRAY[3, 4];
SELECT ARRAY[1, 2] || 3;
SELECT 1 = ANY(ARRAY[1, 2, 3]);
SELECT 1 = ALL(ARRAY[1, 1, 1]);
SELECT ARRAY[1, 2] @> ARRAY[1];
SELECT ARRAY[1, 2] <@ ARRAY[1, 2, 3];
SELECT ARRAY[1, 2] && ARRAY[2, 3];
```

---

### 5. Functions

#### 5.1 String Functions
```sql
-- Basic
SELECT length('hello');
SELECT char_length('hello');
SELECT character_length('hello');
SELECT octet_length('hello');
SELECT bit_length('hello');

-- Case conversion
SELECT lower('HELLO');
SELECT upper('hello');
SELECT initcap('hello world');

-- Trimming
SELECT trim('  hello  ');
SELECT ltrim('  hello');
SELECT rtrim('hello  ');
SELECT btrim('xxhelloxx', 'x');
SELECT trim(leading 'x' from 'xxxhello');
SELECT trim(trailing 'x' from 'helloxxx');
SELECT trim(both 'x' from 'xxxhelloxxx');

-- Padding
SELECT lpad('hello', 10, ' ');
SELECT lpad('hello', 10, 'xy');
SELECT rpad('hello', 10, ' ');
SELECT rpad('hello', 10, 'xy');

-- Substring
SELECT substring('hello world' from 1 for 5);
SELECT substring('hello world', 1, 5);
SELECT substr('hello world', 1, 5);
SELECT left('hello', 3);
SELECT right('hello', 3);

-- Position/Search
SELECT position('lo' in 'hello');
SELECT strpos('hello', 'lo');

-- Replace
SELECT replace('hello world', 'world', 'there');
SELECT overlay('hello' placing 'XX' from 2 for 3);
SELECT translate('hello', 'el', 'ip');

-- Concatenation
SELECT concat('hello', ' ', 'world');
SELECT concat_ws(', ', 'a', 'b', 'c');
SELECT 'hello' || ' ' || 'world';

-- Split/Join
SELECT string_to_array('a,b,c', ',');
SELECT array_to_string(ARRAY['a', 'b', 'c'], ',');
SELECT split_part('a,b,c', ',', 2);

-- Formatting
SELECT format('Hello %s', 'world');
SELECT format('Value: %I = %L', 'column', 'value');

-- Regular expressions
SELECT regexp_replace('hello', 'l+', 'L');
SELECT regexp_matches('hello', 'l+');
SELECT regexp_split_to_array('a,b;c', '[,;]');
SELECT regexp_split_to_table('a,b;c', '[,;]');

-- Misc
SELECT reverse('hello');
SELECT repeat('ab', 3);
SELECT ascii('A');
SELECT chr(65);
SELECT md5('hello');
SELECT encode(digest('hello', 'sha256'), 'hex');
SELECT quote_literal('hello');
SELECT quote_ident('column');
SELECT quote_nullable(NULL);
```

#### 5.2 Numeric Functions
```sql
-- Basic arithmetic
SELECT abs(-5);
SELECT sign(-5);
SELECT ceil(4.3);
SELECT ceiling(4.3);
SELECT floor(4.7);
SELECT round(4.567);
SELECT round(4.567, 2);
SELECT trunc(4.567);
SELECT trunc(4.567, 2);

-- Division and modulo
SELECT div(10, 3);
SELECT mod(10, 3);
SELECT 10 % 3;

-- Power and roots
SELECT power(2, 10);
SELECT sqrt(16);
SELECT cbrt(27);
SELECT exp(1);
SELECT ln(10);
SELECT log(100);
SELECT log(2, 8);

-- Trigonometric
SELECT sin(0);
SELECT cos(0);
SELECT tan(0);
SELECT asin(0);
SELECT acos(1);
SELECT atan(0);
SELECT atan2(1, 1);
SELECT degrees(3.14159);
SELECT radians(180);
SELECT pi();

-- Random
SELECT random();
SELECT setseed(0.5);

-- Misc
SELECT greatest(1, 2, 3);
SELECT least(1, 2, 3);
SELECT width_bucket(5, 0, 10, 5);
SELECT scale(1.234::NUMERIC);
```

#### 5.3 Date/Time Functions
```sql
-- Current date/time
SELECT current_date;
SELECT current_time;
SELECT current_timestamp;
SELECT localtime;
SELECT localtimestamp;
SELECT now();
SELECT transaction_timestamp();
SELECT statement_timestamp();
SELECT clock_timestamp();

-- Extraction
SELECT extract(year from DATE '2024-01-15');
SELECT extract(month from DATE '2024-01-15');
SELECT extract(day from DATE '2024-01-15');
SELECT extract(hour from TIMESTAMP '2024-01-15 12:30:45');
SELECT extract(minute from TIMESTAMP '2024-01-15 12:30:45');
SELECT extract(second from TIMESTAMP '2024-01-15 12:30:45');
SELECT extract(dow from DATE '2024-01-15');
SELECT extract(doy from DATE '2024-01-15');
SELECT extract(week from DATE '2024-01-15');
SELECT extract(quarter from DATE '2024-01-15');
SELECT extract(epoch from TIMESTAMP '2024-01-15 12:30:45');
SELECT date_part('year', DATE '2024-01-15');

-- Truncation
SELECT date_trunc('year', TIMESTAMP '2024-06-15 12:30:45');
SELECT date_trunc('month', TIMESTAMP '2024-06-15 12:30:45');
SELECT date_trunc('day', TIMESTAMP '2024-06-15 12:30:45');
SELECT date_trunc('hour', TIMESTAMP '2024-06-15 12:30:45');

-- Arithmetic
SELECT DATE '2024-01-15' + INTERVAL '1 month';
SELECT DATE '2024-01-15' - INTERVAL '1 day';
SELECT DATE '2024-01-15' + 7;
SELECT DATE '2024-01-15' - DATE '2024-01-01';
SELECT TIMESTAMP '2024-01-15 12:00:00' - TIMESTAMP '2024-01-14 10:00:00';

-- Age
SELECT age(TIMESTAMP '2024-01-15', TIMESTAMP '2020-06-01');
SELECT age(TIMESTAMP '2024-01-15');

-- Formatting
SELECT to_char(NOW(), 'YYYY-MM-DD');
SELECT to_char(NOW(), 'HH24:MI:SS');
SELECT to_char(NOW(), 'Day, DD Month YYYY');
SELECT to_char(1234.56, '9999.99');
SELECT to_date('2024-01-15', 'YYYY-MM-DD');
SELECT to_timestamp('2024-01-15 12:30:45', 'YYYY-MM-DD HH24:MI:SS');

-- Make functions
SELECT make_date(2024, 1, 15);
SELECT make_time(12, 30, 45);
SELECT make_timestamp(2024, 1, 15, 12, 30, 45);
SELECT make_interval(years => 1, months => 2, days => 3);

-- Misc
SELECT isfinite(DATE '2024-01-15');
SELECT justify_days(INTERVAL '40 days');
SELECT justify_hours(INTERVAL '30 hours');
SELECT justify_interval(INTERVAL '1 month 35 days 30 hours');
```

#### 5.4 Aggregate Functions
```sql
-- Basic aggregates
SELECT count(*) FROM t;
SELECT count(col) FROM t;
SELECT count(DISTINCT col) FROM t;
SELECT sum(val) FROM t;
SELECT avg(val) FROM t;
SELECT min(val) FROM t;
SELECT max(val) FROM t;

-- Statistical aggregates
SELECT stddev(val) FROM t;
SELECT stddev_pop(val) FROM t;
SELECT stddev_samp(val) FROM t;
SELECT variance(val) FROM t;
SELECT var_pop(val) FROM t;
SELECT var_samp(val) FROM t;
SELECT corr(x, y) FROM t;
SELECT covar_pop(x, y) FROM t;
SELECT covar_samp(x, y) FROM t;
SELECT regr_slope(y, x) FROM t;
SELECT regr_intercept(y, x) FROM t;
SELECT regr_r2(y, x) FROM t;

-- Boolean aggregates
SELECT bool_and(flag) FROM t;
SELECT bool_or(flag) FROM t;
SELECT every(flag) FROM t;

-- Bit aggregates
SELECT bit_and(flags) FROM t;
SELECT bit_or(flags) FROM t;

-- Array aggregates
SELECT array_agg(col) FROM t;
SELECT array_agg(col ORDER BY col) FROM t;
SELECT array_agg(DISTINCT col) FROM t;

-- String aggregates
SELECT string_agg(name, ', ') FROM t;
SELECT string_agg(name, ', ' ORDER BY name) FROM t;

-- JSON aggregates
SELECT json_agg(row_to_json(t)) FROM t;
SELECT jsonb_agg(to_jsonb(t)) FROM t;
SELECT json_object_agg(key, value) FROM t;
SELECT jsonb_object_agg(key, value) FROM t;

-- Ordered-set aggregates
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY val) FROM t;
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY val) FROM t;
SELECT mode() WITHIN GROUP (ORDER BY val) FROM t;

-- Hypothetical-set aggregates
SELECT rank(5) WITHIN GROUP (ORDER BY val) FROM t;
SELECT dense_rank(5) WITHIN GROUP (ORDER BY val) FROM t;
SELECT percent_rank(5) WITHIN GROUP (ORDER BY val) FROM t;
SELECT cume_dist(5) WITHIN GROUP (ORDER BY val) FROM t;

-- FILTER clause
SELECT count(*) FILTER (WHERE active = true) FROM t;
SELECT sum(val) FILTER (WHERE category = 'A') FROM t;
```

#### 5.5 JSON Functions
```sql
-- Construction
SELECT json_build_object('a', 1, 'b', 2);
SELECT json_build_array(1, 2, 3);
SELECT row_to_json(row(1, 'hello'));
SELECT to_json('hello');
SELECT to_jsonb('hello');
SELECT array_to_json(ARRAY[1, 2, 3]);

-- Extraction
SELECT json_extract_path('{"a": {"b": 1}}'::JSON, 'a', 'b');
SELECT json_extract_path_text('{"a": {"b": 1}}'::JSON, 'a', 'b');
SELECT jsonb_extract_path('{"a": {"b": 1}}'::JSONB, 'a', 'b');
SELECT jsonb_extract_path_text('{"a": {"b": 1}}'::JSONB, 'a', 'b');

-- Querying
SELECT json_typeof('{"a": 1}'::JSON);
SELECT jsonb_typeof('{"a": 1}'::JSONB);
SELECT json_array_length('[1, 2, 3]'::JSON);
SELECT jsonb_array_length('[1, 2, 3]'::JSONB);
SELECT json_object_keys('{"a": 1, "b": 2}'::JSON);
SELECT jsonb_object_keys('{"a": 1, "b": 2}'::JSONB);

-- Modification (JSONB only)
SELECT jsonb_set('{"a": 1}'::JSONB, '{a}', '2');
SELECT jsonb_insert('{"a": 1}'::JSONB, '{b}', '2');
SELECT '{"a": 1}'::JSONB || '{"b": 2}'::JSONB;
SELECT '{"a": 1, "b": 2}'::JSONB - 'a';
SELECT '{"a": 1, "b": 2}'::JSONB - ARRAY['a'];
SELECT '{"a": {"b": 1}}'::JSONB #- '{a,b}';

-- Expansion
SELECT * FROM json_each('{"a": 1, "b": 2}'::JSON);
SELECT * FROM jsonb_each('{"a": 1, "b": 2}'::JSONB);
SELECT * FROM json_each_text('{"a": 1, "b": 2}'::JSON);
SELECT * FROM jsonb_each_text('{"a": 1, "b": 2}'::JSONB);
SELECT * FROM json_array_elements('[1, 2, 3]'::JSON);
SELECT * FROM jsonb_array_elements('[1, 2, 3]'::JSONB);
SELECT * FROM json_array_elements_text('["a", "b"]'::JSON);
SELECT * FROM jsonb_array_elements_text('["a", "b"]'::JSONB);

-- Path queries (PostgreSQL 12+)
SELECT jsonb_path_query('{"a": [1, 2, 3]}'::JSONB, '$.a[*]');
SELECT jsonb_path_query_array('{"a": [1, 2, 3]}'::JSONB, '$.a[*]');
SELECT jsonb_path_exists('{"a": 1}'::JSONB, '$.a');
SELECT jsonb_path_match('{"a": 1}'::JSONB, '$.a == 1');
```

#### 5.6 Array Functions
```sql
-- Construction
SELECT array_agg(col) FROM t;
SELECT ARRAY[1, 2, 3];
SELECT array_fill(0, ARRAY[3]);
SELECT array_fill(0, ARRAY[3, 2]);

-- Information
SELECT array_length(ARRAY[1, 2, 3], 1);
SELECT array_ndims(ARRAY[[1, 2], [3, 4]]);
SELECT array_dims(ARRAY[[1, 2], [3, 4]]);
SELECT cardinality(ARRAY[1, 2, 3]);

-- Search
SELECT array_position(ARRAY['a', 'b', 'c'], 'b');
SELECT array_positions(ARRAY['a', 'b', 'a'], 'a');

-- Modification
SELECT array_append(ARRAY[1, 2], 3);
SELECT array_prepend(0, ARRAY[1, 2]);
SELECT array_cat(ARRAY[1, 2], ARRAY[3, 4]);
SELECT array_remove(ARRAY[1, 2, 1], 1);
SELECT array_replace(ARRAY[1, 2, 1], 1, 10);

-- Transformation
SELECT unnest(ARRAY[1, 2, 3]);
SELECT array_to_string(ARRAY[1, 2, 3], ',');
SELECT string_to_array('1,2,3', ',');
```

#### 5.7 Conditional Functions
```sql
-- CASE
SELECT CASE WHEN x > 0 THEN 'positive' WHEN x < 0 THEN 'negative' ELSE 'zero' END FROM t;
SELECT CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t;

-- COALESCE
SELECT COALESCE(null_col, 'default') FROM t;
SELECT COALESCE(col1, col2, col3) FROM t;

-- NULLIF
SELECT NULLIF(col, 0) FROM t;

-- GREATEST/LEAST
SELECT GREATEST(1, 2, 3);
SELECT LEAST(1, 2, 3);

-- NVL variants
SELECT IFNULL(col, 'default') FROM t;  -- DuckDB specific, may need transpilation
```

#### 5.8 Type Conversion Functions
```sql
SELECT CAST(1 AS TEXT);
SELECT 1::TEXT;
SELECT CAST('123' AS INTEGER);
SELECT '123'::INTEGER;
SELECT to_number('1,234.56', '9,999.99');
SELECT to_hex(255);
SELECT to_binary('1010', 'bit');
```

---

### 6. Session Commands

#### 6.1 SET Commands
```sql
-- Should work (or be safely ignored)
SET search_path TO public, myschema;
SET search_path = 'public';
SET client_encoding = 'UTF8';
SET timezone = 'UTC';
SET datestyle = 'ISO, MDY';
SET intervalstyle = 'postgres';

-- Should be safely ignored
SET statement_timeout = '30s';
SET lock_timeout = '10s';
SET idle_in_transaction_session_timeout = '1min';
SET work_mem = '256MB';
SET maintenance_work_mem = '512MB';
SET effective_cache_size = '4GB';
SET random_page_cost = 1.1;
SET enable_seqscan = on;
SET enable_hashjoin = on;
SET log_statement = 'all';
SET client_min_messages = 'warning';
SET synchronous_commit = 'off';
SET default_transaction_isolation = 'read committed';
```

#### 6.2 SHOW Commands
```sql
SHOW search_path;
SHOW client_encoding;
SHOW timezone;
SHOW datestyle;
SHOW server_version;
SHOW server_encoding;
SHOW standard_conforming_strings;
SHOW all;
```

#### 6.3 Transaction Commands
```sql
BEGIN;
BEGIN TRANSACTION;
START TRANSACTION;
COMMIT;
END;
ROLLBACK;

-- Transaction modes (may be ignored)
BEGIN ISOLATION LEVEL READ COMMITTED;
BEGIN ISOLATION LEVEL REPEATABLE READ;
BEGIN ISOLATION LEVEL SERIALIZABLE;
BEGIN READ ONLY;
BEGIN READ WRITE;
```

---

### 7. System Catalog Queries

#### 7.1 pg_catalog Tables
```sql
-- Tables/relations
SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r';
SELECT * FROM pg_catalog.pg_tables;
SELECT * FROM pg_catalog.pg_views;
SELECT * FROM pg_catalog.pg_indexes;
SELECT * FROM pg_catalog.pg_matviews;

-- Columns
SELECT * FROM pg_catalog.pg_attribute WHERE attrelid = 'tablename'::regclass;

-- Types
SELECT * FROM pg_catalog.pg_type;
SELECT * FROM pg_catalog.pg_enum;

-- Namespaces/schemas
SELECT * FROM pg_catalog.pg_namespace;

-- Functions
SELECT * FROM pg_catalog.pg_proc;

-- Users/roles
SELECT * FROM pg_catalog.pg_roles;
SELECT * FROM pg_catalog.pg_user;

-- Databases
SELECT * FROM pg_catalog.pg_database;

-- Constraints
SELECT * FROM pg_catalog.pg_constraint;

-- Indexes
SELECT * FROM pg_catalog.pg_index;

-- Settings
SELECT * FROM pg_catalog.pg_settings;

-- Locks (may return empty)
SELECT * FROM pg_catalog.pg_locks;

-- Statistics
SELECT * FROM pg_catalog.pg_stat_user_tables;
SELECT * FROM pg_catalog.pg_stat_activity;
```

#### 7.2 information_schema
```sql
SELECT * FROM information_schema.tables WHERE table_schema = 'public';
SELECT * FROM information_schema.columns WHERE table_name = 'tablename';
SELECT * FROM information_schema.views;
SELECT * FROM information_schema.schemata;
SELECT * FROM information_schema.table_constraints;
SELECT * FROM information_schema.key_column_usage;
SELECT * FROM information_schema.referential_constraints;
SELECT * FROM information_schema.routines;
SELECT * FROM information_schema.parameters;
```

#### 7.3 psql Meta-Commands (Generated SQL)
```sql
-- \dt - list tables
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','')
AND n.nspname <> 'pg_catalog'
AND n.nspname <> 'information_schema'
ORDER BY 1,2;

-- \d tablename - describe table
SELECT a.attname,
  pg_catalog.format_type(a.atttypid, a.atttypmod),
  a.attnotnull,
  pg_catalog.pg_get_expr(d.adbin, d.adrelid) as default
FROM pg_catalog.pg_attribute a
LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum)
WHERE a.attrelid = 'tablename'::regclass
AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum;

-- \di - list indexes
SELECT
  n.nspname as "Schema",
  c.relname as "Name",
  i.indisunique as "Unique",
  i.indisprimary as "Primary"
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema');

-- \dn - list schemas
SELECT n.nspname AS "Name",
  pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
FROM pg_catalog.pg_namespace n
WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
ORDER BY 1;

-- \l - list databases
SELECT d.datname as "Name",
  pg_catalog.pg_get_userbyid(d.datdba) as "Owner",
  pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding"
FROM pg_catalog.pg_database d
ORDER BY 1;
```

---

### 8. Client Tool Compatibility

#### 8.1 Metabase Introspection Queries
```sql
-- Get schemas
SELECT nspname FROM pg_namespace
WHERE nspname NOT IN ('information_schema', 'pg_catalog')
AND nspname NOT LIKE 'pg_%';

-- Get tables
SELECT c.relname, n.nspname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
AND n.nspname NOT IN ('information_schema', 'pg_catalog');

-- Get columns
SELECT attname, format_type(atttypid, atttypmod) as type, attnotnull
FROM pg_attribute
WHERE attrelid = 'schemaname.tablename'::regclass
AND attnum > 0
AND NOT attisdropped;

-- Get primary key
SELECT a.attname
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = 'tablename'::regclass
AND i.indisprimary;
```

#### 8.2 Grafana Introspection Queries
```sql
-- Time column detection
SELECT column_name
FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2
AND data_type IN ('timestamp without time zone', 'timestamp with time zone', 'date');

-- Table list for autocomplete
SELECT table_name FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
AND table_type = 'BASE TABLE';
```

#### 8.3 Superset Introspection Queries
```sql
-- Get all tables
SELECT table_name, table_schema
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_name;

-- Get columns with types
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = :schema AND table_name = :table
ORDER BY ordinal_position;
```

#### 8.4 Tableau Introspection Queries
```sql
-- Connection test
SELECT 1;

-- Schema discovery
SELECT DISTINCT table_schema
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema', 'pg_catalog');

-- Table discovery
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = :schema;

-- Column discovery with detailed metadata
SELECT
  column_name,
  ordinal_position,
  column_default,
  is_nullable,
  data_type,
  character_maximum_length,
  numeric_precision,
  numeric_scale
FROM information_schema.columns
WHERE table_schema = :schema AND table_name = :table;
```

#### 8.5 DBeaver Introspection Queries
```sql
-- Catalog info
SELECT current_database(), current_schema(), session_user, current_user;

-- Full table metadata
SELECT
  c.oid,
  c.relname,
  c.relkind,
  n.nspname,
  pg_get_userbyid(c.relowner) as owner,
  obj_description(c.oid, 'pg_class') as description
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v', 'm')
AND n.nspname = :schema;

-- Column metadata
SELECT
  a.attnum,
  a.attname,
  t.typname,
  a.atttypmod,
  a.attnotnull,
  a.atthasdef,
  pg_get_expr(d.adbin, d.adrelid) as default_value,
  col_description(a.attrelid, a.attnum) as description
FROM pg_attribute a
JOIN pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = :table_oid
AND a.attnum > 0
AND NOT a.attisdropped
ORDER BY a.attnum;
```

#### 8.6 ETL Tool Queries (Fivetran/Airbyte)

```sql
-- Schema sync
SELECT
  table_schema,
  table_name,
  column_name,
  ordinal_position,
  data_type,
  is_nullable,
  column_default
FROM information_schema.columns
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name, ordinal_position;

-- Primary key detection
SELECT
  tc.table_schema,
  tc.table_name,
  kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY';

-- Change tracking (if supported)
SELECT xmin, * FROM tablename WHERE xmin > :last_xmin;
```

#### 8.7 dbt Queries
```sql
-- Relation existence check
SELECT count(*) FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = :name AND n.nspname = :schema;

-- Get columns for relation
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = :schema AND table_name = :name
ORDER BY ordinal_position;

-- Check if schema exists
SELECT count(*) FROM pg_namespace WHERE nspname = :schema;

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS :schema;
```

---

### 9. Protocol-Level Tests

#### 9.1 Simple Query Protocol
```
# Client sends: Query message
> Q "SELECT 1"

# Server sends: RowDescription, DataRow, CommandComplete, ReadyForQuery
< T (1 column: int4)
< D (1 row: 1)
< C "SELECT 1"
< Z (idle)
```

#### 9.2 Extended Query Protocol
```
# Parse
> P "" "SELECT $1::int + $2::int" [int4, int4]
< 1 (ParseComplete)

# Bind
> B "" "" [1, 2] [text, text] [binary]
< 2 (BindComplete)

# Describe
> D P ""
< t (ParameterDescription: [int4, int4])

# Execute
> E "" 0
< D (1 row: 3)
< C "SELECT 1"

# Sync
> S
< Z (idle)
```

#### 9.3 COPY Protocol
```
# COPY TO STDOUT
> Q "COPY tablename TO STDOUT WITH (FORMAT CSV, HEADER)"
< H (CopyOutResponse: text format)
< d "col1,col2,col3\n"
< d "1,a,true\n"
< d "2,b,false\n"
< c (CopyDone)
< C "COPY 2"
< Z (idle)

# COPY FROM STDIN
> Q "COPY tablename FROM STDIN WITH (FORMAT CSV)"
< G (CopyInResponse: text format)
> d "1,a,true\n"
> d "2,b,false\n"
> c (CopyDone)
< C "COPY 2"
< Z (idle)
```

#### 9.4 Error Handling
```
# Syntax error
> Q "SELEC 1"
< E (severity: ERROR, code: 42601, message: syntax error)
< Z (idle)

# Table not found
> Q "SELECT * FROM nonexistent"
< E (severity: ERROR, code: 42P01, message: relation does not exist)
< Z (idle)

# Column not found
> Q "SELECT nonexistent FROM tablename"
< E (severity: ERROR, code: 42703, message: column does not exist)
< Z (idle)
```

---

### 10. Explicit Out-of-Scope (OLTP Features)

The following features are explicitly NOT tested:

- **Stored Procedures/Functions**: CREATE FUNCTION, DO blocks, PL/pgSQL
- **Triggers**: CREATE TRIGGER, trigger functions
- **Row-Level Security**: CREATE POLICY, ALTER TABLE ENABLE RLS
- **Replication**: Logical replication, streaming replication
- **LISTEN/NOTIFY**: Pub/sub messaging
- **Advisory Locks**: pg_advisory_lock, pg_try_advisory_lock
- **Transaction Isolation**: Specific isolation level behavior (READ COMMITTED vs SERIALIZABLE)
- **Cursors**: DECLARE CURSOR, FETCH, CLOSE (except in simple cases)
- **Savepoints**: SAVEPOINT, ROLLBACK TO SAVEPOINT
- **Two-Phase Commit**: PREPARE TRANSACTION, COMMIT PREPARED

---

## Test Implementation Structure

```
tests/
├── integration/
│   ├── docker-compose.yml        # PostgreSQL 16 container
│   ├── setup_test.go             # Test harness initialization
│   ├── compare.go                # Result comparison utilities
│   ├── fixtures/
│   │   ├── schema.sql            # Test table definitions
│   │   └── data.sql              # Test data
│   ├── dql_test.go               # SELECT, JOIN, CTE, Window tests
│   ├── ddl_test.go               # CREATE, ALTER, DROP tests
│   ├── dml_test.go               # INSERT, UPDATE, DELETE tests
│   ├── types_test.go             # Data type tests
│   ├── functions_test.go         # Function compatibility tests
│   ├── catalog_test.go           # pg_catalog/information_schema tests
│   ├── clients/
│   │   ├── metabase_test.go      # Metabase query compatibility
│   │   ├── grafana_test.go       # Grafana query compatibility
│   │   ├── superset_test.go      # Superset query compatibility
│   │   ├── tableau_test.go       # Tableau query compatibility
│   │   ├── dbeaver_test.go       # DBeaver query compatibility
│   │   ├── fivetran_test.go      # Fivetran pattern compatibility
│   │   └── dbt_test.go           # dbt query compatibility
│   └── protocol_test.go          # Wire protocol tests
└── testdata/
    └── queries/                  # SQL files organized by category
```

---

## Test Execution

### Running Tests

```bash
# Start PostgreSQL container
docker-compose -f tests/integration/docker-compose.yml up -d

# Run all integration tests
go test -v ./tests/integration/...

# Run specific test category
go test -v ./tests/integration/... -run TestDQL
go test -v ./tests/integration/... -run TestClients

# Run with verbose comparison output
DUCKGRES_TEST_VERBOSE=1 go test -v ./tests/integration/...

# Generate compatibility report
go test -v ./tests/integration/... -json > report.json
```

### Test Matrix

| Category | Subcategory | Est. Test Count |
|----------|-------------|-----------------|
| DQL | SELECT basics | 50 |
| DQL | WHERE clauses | 40 |
| DQL | ORDER/LIMIT | 20 |
| DQL | GROUP BY/HAVING | 30 |
| DQL | JOINs | 40 |
| DQL | Subqueries | 30 |
| DQL | CTEs | 20 |
| DQL | Set operations | 15 |
| DQL | Window functions | 50 |
| DDL | CREATE/ALTER/DROP | 60 |
| DML | INSERT/UPDATE/DELETE | 50 |
| Types | All data types | 80 |
| Functions | String | 60 |
| Functions | Numeric | 40 |
| Functions | Date/Time | 50 |
| Functions | Aggregate | 40 |
| Functions | JSON | 50 |
| Functions | Array | 30 |
| Catalog | pg_catalog | 30 |
| Catalog | information_schema | 20 |
| Catalog | psql commands | 15 |
| Clients | BI tools | 30 |
| Clients | ETL tools | 20 |
| Protocol | Wire protocol | 25 |
| **Total** | | **~925** |

---

## Success Criteria

1. **Core Compatibility**: All DQL, DDL, DML queries return semantically equivalent results
2. **Type Mapping**: All major types are correctly mapped and round-trip properly
3. **Function Parity**: All tested functions produce equivalent output
4. **Catalog Queries**: psql meta-commands and BI tool introspection work correctly
5. **Protocol Compliance**: Extended query protocol works with prepared statements
6. **Error Handling**: Error codes match PostgreSQL for common errors

## Known Differences to Document

- JSONB stored as JSON (ordering may differ)
- Some network types mapped to TEXT
- SERIAL types may behave differently
- Collation differences
- Numeric precision edge cases

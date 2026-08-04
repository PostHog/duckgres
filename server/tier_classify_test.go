package server

import "testing"

func TestClassifyStatementTier(t *testing.T) {
	smallOK := []string{
		"SELECT count(1) FROM posthog.events",
		"SELECT * FROM posthog.events LIMIT 10",
		"select 1",
		"EXPLAIN SELECT * FROM t",
		"EXPLAIN ANALYZE SELECT * FROM t",
		"SHOW search_path",
		"WITH a AS (SELECT 1) SELECT * FROM a",
		"SELECT * FROM a JOIN b ON a.id = b.id WHERE a.x > 5 ORDER BY b.y",
		"SELECT 1; SELECT 2",
	}
	pinning := []string{
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET x = 1",
		"DELETE FROM t",
		"CREATE TABLE t (i int)",
		"CREATE TEMP TABLE t (i int)",
		"CREATE TEMPORARY TABLE t AS SELECT 1",
		"DROP TABLE t",
		"ALTER TABLE t ADD COLUMN j int",
		"BEGIN",
		"START TRANSACTION",
		"COPY t FROM STDIN",
		"COPY t TO STDOUT",
		"SET search_path TO foo",
		"CREATE SECRET s (TYPE S3)", // unparseable by pg_query -> conservative
		"USE ducklake",              // DuckDB-only spelling -> unparseable -> conservative
		"WITH w AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM w", // writable CTE
		"SELECT * INTO t2 FROM t",                  // SELECT INTO creates a table
		"EXPLAIN ANALYZE INSERT INTO t VALUES (1)", // EXPLAIN ANALYZE executes the DML
		"SELECT 1; INSERT INTO t VALUES (1)",       // any pinning stmt pins the batch
		"CREATE VIEW v AS SELECT 1",
		"TRUNCATE t",
		"MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
		"DECLARE c CURSOR FOR SELECT 1", // cursor state lives on the session
		"PREPARE p AS SELECT 1",
		"VACUUM",
		"garbage that is not sql",
	}
	for _, q := range smallOK {
		if got := classifyStatementTier(q); got != tierSmallOK {
			t.Errorf("classifyStatementTier(%q) = pinning, want smallOK", q)
		}
	}
	for _, q := range pinning {
		if got := classifyStatementTier(q); got != tierPinning {
			t.Errorf("classifyStatementTier(%q) = smallOK, want pinning", q)
		}
	}
}

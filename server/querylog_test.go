package server

import (
	"database/sql"
	"testing"
	"time"
)

func TestClassifyQuery(t *testing.T) {
	tests := []struct {
		cmdType string
		want    string
	}{
		{"SELECT", "Select"},
		{"SHOW", "Select"},
		{"TABLE", "Select"},
		{"VALUES", "Select"},
		{"EXPLAIN", "Select"},
		{"INSERT", "Insert"},
		{"UPDATE", "Update"},
		{"DELETE", "Delete"},
		{"CREATE", "DDL"},
		{"ALTER", "DDL"},
		{"DROP", "DDL"},
		{"TRUNCATE", "DDL"},
		{"COPY", "Copy"},
		{"BEGIN", "Utility"},
		{"COMMIT", "Utility"},
		{"ROLLBACK", "Utility"},
		{"SET", "Utility"},
		{"RESET", "Utility"},
		{"DISCARD", "Utility"},
		{"DEALLOCATE", "Utility"},
		{"LISTEN", "Utility"},
		{"NOTIFY", "Utility"},
		{"UNLISTEN", "Utility"},
		{"UNKNOWN", "Utility"},
		{"", "Utility"},
	}

	for _, tt := range tests {
		t.Run(tt.cmdType, func(t *testing.T) {
			got := classifyQuery(tt.cmdType)
			if got != tt.want {
				t.Errorf("classifyQuery(%q) = %q, want %q", tt.cmdType, got, tt.want)
			}
		})
	}
}

func TestNormalizeQueryHash(t *testing.T) {
	// Same query with different literal values should produce the same hash
	h1 := normalizeQueryHash("SELECT * FROM users WHERE id = 1")
	h2 := normalizeQueryHash("SELECT * FROM users WHERE id = 2")
	if h1 != h2 {
		t.Errorf("Expected same hash for queries differing only in literals, got %d and %d", h1, h2)
	}

	// Same query with different string literals
	h3 := normalizeQueryHash("SELECT * FROM users WHERE name = 'alice'")
	h4 := normalizeQueryHash("SELECT * FROM users WHERE name = 'bob'")
	if h3 != h4 {
		t.Errorf("Expected same hash for queries differing only in string literals, got %d and %d", h3, h4)
	}

	// Different queries should produce different hashes
	h5 := normalizeQueryHash("SELECT * FROM users WHERE id = 1")
	h6 := normalizeQueryHash("SELECT * FROM orders WHERE id = 1")
	if h5 == h6 {
		t.Errorf("Expected different hashes for different queries, both got %d", h5)
	}

	// Whitespace normalization
	h7 := normalizeQueryHash("SELECT  *  FROM  users  WHERE  id = 1")
	h8 := normalizeQueryHash("SELECT * FROM users WHERE id = 1")
	if h7 != h8 {
		t.Errorf("Expected same hash after whitespace normalization, got %d and %d", h7, h8)
	}

	// Case normalization
	h9 := normalizeQueryHash("select * from users where id = 1")
	h10 := normalizeQueryHash("SELECT * FROM users WHERE id = 1")
	if h9 != h10 {
		t.Errorf("Expected same hash after case normalization, got %d and %d", h9, h10)
	}

	// Keywords in IS predicates should not be normalized away as literals.
	h11 := normalizeQueryHash("SELECT * FROM users WHERE active IS TRUE")
	h12 := normalizeQueryHash("SELECT * FROM users WHERE active IS NULL")
	if h11 == h12 {
		t.Errorf("Expected different hashes for IS TRUE vs IS NULL predicates, both got %d", h11)
	}
}

func TestIsQueryLogSelfReferential(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{"SELECT * FROM system.query_log", true},
		{"SELECT * FROM ducklake.system.query_log ORDER BY event_time DESC", true},
		{"SELECT * FROM SYSTEM.QUERY_LOG", true},
		{"SELECT * FROM users", false},
		{"INSERT INTO logs VALUES (1, 'test')", false},
		{"SELECT query_log FROM metadata", false}, // "query_log" without "system." prefix is not self-referential
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			got := isQueryLogSelfReferential(tt.query)
			if got != tt.want {
				t.Errorf("isQueryLogSelfReferential(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

func TestTruncateQuery(t *testing.T) {
	short := "SELECT 1"
	if truncateQuery(short) != short {
		t.Error("Short query should not be truncated")
	}

	long := make([]byte, maxQueryLength+100)
	for i := range long {
		long[i] = 'x'
	}
	result := truncateQuery(string(long))
	if len(result) != maxQueryLength {
		t.Errorf("Expected truncated length %d, got %d", maxQueryLength, len(result))
	}
}

func TestQueryLogNonBlocking(t *testing.T) {
	// Create a logger with a tiny channel to test non-blocking behavior
	ql := &QueryLogger{
		ch:   make(chan QueryLogEntry, 1),
		cfg:  QueryLogConfig{BatchSize: 1000},
		done: make(chan struct{}),
	}

	// Fill the channel
	ql.ch <- QueryLogEntry{EventTime: time.Now(), Query: "first"}

	// This should not block
	done := make(chan bool, 1)
	go func() {
		ql.Log(QueryLogEntry{EventTime: time.Now(), Query: "second"})
		done <- true
	}()

	select {
	case <-done:
		// Success - Log returned without blocking
	case <-time.After(100 * time.Millisecond):
		t.Error("Log() blocked when channel was full")
	}
}

func TestQueryLoggerStopIsIdempotent(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}

	ql := &QueryLogger{
		db: db,
		cfg: QueryLogConfig{
			BatchSize:       1,
			FlushInterval:   time.Hour,
			CompactInterval: time.Hour,
		},
		ch:   make(chan QueryLogEntry, 1),
		done: make(chan struct{}),
	}

	go ql.flushLoop()

	ql.Stop()
	ql.Stop()
}

func TestHighBitHashInsertIntoBigint(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE test_hash (h BIGINT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// FNV-1a produces uint64 values; ~50% have the high bit set, which
	// becomes negative when stored as int64. Verify BIGINT accepts these.
	var highBitHash int64 = -0x2900_0000_0000_0000 // equivalent to uint64(0xD700_0000_0000_0000)

	_, err = db.Exec("INSERT INTO test_hash VALUES ($1)", highBitHash)
	if err != nil {
		t.Fatalf("insert failed (this was the original bug): %v", err)
	}

	var stored int64
	err = db.QueryRow("SELECT h FROM test_hash").Scan(&stored)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if stored != highBitHash {
		t.Errorf("hash round-trip mismatch: got %d, want %d", stored, highBitHash)
	}
}

// TestHighBitHashRejectsUbigint documents that DuckDB's UBIGINT column rejects
// negative int64 values — this is the DuckDB behavior that motivated switching
// the query_log column to BIGINT. If a future DuckDB version changes this
// behavior (e.g., bitwise reinterpretation), this test failing means the column
// type choice should be re-evaluated, not that duckgres has regressed.
func TestHighBitHashRejectsUbigint(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE TABLE test_hash_u (h UBIGINT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	var highBitHash int64 = -0x2900_0000_0000_0000
	_, err = db.Exec("INSERT INTO test_hash_u VALUES ($1)", highBitHash)
	if err == nil {
		t.Fatal("expected error inserting negative int64 into UBIGINT, but insert succeeded")
	}
}

// TestUbigintToBigintMigrationViaDrop simulates the migration path: an existing
// table with a UBIGINT column is dropped and recreated as BIGINT, after which
// negative int64 hash values insert successfully.
func TestUbigintToBigintMigrationViaDrop(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Start with the old schema (UBIGINT)
	_, err = db.Exec("CREATE TABLE query_log (normalized_query_hash UBIGINT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Verify negative int64 fails with UBIGINT (the bug)
	var highBitHash int64 = -2949375574818077459
	_, err = db.Exec("INSERT INTO query_log VALUES ($1)", highBitHash)
	if err == nil {
		t.Fatal("expected UBIGINT to reject negative int64")
	}

	// Check column type via information_schema (simulates the migration check)
	var colType string
	err = db.QueryRow("SELECT data_type FROM information_schema.columns WHERE table_name = 'query_log' AND column_name = 'normalized_query_hash'").Scan(&colType)
	if err != nil {
		t.Fatalf("query information_schema: %v", err)
	}
	if colType != "UBIGINT" {
		t.Fatalf("expected UBIGINT, got %s", colType)
	}

	// Simulate migration: drop and recreate with BIGINT
	_, err = db.Exec("DROP TABLE query_log")
	if err != nil {
		t.Fatalf("drop table: %v", err)
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS query_log (normalized_query_hash BIGINT)")
	if err != nil {
		t.Fatalf("recreate table: %v", err)
	}

	// Now the insert should succeed
	_, err = db.Exec("INSERT INTO query_log VALUES ($1)", highBitHash)
	if err != nil {
		t.Fatalf("insert after migration failed: %v", err)
	}

	var stored int64
	err = db.QueryRow("SELECT normalized_query_hash FROM query_log").Scan(&stored)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if stored != highBitHash {
		t.Errorf("round-trip mismatch: got %d, want %d", stored, highBitHash)
	}
}

func TestSplitHostPort(t *testing.T) {
	host, port, err := splitHostPort("192.168.1.1:5432")
	if err != nil {
		t.Fatal(err)
	}
	if host != "192.168.1.1" || port != "5432" {
		t.Errorf("Got host=%q port=%q", host, port)
	}
}

func TestSplitHostPortIPv6(t *testing.T) {
	host, port, err := splitHostPort("[::1]:5432")
	if err != nil {
		t.Fatal(err)
	}
	if host != "::1" || port != "5432" {
		t.Errorf("Got host=%q port=%q", host, port)
	}
}

func TestParsePort(t *testing.T) {
	p, err := parsePort("5432")
	if err != nil {
		t.Fatal(err)
	}
	if p != 5432 {
		t.Errorf("Expected 5432, got %d", p)
	}

	_, err = parsePort("abc")
	if err == nil {
		t.Error("Expected error for non-numeric port")
	}

	_, err = parsePort("")
	if err == nil {
		t.Error("Expected error for empty port")
	}
}

func TestTruncateNullableQuery(t *testing.T) {
	if truncateNullableQuery(nil) != nil {
		t.Fatal("nil query should stay nil")
	}

	short := "SELECT 1"
	shortPtr := &short
	if got := truncateNullableQuery(shortPtr); got == nil || *got != short {
		t.Fatalf("expected short query unchanged, got %#v", got)
	}

	long := make([]byte, maxQueryLength+100)
	for i := range long {
		long[i] = 'x'
	}
	longStr := string(long)
	longPtr := &longStr
	got := truncateNullableQuery(longPtr)
	if got == nil {
		t.Fatal("expected non-nil truncated query")
	} else if len(*got) != maxQueryLength {
		t.Fatalf("expected truncated length %d, got %d", maxQueryLength, len(*got))
	}
}

func TestEscapeSQLStringLiteral(t *testing.T) {
	got := escapeSQLStringLiteral("postgres:host=localhost password=pa'ss")
	want := "postgres:host=localhost password=pa''ss"
	if got != want {
		t.Fatalf("escapeSQLStringLiteral() = %q, want %q", got, want)
	}
}

package server

import (
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

func TestSplitHostPort(t *testing.T) {
	host, port, err := splitHostPort("192.168.1.1:5432")
	if err != nil {
		t.Fatal(err)
	}
	if host != "192.168.1.1" || port != "5432" {
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
}

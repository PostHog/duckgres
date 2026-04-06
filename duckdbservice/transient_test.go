package duckdbservice

import (
	"errors"
	"strings"
	"testing"
)

func TestIsTransactionControlStmt(t *testing.T) {
	tests := []struct {
		query    string
		expected bool
	}{
		{"COMMIT", true},
		{"commit", true},
		{"  COMMIT  ", true},
		{"COMMIT;", true},
		{"ROLLBACK", true},
		{"rollback", true},
		{"END", true},
		{"END TRANSACTION", true},
		{"BEGIN", false},
		{"begin", false},
		{"INSERT INTO t VALUES (1)", false},
		{"SELECT 1", false},
		{"CREATE TABLE t (id INT)", false},
		{"", false},
		{"   ", false},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			if got := isTransactionControlStmt(tt.query); got != tt.expected {
				t.Errorf("isTransactionControlStmt(%q) = %v, want %v", tt.query, got, tt.expected)
			}
		})
	}
}

func TestIsTransientDuckLakeError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil", nil, false},
		{"generic error", errors.New("syntax error"), false},
		{"dns resolution", errors.New(`could not translate host name "foo.rds.amazonaws.com" to address: Temporary failure in name resolution`), true},
		{"name resolution", errors.New("IO Error: Failed to get data file list from DuckLake: Unable to connect to Postgres: name resolution failed"), true},
		{"connection refused", errors.New("Connection refused"), true},
		{"connection reset", errors.New("read tcp: connection reset by peer"), true},
		{"connection timed out", errors.New("connection timed out"), true},
		{"server closed", errors.New("server closed the connection unexpectedly"), true},
		{"SSL closed", errors.New(`Failed to execute query "COMMIT": SSL connection has been closed unexpectedly`), true},
		{"current transaction aborted", errors.New("Current transaction is aborted, commands ignored until end of transaction block"), true},
		{"no route", errors.New("no route to host"), true},
		{"network unreachable", errors.New("network is unreachable"), true},
		{"auth error", errors.New("password authentication failed for user"), false},
		{"table not found", errors.New("Table with name foo does not exist"), false},
		{"transaction conflict is not transient", errors.New(`Transaction conflict - attempting to insert into table with index "29784"`), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransientDuckLakeError(tt.err); got != tt.expected {
				t.Errorf("isTransientDuckLakeError(%v) = %v, want %v", tt.err, got, tt.expected)
			}
		})
	}
}

func TestRetryOnTransientSucceedsAfterRetry(t *testing.T) {
	calls := 0
	result, err := retryOnTransient(func() (string, error) {
		calls++
		if calls < 3 {
			return "", errors.New("could not translate host name: Temporary failure in name resolution")
		}
		return "ok", nil
	})

	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if result != "ok" {
		t.Fatalf("expected result 'ok', got %q", result)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryOnTransientNoRetryForNonTransient(t *testing.T) {
	calls := 0
	_, err := retryOnTransient(func() (string, error) {
		calls++
		return "", errors.New("syntax error at position 42")
	})

	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("expected 1 call (no retry for non-transient), got %d", calls)
	}
}

func TestIsDuckLakeTransactionConflict(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil", nil, false},
		{"generic error", errors.New("syntax error"), false},
		{"transaction conflict", errors.New(`Transaction conflict - attempting to insert into table with index "29784"`), true},
		{"transaction conflict variant", errors.New("Transaction conflict on commit"), true},
		{"SSL closed is not conflict", errors.New("SSL connection has been closed unexpectedly"), false},
		{"connection refused is not conflict", errors.New("Connection refused"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isDuckLakeTransactionConflict(tt.err); got != tt.expected {
				t.Errorf("isDuckLakeTransactionConflict(%v) = %v, want %v", tt.err, got, tt.expected)
			}
		})
	}
}

func TestRetryOnConflictSucceedsAfterRetry(t *testing.T) {
	calls := 0
	result, err := retryOnConflict(func() (string, error) {
		calls++
		if calls <= 2 {
			return "", errors.New(`Transaction conflict - attempting to insert into table with index "29784"`)
		}
		return "ok", nil
	})

	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if result != "ok" {
		t.Fatalf("expected result 'ok', got %q", result)
	}
	// Initial call fails (handled externally), then retryOnConflict is called:
	// attempt 1 fails (calls=2), attempt 2 succeeds (calls=3)
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryOnConflictExhaustsRetries(t *testing.T) {
	calls := 0
	_, err := retryOnConflict(func() (string, error) {
		calls++
		return "", errors.New("Transaction conflict on commit")
	})

	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if calls != conflictMaxRetries {
		t.Fatalf("expected %d calls, got %d", conflictMaxRetries, calls)
	}
	if !strings.Contains(err.Error(), "Transaction conflict on commit") {
		t.Fatalf("expected wrapped original error, got: %v", err)
	}
}

func TestRetryOnConflictStopsOnNonConflictError(t *testing.T) {
	calls := 0
	_, err := retryOnConflict(func() (string, error) {
		calls++
		return "", errors.New("syntax error at position 42")
	})

	if err == nil {
		t.Fatal("expected error")
	}
	// retryOnConflict always makes one attempt (the first retry); when the
	// error is not a transaction conflict it stops immediately.
	if calls != 1 {
		t.Fatalf("expected 1 call (stop on non-conflict error), got %d", calls)
	}
}

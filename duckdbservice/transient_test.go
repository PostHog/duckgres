package duckdbservice

import (
	"errors"
	"testing"
)

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
		{"no route", errors.New("no route to host"), true},
		{"network unreachable", errors.New("network is unreachable"), true},
		{"auth error", errors.New("password authentication failed for user"), false},
		{"table not found", errors.New("Table with name foo does not exist"), false},
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

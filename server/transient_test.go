package server

import (
	"errors"
	"strings"
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

func TestRetryOnTransientAttachSucceedsAfterRetry(t *testing.T) {
	calls := 0
	err := retryOnTransientAttach(func() error {
		calls++
		if calls < 3 {
			return errors.New("could not translate host name: Temporary failure in name resolution")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryOnTransientAttachExhaustsRetries(t *testing.T) {
	calls := 0
	err := retryOnTransientAttach(func() error {
		calls++
		return errors.New("could not translate host name: Temporary failure in name resolution")
	})

	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if calls != 4 { // 1 initial + 3 retries
		t.Fatalf("expected 4 calls (1 initial + 3 retries), got %d", calls)
	}
}

func TestRetryOnTransientAttachNoRetryForNonTransient(t *testing.T) {
	calls := 0
	err := retryOnTransientAttach(func() error {
		calls++
		return errors.New("syntax error at position 42")
	})

	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("expected 1 call (no retry for non-transient), got %d", calls)
	}
}

func TestIsAlterTableNotTableError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "classic not a table error",
			err:  errors.New("Catalog Error: cannot use ALTER TABLE on view because it is not a table"),
			want: true,
		},
		{
			name: "can only modify view with alter view statement",
			err:  errors.New("Catalog Error: Can only modify view with ALTER VIEW statement"),
			want: true,
		},
		{
			name: "qualified view rename reports missing table",
			err: errors.New(
				"Catalog Error: Table with name stg_customers__dbt_tmp does not exist!\nDid you mean \"stg_customers\"?",
			),
			want: false,
		},
		{
			name: "unrelated missing table stays false",
			err:  errors.New("Catalog Error: Table with name users does not exist!"),
			want: false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isAlterTableNotTableError(tt.err); got != tt.want {
				t.Fatalf("isAlterTableNotTableError(%v) = %v, want %v", tt.err, got, tt.want)
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

func TestRecoverAbortedTransactionRetriesAfterRollbackInAutocommit(t *testing.T) {
	rollbackCalls := 0
	retryCalls := 0

	result, err, recovered := recoverAbortedTransaction(
		errors.New("TransactionContext Error: Current transaction is aborted (please ROLLBACK)"),
		true,
		func() error {
			rollbackCalls++
			return nil
		},
		func() (string, error) {
			retryCalls++
			return "ok", nil
		},
	)

	if !recovered {
		t.Fatal("expected aborted transaction recovery to trigger")
	}
	if err != nil {
		t.Fatalf("expected retry to succeed, got error: %v", err)
	}
	if result != "ok" {
		t.Fatalf("expected result 'ok', got %q", result)
	}
	if rollbackCalls != 1 {
		t.Fatalf("expected 1 rollback, got %d", rollbackCalls)
	}
	if retryCalls != 1 {
		t.Fatalf("expected 1 retry, got %d", retryCalls)
	}
}

func TestRecoverAbortedTransactionSkipsRecoveryInsideUserTransaction(t *testing.T) {
	rollbackCalls := 0
	retryCalls := 0
	origErr := errors.New("TransactionContext Error: Current transaction is aborted (please ROLLBACK)")

	_, err, recovered := recoverAbortedTransaction(
		origErr,
		false,
		func() error {
			rollbackCalls++
			return nil
		},
		func() (string, error) {
			retryCalls++
			return "ok", nil
		},
	)

	if recovered {
		t.Fatal("expected aborted transaction recovery to be skipped")
	}
	if !errors.Is(err, origErr) {
		t.Fatalf("expected original error, got %v", err)
	}
	if rollbackCalls != 0 {
		t.Fatalf("expected 0 rollbacks, got %d", rollbackCalls)
	}
	if retryCalls != 0 {
		t.Fatalf("expected 0 retries, got %d", retryCalls)
	}
}

func TestRecoverAbortedTransactionReturnsRollbackFailure(t *testing.T) {
	rollbackCalls := 0
	retryCalls := 0

	_, err, recovered := recoverAbortedTransaction(
		errors.New("TransactionContext Error: Current transaction is aborted (please ROLLBACK)"),
		true,
		func() error {
			rollbackCalls++
			return errors.New("rollback transport failed")
		},
		func() (string, error) {
			retryCalls++
			return "ok", nil
		},
	)

	if !recovered {
		t.Fatal("expected aborted transaction recovery to trigger")
	}
	if err == nil || !strings.Contains(err.Error(), "rollback transport failed") {
		t.Fatalf("expected rollback failure, got %v", err)
	}
	if rollbackCalls != 1 {
		t.Fatalf("expected 1 rollback, got %d", rollbackCalls)
	}
	if retryCalls != 0 {
		t.Fatalf("expected 0 retries after rollback failure, got %d", retryCalls)
	}
}

func TestClassifyErrorCode(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{"transaction conflict", errors.New("Transaction conflict on commit"), "40001"},
		{"query cancelled", errors.New("context canceled"), "57014"},
		{"generic error", errors.New("syntax error"), "42000"},
		{"SSL closed is not conflict", errors.New("SSL connection has been closed unexpectedly"), "42000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyErrorCode(tt.err); got != tt.expected {
				t.Errorf("classifyErrorCode(%v) = %q, want %q", tt.err, got, tt.expected)
			}
		})
	}
}

func TestIsAlterTableNotTableErrorDoesNotTreatMissingObjectSuggestionAsWrongType(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "qualified missing table suggestion is not treated as wrong object type",
			err: errors.New(
				"Catalog Error: Table with name stg_customers__dbt_tmp does not exist!\nDid you mean \"stg_customers\"?",
			),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isAlterTableNotTableError(tt.err); got != tt.want {
				t.Fatalf("isAlterTableNotTableError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestIsDropTableOnViewError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "drop table on view error",
			err:  errors.New("Catalog Error: Existing object users is of type View, trying to drop type Table"),
			want: true,
		},
		{
			name: "unrelated error",
			err:  errors.New("Catalog Error: Table with name users does not exist!"),
			want: false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isDropTableOnViewError(tt.err); got != tt.want {
				t.Fatalf("isDropTableOnViewError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

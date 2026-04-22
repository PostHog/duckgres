package duckdbservice

import (
	"errors"
	"strings"
	"sync/atomic"
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
		return
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
		return
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
		return
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

func TestSQLTxActiveTracking(t *testing.T) {
	var flag atomic.Bool

	trackSQLTransactionState("BEGIN", nil, &flag)
	if !flag.Load() {
		t.Fatal("sqlTxActive should be true after BEGIN")
	}

	// Inside a transaction, inTransaction should be true.
	inTransaction := flag.Load()
	if !inTransaction {
		t.Fatal("should be in transaction after BEGIN")
	}

	trackSQLTransactionState("COMMIT", nil, &flag)
	if flag.Load() {
		t.Fatal("sqlTxActive should be false after COMMIT")
	}
}

func TestSQLTxActiveTrackingDoesNotStartOnFailedBegin(t *testing.T) {
	var flag atomic.Bool

	trackSQLTransactionState("BEGIN", errors.New("boom"), &flag)
	if flag.Load() {
		t.Fatal("sqlTxActive should remain false after failed BEGIN")
	}
}

func TestSQLTxActiveSkipsRetry(t *testing.T) {
	// Verify that when sqlTxActive is true, the retry logic (simulated here)
	// does not retry — the transient error is returned directly.
	var sqlTxActive atomic.Bool
	sqlTxActive.Store(true)

	calls := 0
	transientErr := errors.New("SSL connection has been closed unexpectedly")

	// Simulate the DoPutCommandStatementUpdate logic: when in a transaction,
	// execute directly without retryOnTransient.
	inTransaction := sqlTxActive.Load()
	isTxControl := isTransactionControlStmt("COPY t TO 's3://bucket/file.parquet'")

	var err error
	execFn := func() (string, error) {
		calls++
		return "", transientErr
	}

	if inTransaction || isTxControl {
		_, err = execFn()
	} else {
		_, err = retryOnTransient(execFn)
	}

	if err == nil {
		t.Fatal("expected error to be returned")
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 call (no retry inside transaction), got %d", calls)
	}
	if !strings.Contains(err.Error(), "SSL connection") {
		t.Fatalf("expected original SSL error, got: %v", err)
	}
}

func TestSQLTxActiveAllowsRetryOutsideTransaction(t *testing.T) {
	// When sqlTxActive is false and not in a Flight SQL txn, retries should work.
	var sqlTxActive atomic.Bool
	sqlTxActive.Store(false)

	calls := 0

	inTransaction := sqlTxActive.Load()
	isTxControl := isTransactionControlStmt("COPY t TO 's3://bucket/file.parquet'")

	var result string
	var err error
	execFn := func() (string, error) {
		calls++
		if calls == 1 {
			return "", errors.New("SSL connection has been closed unexpectedly")
		}
		return "ok", nil
	}

	if inTransaction || isTxControl {
		result, err = execFn()
	} else {
		result, err = retryOnTransient(execFn)
	}

	if err != nil {
		t.Fatalf("expected success after retry, got error: %v", err)
	}
	if result != "ok" {
		t.Fatalf("expected 'ok', got %q", result)
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls (1 failure + 1 retry), got %d", calls)
	}
}

func TestStartTransactionTracking(t *testing.T) {
	var flag atomic.Bool

	trackSQLTransactionState("START TRANSACTION", nil, &flag)
	if !flag.Load() {
		t.Fatal("sqlTxActive should be true after START TRANSACTION")
	}

	trackSQLTransactionState("ROLLBACK", nil, &flag)
	if flag.Load() {
		t.Fatal("sqlTxActive should be false after ROLLBACK")
	}
}

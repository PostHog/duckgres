package server

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
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

		{"catalog missing table", errors.New("Catalog Error: Table with name users does not exist!"), "42P01"},
		{"catalog missing table with suggestion", errors.New("Catalog Error: Table with name stg_customers__dbt_tmp does not exist!\nDid you mean \"stg_customers\"?"), "42P01"},
		{"catalog missing view", errors.New("Catalog Error: View with name v does not exist!"), "42P01"},
		{"catalog missing schema", errors.New("Catalog Error: Schema with name \"missing\" does not exist!"), "3F000"},
		{"catalog schema-qualified table, missing schema", errors.New("Catalog Error: Table with name \"missing_schema.t\" does not exist because schema \"missing_schema\" does not exist."), "3F000"},
		{"catalog missing function", errors.New("Catalog Error: Scalar Function with name no_such_func does not exist!"), "42883"},
		{"catalog missing type", errors.New("Catalog Error: Type with name mytype does not exist!"), "42704"},
		{"catalog table already exists", errors.New("Catalog Error: Table with name \"t\" already exists!"), "42P07"},
		{"catalog schema already exists", errors.New("Catalog Error: Schema with name \"s\" already exists!"), "42P06"},
		{"catalog function already exists", errors.New("Catalog Error: Function with name \"f\" already exists!"), "42723"},

		{"binder missing column", errors.New("Binder Error: Referenced column \"missing_col\" not found in FROM clause!"), "42703"},
		{"binder ambiguous column", errors.New("Binder Error: Ambiguous reference to column \"id\""), "42702"},
		{"binder missing table alias", errors.New("Binder Error: Referenced table \"t\" not found!\nCandidate tables: \"users\""), "42P01"},
		{"binder function overload", errors.New("Binder Error: No function matches the given name and argument types 'foo(INTEGER)'. You might need to add explicit type casts."), "42883"},
		{"binder other", errors.New("Binder Error: cannot use alter table on a view because this object is not a table; use ALTER VIEW instead"), "42601"},

		{"parser syntax", errors.New("Parser Error: syntax error at or near \"FORM\""), "42601"},
		{"conversion error", errors.New("Conversion Error: Could not convert string 'abc' to INT32"), "22P02"},
		{"conversion out of range cast", errors.New("Conversion Error: Type INT32 with value 1000 can't be cast because the value is out of range for the destination type INT8"), "22003"},
		{"conversion overflow", errors.New("Conversion Error: Overflow in cast from DOUBLE to INT32"), "22003"},
		{"out of range", errors.New("Out of Range Error: Overflow in multiplication of INT32"), "22003"},

		{"constraint unique", errors.New("Constraint Error: Duplicate key \"id: 1\" violates primary key constraint"), "23505"},
		{"constraint not null", errors.New("Constraint Error: NOT NULL constraint failed: t.col"), "23502"},
		{"constraint foreign key", errors.New("Constraint Error: Violates foreign key constraint because key is still referenced"), "23503"},
		{"constraint check", errors.New("Constraint Error: CHECK constraint failed: positive"), "23514"},
		{"constraint generic", errors.New("Constraint Error: some other constraint failure"), "23000"},

		{"permission denied", errors.New("Permission Error: not allowed to write here"), "42501"},
		{"transaction invalid", errors.New("Transaction Error: cannot begin within an existing transaction"), "25000"},
		{"transaction context nested begin", errors.New("TransactionContext Error: cannot start a transaction within a transaction"), "25000"},
		{"dependency error", errors.New("Dependency Error: Cannot drop entry because there are other entries that depend on it"), "2BP01"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyErrorCode(tt.err); got != tt.expected {
				t.Errorf("classifyErrorCode(%v) = %q, want %q", tt.err, got, tt.expected)
			}
		})
	}
}

// TestClassifyErrorCodeAgainstRealDuckDB drives queries that reliably
// produce each error prefix the classifier branches on, against a real
// in-memory DuckDB, and asserts the SQLSTATE we map to.
func TestClassifyErrorCodeAgainstRealDuckDB(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)`); err != nil {
		t.Fatalf("setup create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (1, 1)`); err != nil {
		t.Fatalf("setup insert: %v", err)
	}

	cases := []struct {
		name     string
		query    string
		wantCode string
	}{
		{"missing table", `SELECT * FROM no_such_table_xyz`, "42P01"},
		{"missing column", `SELECT no_such_col FROM t`, "42703"},
		{"parser syntax", `SELEC 1`, "42601"},
		{"bad cast", `SELECT CAST('abc' AS INTEGER)`, "22P02"},
		{"cast overflow", `SELECT CAST(1000 AS TINYINT)`, "22003"},
		{"unique violation", `INSERT INTO t VALUES (1, 2)`, "23505"},
		{"not null violation", `INSERT INTO t (id) VALUES (2)`, "23502"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec(tc.query)
			if err == nil {
				t.Fatalf("expected error from %q, got nil", tc.query)
			}
			if got := classifyErrorCode(err); got != tc.wantCode {
				t.Fatalf("classifyErrorCode for %q = %q, want %q (raw err: %v)", tc.query, got, tc.wantCode, err)
			}
		})
	}

	t.Run("nested begin", func(t *testing.T) {
		if _, err := db.Exec(`BEGIN`); err != nil {
			t.Fatalf("outer begin: %v", err)
		}
		defer func() { _, _ = db.Exec(`ROLLBACK`) }()

		_, err := db.Exec(`BEGIN`)
		if err == nil {
			t.Fatal("expected error on nested BEGIN, got nil")
		}
		if got := classifyErrorCode(err); got != "25000" {
			t.Fatalf("nested BEGIN SQLSTATE = %q, want %q (raw err: %v)", got, "25000", err)
		}
	})
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

package server

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
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
		return
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
		return
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

func TestClassifyErrorCode(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{"transaction conflict", errors.New("Transaction conflict on commit"), "40001"},
		{"query cancelled", errors.New("context canceled"), "57014"},
		// Unknown error class (no DuckDB prefix) maps to XX000 — internal /
		// infra rather than user-class — so isUserQueryError correctly
		// routes it to the alert-worthy log path. Adding this prefix to a
		// user class would hide real infra failures.
		{"generic error with no DuckDB prefix", errors.New("syntax error"), "XX000"},
		{"SSL closed is infra not user", errors.New("SSL connection has been closed unexpectedly"), "XX000"},

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

// TestIsUserQueryError pins the SQLSTATE-class-based discriminator
// that splits Query execution log lines between Info ("user wrote
// something that doesn't make sense") and Error ("the system itself
// failed"). The logger uses this to keep the Error level meaningful
// for alerting; a regression here would either drown alerts in user-
// typo noise or silently downgrade real infra failures.
func TestIsUserQueryError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool // true == user error (Info), false == infra error (Error)
	}{
		// Class 42 — by far the most common user errors (table/column not
		// found, syntax errors, access rule violations).
		{"missing table (42P01)", errors.New("Catalog Error: Table with name users does not exist!"), true},
		{"missing column (42703)", errors.New("Binder Error: Referenced column \"missing_col\" not found in FROM clause!"), true},
		{"syntax error (42601)", errors.New("Parser Error: syntax error at or near \"FORM\""), true},
		{"missing function (42883)", errors.New("Catalog Error: Scalar Function with name no_such_func does not exist!"), true},
		{"permission denied (42501)", errors.New("Permission Error: not allowed to write here"), true},
		{"duplicate table (42P07)", errors.New("Catalog Error: Table with name \"t\" already exists!"), true},

		// Other user classes — bad input, integrity, transaction misuse.
		{"data exception conversion (22P02)", errors.New("Conversion Error: Could not convert string 'abc' to INT32"), true},
		{"data exception out of range (22003)", errors.New("Out of Range Error: Overflow in multiplication of INT32"), true},
		{"unique violation (23505)", errors.New("Constraint Error: Duplicate key \"id: 1\" violates primary key constraint"), true},
		{"not null violation (23502)", errors.New("Constraint Error: NOT NULL constraint failed: t.col"), true},
		{"invalid transaction state (25000)", errors.New("Transaction Error: cannot begin within an existing transaction"), true},
		{"missing schema (3F000)", errors.New("Catalog Error: Schema with name \"missing\" does not exist!"), true},
		{"dependent objects (2BP01)", errors.New("Dependency Error: Cannot drop entry because there are other entries that depend on it"), true},

		// 57014 cancellation — class 57 is "operator intervention". Caller-
		// driven cancellation (Ctrl-C, deadline, client disconnect) is filtered
		// at the call site via clientConn.isCallerCancellation, so any
		// cancellation reaching isUserQueryError is infra (gRPC client closed
		// because the worker died, takeover, etc.) and must surface at Error.
		{"infra cancellation (57014)", errors.New("context canceled"), false},

		// Infra classes — must NOT be treated as user errors.
		{"unknown error → XX000", errors.New("something went wrong"), false},
		{"SSL closed → infra", errors.New("SSL connection has been closed unexpectedly"), false},
		{"nil error", nil, false},

		// 40001 retryable conflicts are a special case handled before the
		// SQLSTATE check fires (logQueryError emits its own Warn for them),
		// so they never reach this function in production. But verify the
		// classification is unambiguously infra-side here so a future
		// caller doesn't accidentally bucket retries as user errors.
		{"transaction conflict 40001 is not user", errors.New("Transaction conflict on commit"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isUserQueryError(tt.err); got != tt.want {
				t.Errorf("isUserQueryError(%v) = %v, want %v (SQLSTATE=%s)",
					tt.err, got, tt.want, classifyErrorCodeOrEmpty(tt.err))
			}
		})
	}
}

// classifyErrorCodeOrEmpty is a test helper to surface the computed
// SQLSTATE in failure messages without crashing on nil errors.
func classifyErrorCodeOrEmpty(err error) string {
	if err == nil {
		return "(nil)"
	}
	return classifyErrorCode(err)
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
				return
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
			return
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

// TestIsCallerCancellation pins down the discriminator that makes the
// difference between "user pressed Ctrl-C" and "the worker died and gRPC
// surfaced a Canceled status with a healthy request ctx." The latter must
// NOT be classified as caller-driven, otherwise the error is silently
// suppressed and infra failures don't show up in alerting.
func TestIsCallerCancellation(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{
			name: "infra-driven gRPC Canceled with healthy ctx is NOT caller cancel",
			ctx:  context.Background(),
			err:  errors.New("flight execute: rpc error: code = Canceled desc = context canceled"),
			want: false,
		},
		{
			name: "user Ctrl-C: ctx canceled and err propagates",
			ctx:  cancelledCtx,
			err:  errors.New("flight execute: rpc error: code = Canceled desc = context canceled"),
			want: true,
		},
		{
			name: "raw context.Canceled with cancelled ctx",
			ctx:  cancelledCtx,
			err:  context.Canceled,
			want: true,
		},
		{
			name: "non-cancel error with cancelled ctx is not classified as cancel",
			ctx:  cancelledCtx,
			err:  errors.New("Catalog Error: table does not exist"),
			want: false,
		},
		{
			name: "nil err",
			ctx:  cancelledCtx,
			err:  nil,
			want: false,
		},
		{
			name: "client connection closing (worker died)",
			ctx:  context.Background(),
			err:  errors.New("flight execute: rpc error: code = Canceled desc = grpc: the client connection is closing"),
			want: false, // pre-fix this would have looked like a user cancel via the substring match
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &clientConn{ctx: tt.ctx}
			if got := c.isCallerCancellation(tt.err); got != tt.want {
				t.Errorf("isCallerCancellation = %v, want %v (err=%v, ctx.Err=%v)",
					got, tt.want, tt.err, tt.ctx.Err())
			}
		})
	}
}

// TestLogQueryErrorRoutesInfraCancelToErrorLevel locks in the alerting
// invariant: a worker-death cancellation (gRPC Canceled bubbling up while
// c.ctx is still healthy) must produce slog.LevelError "Query execution
// errored.", not Info "Query execution failed." — because the latter gets
// filtered out of alerting and drove the dev/prod observability blind spot
// reported on 2026-05-04 (PR #516).
func TestLogQueryErrorRoutesInfraCancelToErrorLevel(t *testing.T) {
	prevLogger := slog.Default()
	defer slog.SetDefault(prevLogger)

	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))

	c := &clientConn{ctx: context.Background(), username: "test"}
	infraErr := errors.New("flight execute: rpc error: code = Canceled desc = context canceled")
	c.logQueryError("SELECT 1", infraErr)

	out := buf.String()
	if !strings.Contains(out, `level=ERROR`) {
		t.Errorf("expected ERROR level for infra cancel, got:\n%s", out)
	}
	if !strings.Contains(out, `Query execution errored.`) {
		t.Errorf("expected message 'Query execution errored.', got:\n%s", out)
	}
	if strings.Contains(out, `Query execution failed.`) {
		t.Errorf("infra cancel should NOT emit 'Query execution failed.':\n%s", out)
	}
}

package server

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
		want: true,
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

func TestIsRenameStmtAlreadyAppliedError(t *testing.T) {
	query := `ALTER VIEW ducklake.bill.stg_customers__dbt_tmp RENAME TO stg_customers`

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "rename already applied after alter table fallback",
			err: errors.New(
				"Catalog Error: View with name stg_customers__dbt_tmp does not exist!\nDid you mean \"stg_customers\"?",
			),
			want: true,
		},
		{
			name: "different suggestion is not treated as success",
			err: errors.New(
				"Catalog Error: View with name stg_customers__dbt_tmp does not exist!\nDid you mean \"other_view\"?",
			),
			want: false,
		},
		{
			name: "non rename error is false",
			err:  errors.New("Catalog Error: permission denied"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRenameStmtAlreadyAppliedError(query, tt.err); got != tt.want {
				t.Fatalf("isRenameStmtAlreadyAppliedError(%v) = %v, want %v", tt.err, got, tt.want)
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

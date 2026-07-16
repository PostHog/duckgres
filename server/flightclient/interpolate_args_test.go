package flightclient

import (
	"errors"
	"strings"
	"testing"

	"github.com/posthog/duckgres/server/sqlcore"
)

type testLiteralAppender struct {
	literals []string
	errAt    int
}

func (a testLiteralAppender) BindParameterCount() int { return len(a.literals) }

func (a testLiteralAppender) AppendBindParameterLiteral(dst *strings.Builder, index int) error {
	if index == a.errAt {
		return errors.New("malformed binary value")
	}
	dst.WriteString(a.literals[index])
	return nil
}

var _ sqlcore.SQLLiteralAppender = testLiteralAppender{}

type countingLiteralAppender struct {
	calls int
}

func (a *countingLiteralAppender) BindParameterCount() int { return 1 }

func (a *countingLiteralAppender) AppendBindParameterLiteral(*strings.Builder, int) error {
	a.calls++
	return errors.New("literal appender should not be called")
}

var _ sqlcore.SQLLiteralAppender = (*countingLiteralAppender)(nil)

func TestInterpolateArgs(t *testing.T) {
	tests := []struct {
		name  string
		query string
		args  []any
		want  string
	}{
		{
			name:  "no args returns query unchanged",
			query: "SELECT 1 WHERE x = ?",
			args:  nil,
			want:  "SELECT 1 WHERE x = ?",
		},
		{
			name:  "single ? positional",
			query: "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
			args:  []any{"fivetran_testing_schema_abc"},
			want:  "SELECT table_name FROM information_schema.tables WHERE table_schema = 'fivetran_testing_schema_abc'",
		},
		{
			name:  "multiple ? consumed in order",
			query: "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?",
			args:  []any{1, "two", true},
			want:  "SELECT * FROM t WHERE a = 1 AND b = 'two' AND c = TRUE",
		},
		{
			name:  "$N positional",
			query: "SELECT * FROM t WHERE a = $1 AND b = $2",
			args:  []any{int64(7), "x"},
			want:  "SELECT * FROM t WHERE a = 7 AND b = 'x'",
		},
		{
			name:  "$N multi-digit and reuse",
			query: "SELECT $10, $1",
			args:  []any{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
			want:  "SELECT 'j', 'a'",
		},
		{
			name:  "? inside single-quoted literal is not replaced",
			query: "SELECT 'is this ?' WHERE x = ?",
			args:  []any{5},
			want:  "SELECT 'is this ?' WHERE x = 5",
		},
		{
			name:  "? inside double-quoted identifier is not replaced",
			query: `SELECT "weird?col" FROM t WHERE x = ?`,
			args:  []any{5},
			want:  `SELECT "weird?col" FROM t WHERE x = 5`,
		},
		{
			name:  "? inside line comment is not replaced",
			query: "SELECT 1 -- a ? here\nWHERE x = ?",
			args:  []any{9},
			want:  "SELECT 1 -- a ? here\nWHERE x = 9",
		},
		{
			name:  "? inside block comment is not replaced",
			query: "SELECT 1 /* ? ignored */ WHERE x = ?",
			args:  []any{9},
			want:  "SELECT 1 /* ? ignored */ WHERE x = 9",
		},
		{
			name:  "string arg with single quote is escaped",
			query: "WHERE s = ?",
			args:  []any{"o'brien"},
			want:  "WHERE s = 'o''brien'",
		},
		{
			name:  "doubled-quote escape inside literal preserved",
			query: "SELECT 'a''?b' WHERE x = ?",
			args:  []any{1},
			want:  "SELECT 'a''?b' WHERE x = 1",
		},
		{
			name:  "unmatched ? (too few args) passes through",
			query: "WHERE a = ? AND b = ?",
			args:  []any{1},
			want:  "WHERE a = 1 AND b = ?",
		},
		{
			name:  "nil arg becomes NULL",
			query: "WHERE a = ?",
			args:  []any{nil},
			want:  "WHERE a = NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := interpolateArgs(tt.query, tt.args); got != tt.want {
				t.Fatalf("interpolateArgs(%q, %v)\n  = %q\n want %q", tt.query, tt.args, got, tt.want)
			}
		})
	}
}

func TestInterpolateBoundArgs(t *testing.T) {
	source := testLiteralAppender{literals: []string{"'o''brien'", "NULL", "42"}, errAt: -1}
	got, err := interpolateBoundArgs("SELECT ?, $3, $1, '?' /* $2 */", source)
	if err != nil {
		t.Fatalf("interpolateBoundArgs() error = %v", err)
	}
	const want = "SELECT 'o''brien', 42, 'o''brien', '?' /* $2 */"
	if got != want {
		t.Fatalf("interpolateBoundArgs() = %q, want %q", got, want)
	}

	_, err = interpolateBoundArgs("SELECT ?", testLiteralAppender{literals: []string{"ignored"}, errAt: 0})
	if err == nil || !strings.Contains(err.Error(), "malformed binary value") {
		t.Fatalf("interpolateBoundArgs() error = %v, want literal decode error", err)
	}
}

func TestBoundParamsPreflightSkipsInterpolation(t *testing.T) {
	t.Run("dead worker", func(t *testing.T) {
		exec := &FlightExecutor{}
		exec.MarkDead()
		params := &countingLiteralAppender{}

		if _, err := exec.QueryWithBoundParams("SELECT ?", params); !errors.Is(err, ErrWorkerDead) {
			t.Fatalf("QueryWithBoundParams() error = %v, want ErrWorkerDead", err)
		}
		if _, err := exec.ExecWithBoundParams("SELECT ?", params); !errors.Is(err, ErrWorkerDead) {
			t.Fatalf("ExecWithBoundParams() error = %v, want ErrWorkerDead", err)
		}
		if params.calls != 0 {
			t.Fatalf("literal appender calls = %d, want 0", params.calls)
		}
	})

	t.Run("empty query", func(t *testing.T) {
		exec := &FlightExecutor{}
		params := &countingLiteralAppender{}

		rows, err := exec.QueryWithBoundParams("-- ping", params)
		if err != nil {
			t.Fatalf("QueryWithBoundParams() error = %v", err)
		}
		if rows.Next() {
			t.Fatal("empty query returned a row")
		}
		if _, err := exec.ExecWithBoundParams("-- ping", params); err != nil {
			t.Fatalf("ExecWithBoundParams() error = %v", err)
		}
		if params.calls != 0 {
			t.Fatalf("literal appender calls = %d, want 0", params.calls)
		}
	})
}

package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"testing"
)

type describeRecordingExecutor struct {
	noopProfiling
	queries []string
	rowSet  RowSet
}

func (e *describeRecordingExecutor) QueryContext(context.Context, string, ...any) (RowSet, error) {
	return nil, errors.New("not implemented")
}

func (e *describeRecordingExecutor) ExecContext(context.Context, string, ...any) (ExecResult, error) {
	return nil, errors.New("not implemented")
}

func (e *describeRecordingExecutor) Query(query string, _ ...any) (RowSet, error) {
	e.queries = append(e.queries, query)
	return e.rowSet, nil
}

func (e *describeRecordingExecutor) Exec(string, ...any) (ExecResult, error) {
	return nil, errors.New("not implemented")
}

func (e *describeRecordingExecutor) ConnContext(context.Context) (RawConn, error) {
	return nil, errors.New("not implemented")
}

func (e *describeRecordingExecutor) PingContext(context.Context) error {
	return errors.New("not implemented")
}

func (e *describeRecordingExecutor) Close() error {
	return nil
}

type describeStaticRowSet struct {
	cols     []string
	colTypes []ColumnTyper
}

func (r *describeStaticRowSet) Columns() ([]string, error) { return r.cols, nil }
func (r *describeStaticRowSet) ColumnTypes() ([]ColumnTyper, error) {
	return r.colTypes, nil
}
func (r *describeStaticRowSet) Next() bool        { return false }
func (r *describeStaticRowSet) Scan(...any) error { return nil }
func (r *describeStaticRowSet) Close() error      { return nil }
func (r *describeStaticRowSet) Err() error        { return nil }

type describeColumnType string

func (t describeColumnType) DatabaseTypeName() string {
	return string(t)
}

func TestHandleDescribePortalUsesLimitZeroProbe(t *testing.T) {
	exec := &describeRecordingExecutor{
		rowSet: &describeStaticRowSet{
			cols:     []string{"count", "version"},
			colTypes: []ColumnTyper{describeColumnType("BIGINT"), describeColumnType("VARCHAR")},
		},
	}

	var out bytes.Buffer
	c := &clientConn{
		executor: exec,
		writer:   bufio.NewWriter(&out),
		portals: map[string]*portal{
			"p1": {
				stmt: &preparedStmt{
					query:          "SELECT count(1), version() FROM posthog.events",
					convertedQuery: "SELECT count(1), version() FROM posthog.events",
				},
			},
		},
		cursors: map[string]*cursorState{},
	}

	c.handleDescribe([]byte{'P', 'p', '1', 0})

	if len(exec.queries) != 1 {
		t.Fatalf("expected one describe probe query, got %d", len(exec.queries))
	}
	if got := exec.queries[0]; got != "SELECT count(1), version() FROM posthog.events LIMIT 0" {
		t.Fatalf("unexpected describe probe query: %q", got)
	}
}

func TestHandleDescribeExplainDoesNotExecute(t *testing.T) {
	// Describing an EXPLAIN [ANALYZE] must NOT execute a probe query — for
	// EXPLAIN ANALYZE of a write that would mutate, then Execute mutates again.
	for _, tc := range []struct {
		name    string
		query   string
		wantCol string
	}{
		{"statement explain", "EXPLAIN SELECT 1", "physical_plan"},
		{"portal explain analyze", "EXPLAIN ANALYZE INSERT INTO t VALUES (1)", "analyzed_plan"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := &describeRecordingExecutor{}
			var out bytes.Buffer
			c := &clientConn{
				executor: exec,
				writer:   bufio.NewWriter(&out),
				stmts: map[string]*preparedStmt{
					"s1": {query: tc.query, convertedQuery: tc.query},
				},
				portals: map[string]*portal{
					"p1": {stmt: &preparedStmt{query: tc.query, convertedQuery: tc.query}},
				},
				cursors: map[string]*cursorState{},
			}

			c.handleDescribe([]byte{'S', 's', '1', 0})
			c.handleDescribe([]byte{'P', 'p', '1', 0})

			if len(exec.queries) != 0 {
				t.Fatalf("EXPLAIN describe executed probe queries: %v", exec.queries)
			}
		})
	}
}

func TestHandleDescribePortalPreservesExistingLimit(t *testing.T) {
	exec := &describeRecordingExecutor{
		rowSet: &describeStaticRowSet{
			cols:     []string{"version"},
			colTypes: []ColumnTyper{describeColumnType("VARCHAR")},
		},
	}

	var out bytes.Buffer
	c := &clientConn{
		executor: exec,
		writer:   bufio.NewWriter(&out),
		portals: map[string]*portal{
			"p1": {
				stmt: &preparedStmt{
					query:          "SELECT version() LIMIT 1",
					convertedQuery: "SELECT version() LIMIT 1",
				},
			},
		},
		cursors: map[string]*cursorState{},
	}

	c.handleDescribe([]byte{'P', 'p', '1', 0})

	if len(exec.queries) != 1 {
		t.Fatalf("expected one describe probe query, got %d", len(exec.queries))
	}
	if got := exec.queries[0]; got != "SELECT version() LIMIT 1" {
		t.Fatalf("unexpected describe probe query: %q", got)
	}
}

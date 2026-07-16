package sqlcore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

type resultColumnCountTestExecResult struct{}

func (resultColumnCountTestExecResult) RowsAffected() (int64, error) { return 0, nil }

// resultColumnCountTestRowSet intentionally only accepts *any scan targets,
// matching the Flight row-set contract. The helper must therefore not depend
// on database/sql's numeric destination conversion.
type resultColumnCountTestRowSet struct {
	value   any
	nexted  bool
	closed  bool
	scanErr error
}

func (*resultColumnCountTestRowSet) Columns() ([]string, error)          { return []string{"column_count"}, nil }
func (*resultColumnCountTestRowSet) ColumnTypes() ([]ColumnTyper, error) { return nil, nil }
func (r *resultColumnCountTestRowSet) Next() bool {
	if r.nexted {
		return false
	}
	r.nexted = true
	return true
}
func (r *resultColumnCountTestRowSet) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if len(dest) != 1 {
		return fmt.Errorf("scan destinations = %d, want 1", len(dest))
	}
	value, ok := dest[0].(*any)
	if !ok {
		return fmt.Errorf("scan destination = %T, want *any", dest[0])
	}
	*value = r.value
	return nil
}
func (r *resultColumnCountTestRowSet) Close() error { r.closed = true; return nil }
func (*resultColumnCountTestRowSet) Err() error     { return nil }

type resultColumnCountTestExecutor struct {
	execQueries   []string
	queryQuery    string
	queryArgs     []any
	queryErr      error
	deallocateErr error
	rows          *resultColumnCountTestRowSet
}

func (e *resultColumnCountTestExecutor) ExecContext(_ context.Context, query string, _ ...any) (ExecResult, error) {
	e.execQueries = append(e.execQueries, query)
	if strings.HasPrefix(query, "DEALLOCATE ") {
		return resultColumnCountTestExecResult{}, e.deallocateErr
	}
	return resultColumnCountTestExecResult{}, nil
}

func (e *resultColumnCountTestExecutor) QueryContext(_ context.Context, query string, args ...any) (RowSet, error) {
	e.queryQuery = query
	e.queryArgs = append([]any(nil), args...)
	if e.queryErr != nil {
		return nil, e.queryErr
	}
	return e.rows, nil
}

func preparedStatementNameFromSQL(t testing.TB, query string) string {
	t.Helper()
	if !strings.HasPrefix(query, "PREPARE ") {
		t.Fatalf("prepare SQL = %q", query)
	}
	rest := strings.TrimPrefix(query, "PREPARE ")
	idx := strings.Index(rest, " AS ")
	if idx < 0 {
		t.Fatalf("prepare SQL lacks AS: %q", query)
	}
	return rest[:idx]
}

func TestPreparedStatementResultColumnCountUsesMetadataAndDeallocates(t *testing.T) {
	rows := &resultColumnCountTestRowSet{value: int64(2)}
	executor := &resultColumnCountTestExecutor{rows: rows}

	count, err := PreparedStatementResultColumnCount(context.Background(), executor, "  INSERT INTO target VALUES (?) RETURNING *;  ")
	if err != nil {
		t.Fatalf("PreparedStatementResultColumnCount: %v", err)
	}
	if count != 2 {
		t.Fatalf("column count = %d, want 2", count)
	}
	if !rows.closed {
		t.Fatal("metadata rows were not closed")
	}
	if len(executor.execQueries) != 2 {
		t.Fatalf("exec queries = %v, want PREPARE and DEALLOCATE", executor.execQueries)
	}
	preparedName := preparedStatementNameFromSQL(t, executor.execQueries[0])
	if !strings.HasPrefix(preparedName, `"__duckgres_result_columns_`) || !strings.HasSuffix(preparedName, `"`) {
		t.Fatalf("prepared statement name = %q, want quoted internal unique name", preparedName)
	}
	if strings.Contains(executor.execQueries[0], "RETURNING *;") {
		t.Fatalf("prepare SQL retained trailing semicolon: %q", executor.execQueries[0])
	}
	if got, want := executor.execQueries[1], "DEALLOCATE "+preparedName; got != want {
		t.Fatalf("deallocate SQL = %q, want %q", got, want)
	}
	if !strings.Contains(executor.queryQuery, "duckdb_prepared_statements()") || !strings.Contains(executor.queryQuery, "array_length(result_types)") {
		t.Fatalf("metadata query = %q, want prepared-statement result-type count", executor.queryQuery)
	}
	if len(executor.queryArgs) != 1 || executor.queryArgs[0] != strings.Trim(preparedName, `"`) {
		t.Fatalf("metadata args = %#v, want generated statement name", executor.queryArgs)
	}
}

func TestPreparedStatementResultColumnCountDeallocatesAfterMetadataFailure(t *testing.T) {
	metadataErr := errors.New("metadata lookup failed")
	executor := &resultColumnCountTestExecutor{queryErr: metadataErr}

	_, err := PreparedStatementResultColumnCount(context.Background(), executor, "SELECT 1")
	if !errors.Is(err, metadataErr) {
		t.Fatalf("error = %v, want metadata error", err)
	}
	if len(executor.execQueries) != 2 {
		t.Fatalf("exec queries = %v, want PREPARE and guaranteed DEALLOCATE", executor.execQueries)
	}
	preparedName := preparedStatementNameFromSQL(t, executor.execQueries[0])
	if got, want := executor.execQueries[1], "DEALLOCATE "+preparedName; got != want {
		t.Fatalf("deallocate SQL = %q, want %q", got, want)
	}
}

func TestPreparedStatementResultColumnCountRejectsEmptyQuery(t *testing.T) {
	executor := &resultColumnCountTestExecutor{}
	_, err := PreparedStatementResultColumnCount(context.Background(), executor, " ; ")
	if err == nil {
		t.Fatal("empty query did not fail")
	}
	if len(executor.execQueries) != 0 {
		t.Fatalf("empty query ran SQL: %v", executor.execQueries)
	}
}

package server

import (
	"context"
	"errors"
	"testing"
)

type attachedCatalogProbeExecutor struct {
	noopProfiling
	lastQuery string
	lastArgs  []any
}

func (e *attachedCatalogProbeExecutor) QueryContext(_ context.Context, query string, args ...any) (RowSet, error) {
	e.lastQuery = query
	e.lastArgs = append([]any(nil), args...)
	return &attachedCatalogProbeRowSet{count: 1, remaining: 1}, nil
}

func (e *attachedCatalogProbeExecutor) ExecContext(context.Context, string, ...any) (ExecResult, error) {
	return nil, errors.New("not implemented")
}

func (e *attachedCatalogProbeExecutor) Query(string, ...any) (RowSet, error) {
	return nil, errors.New("not implemented")
}

func (e *attachedCatalogProbeExecutor) Exec(string, ...any) (ExecResult, error) {
	return nil, errors.New("not implemented")
}

func (e *attachedCatalogProbeExecutor) ConnContext(context.Context) (RawConn, error) {
	return nil, errors.New("not implemented")
}

func (e *attachedCatalogProbeExecutor) PingContext(context.Context) error {
	return errors.New("not implemented")
}

func (e *attachedCatalogProbeExecutor) Close() error {
	return nil
}

type attachedCatalogProbeRowSet struct {
	count     int
	remaining int
}

func (r *attachedCatalogProbeRowSet) Columns() ([]string, error) {
	return []string{"count"}, nil
}

func (r *attachedCatalogProbeRowSet) ColumnTypes() ([]ColumnTyper, error) {
	return []ColumnTyper{describeColumnType("INTEGER")}, nil
}

func (r *attachedCatalogProbeRowSet) Next() bool {
	if r.remaining == 0 {
		return false
	}
	r.remaining--
	return true
}

func (r *attachedCatalogProbeRowSet) Scan(dest ...any) error {
	if len(dest) != 1 {
		return errors.New("expected one scan destination")
	}
	ptr, ok := dest[0].(*interface{})
	if !ok {
		return errors.New("expected *interface{} scan destination")
	}
	*ptr = int64(r.count)
	return nil
}

func (r *attachedCatalogProbeRowSet) Close() error {
	return nil
}

func (r *attachedCatalogProbeRowSet) Err() error {
	return nil
}

func TestHasAttachedCatalogUsesEmbeddedCatalogLiteral(t *testing.T) {
	executor := &attachedCatalogProbeExecutor{}

	attached, err := hasAttachedCatalog(context.Background(), executor, "ducklake")
	if err != nil {
		t.Fatalf("hasAttachedCatalog returned error: %v", err)
	}
	if !attached {
		t.Fatal("expected ducklake catalog to be reported as attached")
	}
	if got, want := executor.lastQuery, "SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'ducklake'"; got != want {
		t.Fatalf("probe query = %q, want %q", got, want)
	}
	if len(executor.lastArgs) != 0 {
		t.Fatalf("probe args = %#v, want none", executor.lastArgs)
	}
}

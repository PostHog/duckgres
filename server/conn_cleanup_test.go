package server

import (
	"context"
	"errors"
	"testing"
)

type cleanupRecordingExecutor struct {
	execQueries []string
}

func (e *cleanupRecordingExecutor) QueryContext(context.Context, string, ...any) (RowSet, error) {
	return nil, errors.New("not implemented")
}

func (e *cleanupRecordingExecutor) ExecContext(_ context.Context, query string, _ ...any) (ExecResult, error) {
	e.execQueries = append(e.execQueries, query)
	return &fakeExecResult{}, nil
}

func (e *cleanupRecordingExecutor) Query(string, ...any) (RowSet, error) {
	return nil, errors.New("not implemented")
}

func (e *cleanupRecordingExecutor) Exec(string, ...any) (ExecResult, error) {
	return nil, errors.New("not implemented")
}

func (e *cleanupRecordingExecutor) ConnContext(context.Context) (RawConn, error) {
	return nil, errors.New("not implemented")
}

func (e *cleanupRecordingExecutor) PingContext(context.Context) error {
	return errors.New("not implemented")
}

func (e *cleanupRecordingExecutor) Close() error {
	return nil
}

func TestSafeCleanupDBUsesValidDuckLakeHealthProbe(t *testing.T) {
	exec := &cleanupRecordingExecutor{}
	c := &clientConn{
		server: &Server{
			cfg: Config{
				DuckLake: DuckLakeConfig{
					MetadataStore: "postgres:host=127.0.0.1 dbname=ducklake",
				},
			},
		},
		executor: exec,
		txStatus: txStatusIdle,
	}

	c.safeCleanupDB()

	want := []string{
		"SELECT 1 FROM duckdb_tables() WHERE database_name = 'ducklake' LIMIT 1",
		"USE memory",
		"DETACH ducklake",
	}
	if len(exec.execQueries) != len(want) {
		t.Fatalf("expected %d cleanup statements, got %d: %#v", len(want), len(exec.execQueries), exec.execQueries)
	}
	for i, query := range want {
		if exec.execQueries[i] != query {
			t.Fatalf("cleanup query %d = %q, want %q", i, exec.execQueries[i], query)
		}
	}
}

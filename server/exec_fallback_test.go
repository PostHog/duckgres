package server

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestExecCompatibilityFallbackReturnsIcebergFallbackError(t *testing.T) {
	execErr := errors.New("Not implemented Error: DROP SCHEMA <schema_name> CASCADE is not supported for Iceberg schemas currently")
	c := &clientConn{executor: &failingFallbackExecutor{}}

	_, handled, err := c.execCompatibilityFallback(context.Background(), "DROP SCHEMA IF EXISTS stripe CASCADE", execErr, func(string) (ExecResult, error) {
		return nil, errors.New("unexpected fallback exec")
	})
	if !handled {
		t.Fatal("expected Iceberg fallback to handle unsupported DROP SCHEMA CASCADE error")
	}
	if err == nil || !strings.Contains(err.Error(), "iceberg DROP SCHEMA CASCADE fallback failed") {
		t.Fatalf("fallback error = %v, want contextual fallback failure", err)
	}
}

type failingFallbackExecutor struct {
	noopProfiling
}

func (e *failingFallbackExecutor) QueryContext(context.Context, string, ...any) (RowSet, error) {
	return nil, errors.New("settings query failed")
}

func (e *failingFallbackExecutor) ExecContext(context.Context, string, ...any) (ExecResult, error) {
	return nil, errors.New("unexpected exec")
}

func (e *failingFallbackExecutor) Query(string, ...any) (RowSet, error) {
	return nil, errors.New("not implemented")
}

func (e *failingFallbackExecutor) Exec(string, ...any) (ExecResult, error) {
	return nil, errors.New("not implemented")
}

func (e *failingFallbackExecutor) ConnContext(context.Context) (RawConn, error) {
	return nil, errors.New("not implemented")
}

func (e *failingFallbackExecutor) PingContext(context.Context) error {
	return errors.New("not implemented")
}

func (e *failingFallbackExecutor) Close() error {
	return nil
}

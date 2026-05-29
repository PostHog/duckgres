package server

import (
	"context"
	"fmt"

	"github.com/posthog/duckgres/transpiler"
)

func (c *clientConn) execCompatibilityFallback(ctx context.Context, query string, execErr error) (ExecResult, bool, error) {
	if isAlterTableNotTableError(execErr) {
		if alteredQuery, ok := transpiler.ConvertAlterTableToAlterView(query); ok {
			result, err := c.executor.ExecContext(ctx, alteredQuery)
			return result, true, err
		}
	}

	if isDropTableOnViewError(execErr) {
		if alteredQuery, ok := transpiler.ConvertDropTableToDropView(query); ok {
			result, err := c.executor.ExecContext(ctx, alteredQuery)
			return result, true, err
		}
	}

	if isIcebergDropSchemaCascadeUnsupported(execErr) {
		result, err := c.dropIcebergSchemaCascade(ctx, query)
		if err != nil {
			return nil, true, fmt.Errorf("iceberg DROP SCHEMA CASCADE fallback failed: %w", err)
		}
		return result, true, nil
	}

	return nil, false, nil
}

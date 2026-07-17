package server

import (
	"time"

	"github.com/posthog/duckgres/transpiler"
)

type execFallbackRunner func(string) (ExecResult, error)

// execCompatibilityFallback runs a compatibility rewrite after a client-derived
// physical statement has failed. beforeFallback is invoked only when a rewrite
// will actually run, allowing the caller to finish the failed original worker
// statement before this generated worker statement starts.
func (c *clientConn) execCompatibilityFallback(query string, execErr error, exec execFallbackRunner, beforeFallback func()) (ExecResult, bool, error) {
	var fallbackQuery, fallbackKind string
	if isAlterTableNotTableError(execErr) {
		if alteredQuery, ok := transpiler.ConvertAlterTableToAlterView(query); ok {
			fallbackQuery = alteredQuery
			fallbackKind = "alter_view"
		}
	}

	if fallbackQuery == "" && isDropTableOnViewError(execErr) {
		if alteredQuery, ok := transpiler.ConvertDropTableToDropView(query); ok {
			fallbackQuery = alteredQuery
			fallbackKind = "drop_view"
		}
	}

	if fallbackQuery == "" {
		return nil, false, nil
	}
	if beforeFallback != nil {
		beforeFallback()
	}

	statement := generatedWorkerStatement(
		workerOriginRewrite,
		workerOperationCompatibilityFallback,
		"fallback_kind", fallbackKind,
	)
	start := time.Now()
	c.logWorkerStatementStarted(statement)
	result, err := exec(fallbackQuery)
	var rows int64
	if result != nil {
		rows, _ = result.RowsAffected()
	}
	c.logWorkerStatementFinished(statement, start, rows, err)
	return result, true, err
}

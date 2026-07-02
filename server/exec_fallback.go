package server

import (
	"github.com/posthog/duckgres/transpiler"
)

type execFallbackRunner func(string) (ExecResult, error)

func (c *clientConn) execCompatibilityFallback(query string, execErr error, exec execFallbackRunner) (ExecResult, bool, error) {
	if isAlterTableNotTableError(execErr) {
		if alteredQuery, ok := transpiler.ConvertAlterTableToAlterView(query); ok {
			result, err := exec(alteredQuery)
			return result, true, err
		}
	}

	if isDropTableOnViewError(execErr) {
		if alteredQuery, ok := transpiler.ConvertDropTableToDropView(query); ok {
			result, err := exec(alteredQuery)
			return result, true, err
		}
	}

	return nil, false, nil
}

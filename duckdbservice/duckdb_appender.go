package duckdbservice

import (
	"database/sql/driver"
	"fmt"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/sqlcore"
)

// init wires the real duckdb-go Appender implementation into server's
// COPY codepath. Binaries that link duckdbservice (worker, all-in-one)
// get full Appender support; binaries that don't (a future control-plane-
// only binary) hit the unavailable fallback in server and naturally fall
// back to batched INSERT statements.
func init() {
	server.RegisterDuckDBAppender(appendDuckDBRows)
}

// appendDuckDBRows is the real DuckDB Appender implementation. Mirrors the
// logic that previously lived inline in (*clientConn).appendWithDuckDBAppender.
func appendDuckDBRows(rawConn sqlcore.RawConn, parts []string, rows [][]any) (int, error) {
	var rowCount int
	err := rawConn.Raw(func(driverConn any) error {
		dc, ok := driverConn.(driver.Conn)
		if !ok {
			return fmt.Errorf("underlying connection does not implement driver.Conn")
		}

		var appender *duckdb.Appender
		var appErr error
		switch len(parts) {
		case 1:
			appender, appErr = duckdb.NewAppenderFromConn(dc, "", parts[0])
		case 2:
			appender, appErr = duckdb.NewAppenderFromConn(dc, parts[0], parts[1])
		default:
			appender, appErr = duckdb.NewAppender(dc, parts[0], parts[1], parts[2])
		}
		if appErr != nil {
			return fmt.Errorf("failed to create Appender: %w", appErr)
		}

		for i, row := range rows {
			driverVals := make([]driver.Value, len(row))
			for j, v := range row {
				driverVals[j] = v
			}
			if appErr = appender.AppendRow(driverVals...); appErr != nil {
				_ = appender.Close()
				return fmt.Errorf("AppendRow failed at row %d: %w", i+1, appErr)
			}
		}

		if appErr = appender.Close(); appErr != nil {
			return fmt.Errorf("Appender.Close failed: %w", appErr)
		}

		rowCount = len(rows)
		return nil
	})

	return rowCount, err
}

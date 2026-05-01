package server

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/posthog/duckgres/server/sqlcore"
)

// DuckDBAppendFunc bulk-inserts rows into a DuckDB table using the duckdb-go
// Appender API. The implementation calls rawConn.Raw to extract the
// underlying driver.Conn and then drives duckdb.NewAppender* against it.
//
// The signature lives in the server package (rather than in duckdbservice)
// because the COPY codepath in clientConn dispatches through it; the
// duckdbservice package registers the actual implementation at init time
// via RegisterDuckDBAppender, which keeps server/conn.go itself free of any
// duckdb-go imports.
//
// parts is the result of splitQualifiedName(tableName) — a 1, 2, or 3
// element slice of catalog/schema/table parts (preserving the precise
// args the original switch passed to NewAppender* / NewAppenderFromConn).
type DuckDBAppendFunc func(rawConn sqlcore.RawConn, parts []string, rows [][]any) (int, error)

// duckdbAppender is loaded once via RegisterDuckDBAppender. Reads on the
// COPY hot path are lock-free.
var duckdbAppender atomic.Value // DuckDBAppendFunc

// RegisterDuckDBAppender wires a real DuckDB Appender implementation into
// the COPY codepath. duckdbservice's init() calls this; binaries that
// don't link duckdbservice get the unavailable fallback below.
func RegisterDuckDBAppender(f DuckDBAppendFunc) {
	if f == nil {
		return
	}
	duckdbAppender.Store(f)
}

// errDuckDBAppenderUnavailable is returned by appendWithDuckDBAppender when
// no implementation has been registered (typically a control-plane build
// that doesn't link duckdbservice). Callers fall back to batchInsertRows.
var errDuckDBAppenderUnavailable = errors.New("duckdb Appender support not linked in this build")

// appendWithDuckDBAppender dispatches to the registered DuckDB Appender, if
// any. Returns errDuckDBAppenderUnavailable when no implementation has been
// registered so callers can choose a fallback.
func (c *clientConn) appendWithDuckDBAppender(tableName string, rows [][]any) (int, error) {
	fn, _ := duckdbAppender.Load().(DuckDBAppendFunc)
	if fn == nil {
		return 0, errDuckDBAppenderUnavailable
	}

	parts := splitQualifiedName(tableName)

	ctx := context.Background()
	rawConn, err := c.executor.ConnContext(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get DB connection: %w", err)
	}
	defer rawConn.Close() //nolint:errcheck

	return fn(rawConn, parts, rows)
}

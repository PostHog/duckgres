// Package sqlcore holds the duckgres-internal SQL/result interfaces that
// span the wire-protocol/server layer and the Arrow Flight client. It also
// hosts a couple of small helpers (IsEmptyQuery, OTELGRPCClientHandler)
// shared between those layers.
//
// The package has no dependency on github.com/duckdb/duckdb-go, so any
// caller that wants to operate against duckgres without linking libduckdb
// (notably the Flight client and a future control-plane-only binary) can
// import this package without dragging the DuckDB driver in.
package sqlcore

import (
	"context"
	"io"
)

// ColumnTyper provides type name information for a database column.
// *sql.ColumnType satisfies this interface.
type ColumnTyper interface {
	DatabaseTypeName() string
}

// RowSet represents a set of rows from a query result.
type RowSet interface {
	Columns() ([]string, error)
	ColumnTypes() ([]ColumnTyper, error)
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}

// ExecResult represents the result of a non-query execution.
type ExecResult interface {
	RowsAffected() (int64, error)
}

// RawConn provides access to the underlying driver connection.
// *sql.Conn satisfies this interface.
type RawConn interface {
	Raw(func(any) error) error
	Close() error
}

// QueryExecutor abstracts database query execution, allowing both local
// (*sql.DB) and remote (Arrow Flight SQL) backends.
type QueryExecutor interface {
	QueryContext(ctx context.Context, query string, args ...any) (RowSet, error)
	ExecContext(ctx context.Context, query string, args ...any) (ExecResult, error)
	Query(query string, args ...any) (RowSet, error)
	Exec(query string, args ...any) (ExecResult, error)
	ConnContext(ctx context.Context) (RawConn, error)
	PingContext(ctx context.Context) error
	Close() error

	// LastProfilingOutput returns the JSON profiling output from the last
	// executed query, or "" if profiling is not enabled or not available
	// (e.g. Flight SQL mode where the query ran on a remote worker).
	LastProfilingOutput() string
}

// CopyFromStdinExecutor is an optional capability implemented by executors
// that need the COPY input bytes shipped to a different filesystem than the one
// the COPY FROM STDIN handler is running on. The remote (Flight) executor
// implements this to spool the bytes onto the worker pod, where the
// worker can then run COPY FROM <path>. Local executors do not need to
// implement it; the standard local-tempfile path works for them since
// the executor and the COPY FROM SQL run in the same process / host.
//
// copySQLTemplate must contain a path placeholder that the receiver
// substitutes with the destination spool path before executing. The
// placeholder string is defined alongside the implementation
// (flightclient.CopyFromStdinPathPlaceholder).
type CopyFromStdinExecutor interface {
	CopyFromStdin(ctx context.Context, copySQLTemplate string, data io.Reader) (rowCount int64, err error)
}

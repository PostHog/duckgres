package server

import (
	"context"
	"database/sql"
	"os"

	"github.com/posthog/duckgres/server/sqlcore"
)

// The SQL/result interfaces moved to server/sqlcore so the Flight client
// and other duckdb-free callers can implement them without importing
// server. The aliases below preserve the old server.X spellings for the
// dozens of references inside this package and elsewhere.
type (
	ColumnTyper   = sqlcore.ColumnTyper
	RowSet        = sqlcore.RowSet
	ExecResult    = sqlcore.ExecResult
	RawConn       = sqlcore.RawConn
	QueryExecutor = sqlcore.QueryExecutor
)

// LocalExecutor wraps *sql.DB to implement QueryExecutor for local DuckDB access.
type LocalExecutor struct {
	db *sql.DB
}

// NewLocalExecutor creates a new LocalExecutor wrapping the given *sql.DB.
func NewLocalExecutor(db *sql.DB) *LocalExecutor {
	return &LocalExecutor{db: db}
}

// DB returns the underlying *sql.DB (for credential refresh and other direct access).
func (e *LocalExecutor) DB() *sql.DB {
	return e.db
}

func (e *LocalExecutor) QueryContext(ctx context.Context, query string, args ...any) (RowSet, error) {
	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &LocalRowSet{rows: rows}, nil
}

func (e *LocalExecutor) ExecContext(ctx context.Context, query string, args ...any) (ExecResult, error) {
	return e.db.ExecContext(ctx, query, args...)
}

func (e *LocalExecutor) Query(query string, args ...any) (RowSet, error) {
	rows, err := e.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &LocalRowSet{rows: rows}, nil
}

func (e *LocalExecutor) Exec(query string, args ...any) (ExecResult, error) {
	return e.db.Exec(query, args...)
}

func (e *LocalExecutor) ConnContext(ctx context.Context) (RawConn, error) {
	return e.db.Conn(ctx)
}

func (e *LocalExecutor) PingContext(ctx context.Context) error {
	return e.db.PingContext(ctx)
}

func (e *LocalExecutor) Close() error {
	return e.db.Close()
}

func (e *LocalExecutor) LastProfilingOutput() string {
	data, err := os.ReadFile("/tmp/duckgres-profiling.json")
	if err != nil {
		return ""
	}
	return string(data)
}

// PinnedExecutor wraps a pinned *sql.Conn from a shared *sql.DB pool
// to implement QueryExecutor for file-persistence mode.
type PinnedExecutor struct {
	conn *sql.Conn
	db   *sql.DB
}

func NewPinnedExecutor(conn *sql.Conn, db *sql.DB) *PinnedExecutor {
	return &PinnedExecutor{conn: conn, db: db}
}

// DB returns the underlying *sql.DB (for credential refresh and other direct access).
func (e *PinnedExecutor) DB() *sql.DB {
	return e.db
}

func (e *PinnedExecutor) QueryContext(ctx context.Context, query string, args ...any) (RowSet, error) {
	rows, err := e.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &LocalRowSet{rows: rows}, nil
}

func (e *PinnedExecutor) ExecContext(ctx context.Context, query string, args ...any) (ExecResult, error) {
	return e.conn.ExecContext(ctx, query, args...)
}

func (e *PinnedExecutor) Query(query string, args ...any) (RowSet, error) {
	rows, err := e.conn.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	return &LocalRowSet{rows: rows}, nil
}

func (e *PinnedExecutor) Exec(query string, args ...any) (ExecResult, error) {
	return e.conn.ExecContext(context.Background(), query, args...)
}

func (e *PinnedExecutor) ConnContext(ctx context.Context) (RawConn, error) {
	return e.db.Conn(ctx)
}

func (e *PinnedExecutor) PingContext(ctx context.Context) error {
	return e.conn.PingContext(ctx)
}

// Close returns the pinned connection to the pool; it does not close the underlying DB.
func (e *PinnedExecutor) Close() error {
	return e.conn.Close()
}

func (e *PinnedExecutor) LastProfilingOutput() string {
	data, err := os.ReadFile("/tmp/duckgres-profiling.json")
	if err != nil {
		return ""
	}
	return string(data)
}

// LocalRowSet wraps *sql.Rows to implement RowSet.
type LocalRowSet struct {
	rows *sql.Rows
}

func (r *LocalRowSet) Columns() ([]string, error) {
	return r.rows.Columns()
}

func (r *LocalRowSet) ColumnTypes() ([]ColumnTyper, error) {
	colTypes, err := r.rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	result := make([]ColumnTyper, len(colTypes))
	for i, ct := range colTypes {
		result[i] = ct
	}
	return result, nil
}

func (r *LocalRowSet) Next() bool {
	return r.rows.Next()
}

func (r *LocalRowSet) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r *LocalRowSet) Close() error {
	return r.rows.Close()
}

func (r *LocalRowSet) Err() error {
	return r.rows.Err()
}

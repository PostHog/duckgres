package server

import (
	"context"
	"database/sql"
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

// QueryExecutor abstracts database query execution, allowing both local (*sql.DB)
// and remote (Arrow Flight SQL) backends.
type QueryExecutor interface {
	QueryContext(ctx context.Context, query string, args ...any) (RowSet, error)
	ExecContext(ctx context.Context, query string, args ...any) (ExecResult, error)
	Query(query string, args ...any) (RowSet, error)
	Exec(query string, args ...any) (ExecResult, error)
	ConnContext(ctx context.Context) (RawConn, error)
	PingContext(ctx context.Context) error
	Close() error
}

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

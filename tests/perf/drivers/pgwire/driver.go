package pgwire

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/posthog/duckgres/tests/perf/core"
)

type Executor interface {
	Execute(ctx context.Context, query string, args []any) (int64, error)
	Close() error
}

type Driver struct {
	exec     Executor
	protocol core.Protocol
	prepare  func(context.Context) error
}

func NewWithExecutor(exec Executor) *Driver {
	return &Driver{exec: exec, protocol: core.ProtocolPGWire}
}

func NewWithDB(db *sql.DB) *Driver {
	return NewWithExecutor(&sqlExecutor{db: db})
}

// NewWithDBAndProtocol takes ownership of db on success. Explicit cache variants
// pin their connection and apply settings lazily, before the first warmup query.
// Creating a later variant must not change the worker used by an earlier phase.
func NewWithDBAndProtocol(db *sql.DB, protocol core.Protocol) (*Driver, error) {
	if protocol == core.ProtocolPGWire {
		return NewWithDB(db), nil
	}
	if protocol != core.ProtocolPGWireUncached && protocol != core.ProtocolPGWireCached {
		return nil, fmt.Errorf("unsupported pgwire protocol %q", protocol)
	}
	externalCache := protocol == core.ProtocolPGWireCached
	exec := &sqlExecutor{
		db: db,
		settings: []string{
			fmt.Sprintf("SET GLOBAL enable_external_file_cache = %t", externalCache),
			"SET GLOBAL parquet_metadata_cache = false",
			"SET GLOBAL enable_http_metadata_cache = false",
		},
	}
	return &Driver{exec: exec, protocol: protocol, prepare: exec.prepare}, nil
}

func NewFromDSN(dsn string) (*Driver, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open pgwire connection: %w", err)
	}
	return NewWithDB(db), nil
}

func (d *Driver) Protocol() core.Protocol {
	return d.protocol
}

func (d *Driver) Execute(ctx context.Context, query core.Query, args []any) (core.ExecutionResult, error) {
	if d.exec == nil {
		return core.ExecutionResult{}, fmt.Errorf("pgwire driver has no executor")
	}
	sqlText := query.SQLForProtocol(core.ProtocolPGWire)
	if sqlText == "" {
		return core.ExecutionResult{}, fmt.Errorf("query %s missing pgwire_sql", query.QueryID)
	}
	if d.prepare != nil {
		if err := d.prepare(ctx); err != nil {
			return core.ExecutionResult{}, err
		}
	}
	started := time.Now()
	rows, err := d.exec.Execute(ctx, sqlText, args)
	return core.ExecutionResult{
		Rows:     rows,
		Duration: time.Since(started),
	}, err
}

func (d *Driver) Close() error {
	if d.exec == nil {
		return nil
	}
	return d.exec.Close()
}

type sqlExecutor struct {
	db         *sql.DB
	conn       *sql.Conn
	settings   []string
	prepared   bool
	prepareErr error
}

func (e *sqlExecutor) prepare(ctx context.Context) error {
	if e.prepared {
		return e.prepareErr
	}
	e.prepared = true
	conn, err := e.db.Conn(ctx)
	if err != nil {
		e.prepareErr = fmt.Errorf("pin pgwire cache-variant connection: %w", err)
		return e.prepareErr
	}
	e.conn = conn
	for _, setting := range e.settings {
		if _, err := conn.ExecContext(ctx, setting); err != nil {
			e.prepareErr = fmt.Errorf("configure pgwire cache variant (%s): %w", setting, err)
			return e.prepareErr
		}
	}
	return nil
}

func (e *sqlExecutor) Execute(ctx context.Context, query string, args []any) (int64, error) {
	queryContext := e.db.QueryContext
	execContext := e.db.ExecContext
	if e.conn != nil {
		queryContext = e.conn.QueryContext
		execContext = e.conn.ExecContext
	}
	rows, err := queryContext(ctx, query, args...)
	if err == nil {
		defer func() {
			_ = rows.Close()
		}()
		var count int64
		cols, colErr := rows.Columns()
		if colErr != nil {
			return 0, colErr
		}
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		for rows.Next() {
			if scanErr := rows.Scan(ptrs...); scanErr != nil {
				return 0, scanErr
			}
			count++
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return 0, rowsErr
		}
		return count, nil
	}

	res, execErr := execContext(ctx, query, args...)
	if execErr != nil {
		return 0, execErr
	}
	affected, affErr := res.RowsAffected()
	if affErr != nil {
		return 0, nil
	}
	return affected, nil
}

func (e *sqlExecutor) Close() error {
	var connErr error
	if e.conn != nil {
		connErr = e.conn.Close()
		if errors.Is(connErr, sql.ErrConnDone) {
			connErr = nil
		}
	}
	return errors.Join(connErr, e.db.Close())
}

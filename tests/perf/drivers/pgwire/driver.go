package pgwire

import (
	"context"
	"database/sql"
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
	exec Executor
}

func NewWithExecutor(exec Executor) *Driver {
	return &Driver{exec: exec}
}

func NewFromDSN(dsn string) (*Driver, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open pgwire connection: %w", err)
	}
	return &Driver{exec: &sqlExecutor{db: db}}, nil
}

func (d *Driver) Protocol() core.Protocol {
	return core.ProtocolPGWire
}

func (d *Driver) Execute(ctx context.Context, query core.Query, args []any) (core.ExecutionResult, error) {
	if d.exec == nil {
		return core.ExecutionResult{}, fmt.Errorf("pgwire driver has no executor")
	}
	sqlText := query.PGWireSQL
	if sqlText == "" {
		return core.ExecutionResult{}, fmt.Errorf("query %s missing pgwire_sql", query.QueryID)
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
	db *sql.DB
}

func (e *sqlExecutor) Execute(ctx context.Context, query string, args []any) (int64, error) {
	rows, err := e.db.QueryContext(ctx, query, args...)
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

	res, execErr := e.db.ExecContext(ctx, query, args...)
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
	return e.db.Close()
}

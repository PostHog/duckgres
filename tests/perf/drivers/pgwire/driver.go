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

func NewWithDB(db *sql.DB) *Driver {
	return NewWithExecutor(&sqlExecutor{db: db})
}

func NewFromDSN(dsn string) (*Driver, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open pgwire connection: %w", err)
	}
	return NewWithDB(db), nil
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

// scalarExecutor is OPTIONALLY implemented by an Executor that can return a
// single string value. Keeping it separate from Executor means existing fakes
// (and the perf harness's own) need no change: a driver whose executor does not
// implement it simply reports the engine without a version.
type scalarExecutor interface {
	Scalar(ctx context.Context, query string) (string, error)
}

// Environment reports the non-secret comparison metadata recorded in the perf
// artifact for this protocol.
func (d *Driver) Environment(ctx context.Context) (core.ProtocolEnvironment, error) {
	env := core.ProtocolEnvironment{Protocol: core.ProtocolPGWire, Engine: "duckgres"}
	scalar, ok := d.exec.(scalarExecutor)
	if !ok {
		return env, nil
	}
	version, err := scalar.Scalar(ctx, "SELECT version()")
	if err != nil {
		// Best-effort metadata: never fail a benchmark over it.
		return env, err
	}
	env.Version = version
	return env, nil
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

// Scalar runs a single-value query for engine-version reporting.
func (e *sqlExecutor) Scalar(ctx context.Context, query string) (string, error) {
	var value string
	if err := e.db.QueryRowContext(ctx, query).Scan(&value); err != nil {
		return "", err
	}
	return value, nil
}

func (e *sqlExecutor) Close() error {
	return e.db.Close()
}

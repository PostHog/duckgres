package sql

import (
	"context"
	"fmt"
	"time"
)

type Driver interface {
	Execute(context.Context, QueryRequest) (QueryResult, error)
}

type QueryRequest struct {
	StepID  string
	QueryID string
	OrgID   string
	Catalog string
	SQL     string
	PGWire  PGWireConnection
}

type QueryResult struct {
	Rows     int64
	Duration time.Duration
}

type queryRows interface {
	Columns() ([]string, error)
	Next() bool
	Scan(...any) error
	Err() error
	NextResultSet() bool
	Close() error
}

type DatabaseDriver struct{}

func NewDatabaseDriver() *DatabaseDriver {
	return &DatabaseDriver{}
}

func (d *DatabaseDriver) Execute(ctx context.Context, req QueryRequest) (QueryResult, error) {
	started := time.Now()
	db, err := req.PGWire.OpenDB()
	if err != nil {
		return QueryResult{}, fmt.Errorf("open pgwire connection: %w", err)
	}
	defer func() {
		_ = db.Close()
	}()

	rows, err := db.QueryContext(ctx, req.SQL)
	if err == nil {
		count, consumeErr := consumeQueryRows(rows)
		if consumeErr != nil {
			return QueryResult{}, consumeErr
		}
		return QueryResult{Rows: count, Duration: time.Since(started)}, nil
	}

	res, execErr := db.ExecContext(ctx, req.SQL)
	if execErr != nil {
		return QueryResult{}, execErr
	}
	affected, affectedErr := res.RowsAffected()
	if affectedErr != nil {
		affected = 0
	}
	return QueryResult{Rows: affected, Duration: time.Since(started)}, nil
}

func consumeQueryRows(rows queryRows) (count int64, err error) {
	defer func() {
		if closeErr := rows.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	for {
		cols, columnsErr := rows.Columns()
		if columnsErr != nil {
			return count, columnsErr
		}
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		for rows.Next() {
			if scanErr := rows.Scan(ptrs...); scanErr != nil {
				return count, scanErr
			}
			count++
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return count, rowsErr
		}
		if !rows.NextResultSet() {
			if rowsErr := rows.Err(); rowsErr != nil {
				return count, rowsErr
			}
			return count, nil
		}
	}
}

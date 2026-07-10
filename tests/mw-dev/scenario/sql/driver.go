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
		defer func() {
			_ = rows.Close()
		}()
		var count int64
		cols, err := rows.Columns()
		if err != nil {
			return QueryResult{}, err
		}
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		for rows.Next() {
			if err := rows.Scan(ptrs...); err != nil {
				return QueryResult{}, err
			}
			count++
		}
		if err := rows.Err(); err != nil {
			return QueryResult{}, err
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

package flight

import (
	"context"
	"fmt"
	"time"

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

func (d *Driver) Protocol() core.Protocol {
	return core.ProtocolFlight
}

func (d *Driver) Execute(ctx context.Context, query core.Query, args []any) (core.ExecutionResult, error) {
	if d.exec == nil {
		return core.ExecutionResult{}, fmt.Errorf("flight driver has no executor")
	}
	sqlText := query.DuckhogSQL
	if sqlText == "" {
		return core.ExecutionResult{}, fmt.Errorf("query %s missing duckhog_sql", query.QueryID)
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

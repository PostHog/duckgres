package flight

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/posthog/duckgres/server/flightclient"
	"github.com/posthog/duckgres/tests/perf/core"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
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

func NewFromAddress(addr, username, password string, insecureSkipVerify bool) (*Driver, error) {
	tlsCfg := &tls.Config{InsecureSkipVerify: insecureSkipVerify} // test/perf env only
	client, err := flightsql.NewClient(
		addr,
		nil,
		nil,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(flightclient.MaxGRPCMessageSize),
			grpc.MaxCallSendMsgSize(flightclient.MaxGRPCMessageSize),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create Flight SQL client: %w", err)
	}

	token, err := bootstrapSessionToken(client, username, password)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	exec := flightclient.NewFlightExecutorFromClient(client, token)
	return &Driver{
		exec: &flightExecutor{
			client: client,
			exec:   exec,
		},
	}, nil
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

type flightExecutor struct {
	client *flightsql.Client
	exec   *flightclient.FlightExecutor
}

func (e *flightExecutor) Execute(ctx context.Context, query string, args []any) (int64, error) {
	rows, queryErr := e.exec.QueryContext(ctx, query, args...)
	if queryErr == nil {
		defer func() {
			_ = rows.Close()
		}()
		var count int64
		for rows.Next() {
			cols, err := rows.Columns()
			if err != nil {
				return 0, err
			}
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return 0, err
			}
			count++
		}
		if err := rows.Err(); err != nil {
			return 0, err
		}
		return count, nil
	}

	result, execErr := e.exec.ExecContext(ctx, query, args...)
	if execErr != nil {
		return 0, execErr
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, nil
	}
	return affected, nil
}

func (e *flightExecutor) Close() error {
	if e.exec != nil {
		_ = e.exec.Close()
	}
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func bootstrapSessionToken(client *flightsql.Client, username, password string) (string, error) {
	token := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Basic "+token))
	var header metadata.MD
	if _, err := client.GetTables(ctx, &flightsql.GetTablesOpts{}, grpc.Header(&header)); err != nil {
		return "", fmt.Errorf("bootstrap Flight session: %w", err)
	}
	sessionHeaders := header.Get("x-duckgres-session")
	if len(sessionHeaders) == 0 {
		return "", fmt.Errorf("bootstrap Flight session returned no x-duckgres-session header")
	}
	sessionToken := sessionHeaders[0]
	if sessionToken == "" {
		return "", fmt.Errorf("bootstrap Flight session token is empty")
	}
	return sessionToken, nil
}

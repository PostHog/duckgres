package trino

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/posthog/duckgres/tests/perf/core"
	trinoclient "github.com/trinodb/trino-go-client/trino"
)

const (
	defaultSchema              = "posthog"
	defaultSource              = "duckgres-perf"
	defaultStartupTimeout      = 2 * time.Minute
	defaultStartupPollInterval = 2 * time.Second
	startupSmokeSQL            = "SELECT 1"
)

type Executor interface {
	Execute(ctx context.Context, query string, args []any) (int64, error)
	Close() error
}

type SleepFunc func(context.Context, time.Duration) error

type StartupOptions struct {
	Timeout      time.Duration
	PollInterval time.Duration
	Sleep        SleepFunc
}

type ConnectionConfig struct {
	ServerURL  string
	Username   string
	Password   string
	Catalog    string
	Schema     string
	Source     string
	CACertFile string
	Startup    StartupOptions
}

func (c ConnectionConfig) DSN() (string, error) {
	server, err := url.Parse(strings.TrimSpace(c.ServerURL))
	if err != nil {
		return "", fmt.Errorf("parse Trino coordinator URL: %w", err)
	}
	if server.Scheme != "https" {
		return "", fmt.Errorf("trino coordinator URL must use HTTPS for verified password authentication")
	}
	if server.Host == "" {
		return "", fmt.Errorf("trino coordinator URL must include a host")
	}
	if server.User != nil || (server.Path != "" && server.Path != "/") || server.RawQuery != "" || server.Fragment != "" {
		return "", fmt.Errorf("trino coordinator URL must contain only scheme, host, and optional port")
	}
	if c.Username == "" {
		return "", fmt.Errorf("trino username is required")
	}
	if c.Password == "" {
		return "", fmt.Errorf("trino password is required")
	}
	if c.Catalog == "" {
		return "", fmt.Errorf("trino catalog is required")
	}
	schema := c.Schema
	if schema == "" {
		schema = defaultSchema
	}
	source := c.Source
	if source == "" {
		source = defaultSource
	}
	authenticatedServer := &url.URL{
		Scheme: server.Scheme,
		Host:   server.Host,
		User:   url.UserPassword(c.Username, c.Password),
	}
	config := trinoclient.Config{
		ServerURI:   authenticatedServer.String(),
		Source:      source,
		Catalog:     c.Catalog,
		Schema:      schema,
		SSLCertPath: c.CACertFile,
	}
	dsn, err := config.FormatDSN()
	if err != nil {
		return "", fmt.Errorf("format Trino DSN: %w", err)
	}
	return dsn, nil
}

type Driver struct {
	exec Executor
}

func New(ctx context.Context, config ConnectionConfig) (*Driver, error) {
	dsn, err := config.DSN()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, fmt.Errorf("open Trino connection: %w", err)
	}
	driver := NewWithExecutor(&sqlExecutor{db: db})
	if err := driver.WaitReady(ctx, config.Startup); err != nil {
		_ = driver.Close()
		return nil, err
	}
	return driver, nil
}

func NewWithExecutor(exec Executor) *Driver {
	return &Driver{exec: exec}
}

func (d *Driver) Protocol() core.Protocol {
	return core.ProtocolTrino
}

func (d *Driver) Execute(ctx context.Context, query core.Query, args []any) (core.ExecutionResult, error) {
	if d.exec == nil {
		return core.ExecutionResult{}, fmt.Errorf("trino driver has no executor")
	}
	sqlText := query.CanonicalSQL()
	if sqlText == "" {
		return core.ExecutionResult{}, fmt.Errorf("query %s missing canonical SQL", query.QueryID)
	}
	started := time.Now()
	rows, err := d.exec.Execute(ctx, sqlText, args)
	return core.ExecutionResult{
		Rows:     rows,
		Duration: time.Since(started),
	}, err
}

// WaitReady performs an authenticated query before the runner starts timing
// benchmark statements. Readiness state can precede Kubernetes Secret
// projection and Trino file-authenticator refresh, so startup failures are
// retried within an explicit bound.
func (d *Driver) WaitReady(ctx context.Context, options StartupOptions) error {
	if d.exec == nil {
		return fmt.Errorf("trino driver has no executor")
	}
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = defaultStartupTimeout
	}
	interval := options.PollInterval
	if interval <= 0 {
		interval = defaultStartupPollInterval
	}
	sleep := options.Sleep
	if sleep == nil {
		sleep = waitSleep
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastErr error
	for {
		_, err := d.exec.Execute(waitCtx, startupSmokeSQL, nil)
		if err == nil {
			return nil
		}
		lastErr = err
		if err := sleep(waitCtx, interval); err != nil {
			if lastErr != nil {
				return fmt.Errorf("trino startup smoke did not succeed within %s: %w", timeout, lastErr)
			}
			return fmt.Errorf("trino startup smoke did not succeed within %s: %w", timeout, err)
		}
	}
}

func (d *Driver) Close() error {
	if d.exec == nil {
		return nil
	}
	return d.exec.Close()
}

func waitSleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type sqlExecutor struct {
	db *sql.DB
}

func (e *sqlExecutor) Execute(ctx context.Context, query string, args []any) (int64, error) {
	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = rows.Close()
	}()
	columns, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	values := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for index := range values {
		pointers[index] = &values[index]
	}
	var count int64
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return 0, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return count, nil
}

func (e *sqlExecutor) Close() error {
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}

var _ core.ProtocolDriver = (*Driver)(nil)
var _ Executor = (*sqlExecutor)(nil)

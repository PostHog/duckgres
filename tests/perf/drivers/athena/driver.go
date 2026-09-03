package athena

import (
	"context"
	"fmt"
	"strings"
	"time"

	perfcore "github.com/posthog/duckgres/tests/perf/core"
)

const (
	defaultCatalog      = "AwsDataCatalog"
	defaultPollInterval = 500 * time.Millisecond
	defaultQueryTimeout = 30 * time.Minute
	stopTimeout         = 5 * time.Second
)

type QueryState string

const (
	QueryStateQueued    QueryState = "QUEUED"
	QueryStateRunning   QueryState = "RUNNING"
	QueryStateSucceeded QueryState = "SUCCEEDED"
	QueryStateFailed    QueryState = "FAILED"
	QueryStateCancelled QueryState = "CANCELLED"
)

type ConnectionConfig struct {
	Region         string
	WorkGroup      string
	Catalog        string
	Database       string
	OutputLocation string
	PollInterval   time.Duration
	QueryTimeout   time.Duration
}

type StartQueryInput struct {
	SQL                string
	WorkGroup          string
	Catalog            string
	Database           string
	OutputLocation     string
	ResultReuseEnabled bool
}

type QueryStatistics struct {
	QueueDuration    time.Duration
	PlanningDuration time.Duration
	EngineDuration   time.Duration
	ServiceDuration  time.Duration
	BytesScanned     int64
	DPUCount         float64
	ResultReused     bool
}

type QueryExecution struct {
	State             QueryState
	StateChangeReason string
	OutputLocation    string
	EngineVersion     string
	Statistics        QueryStatistics
}

type ResultPage struct {
	RowCount  int64
	NextToken string
}

type Client interface {
	StartQuery(context.Context, StartQueryInput) (string, error)
	GetQuery(context.Context, string) (QueryExecution, error)
	GetResults(context.Context, string, string) (ResultPage, error)
	StopQuery(context.Context, string) error
}

type DriverOptions struct {
	Now   func() time.Time
	Sleep func(context.Context, time.Duration) error
}

type Driver struct {
	client Client
	cfg    ConnectionConfig
	now    func() time.Time
	sleep  func(context.Context, time.Duration) error
}

func New(ctx context.Context, cfg ConnectionConfig) (*Driver, error) {
	client, err := newAWSClient(ctx, cfg.Region)
	if err != nil {
		return nil, err
	}
	return NewWithClient(client, cfg, DriverOptions{})
}

func NewWithClient(client Client, cfg ConnectionConfig, options DriverOptions) (*Driver, error) {
	if client == nil {
		return nil, fmt.Errorf("athena client is required")
	}
	if strings.TrimSpace(cfg.WorkGroup) == "" {
		return nil, fmt.Errorf("athena workgroup is required")
	}
	if strings.TrimSpace(cfg.Database) == "" {
		return nil, fmt.Errorf("athena database is required")
	}
	if !strings.HasPrefix(cfg.OutputLocation, "s3://") {
		return nil, fmt.Errorf("athena output location must be an s3:// URI")
	}
	if cfg.Catalog == "" {
		cfg.Catalog = defaultCatalog
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.QueryTimeout <= 0 {
		cfg.QueryTimeout = defaultQueryTimeout
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Sleep == nil {
		options.Sleep = sleepWithContext
	}
	return &Driver{client: client, cfg: cfg, now: options.Now, sleep: options.Sleep}, nil
}

func (d *Driver) Protocol() perfcore.Protocol { return perfcore.ProtocolAthena }

func (d *Driver) Execute(ctx context.Context, query perfcore.Query, args []any) (result perfcore.ExecutionResult, err error) {
	if len(args) > 0 {
		return result, fmt.Errorf("athena perf queries do not support positional parameters")
	}
	queryCtx, cancel := context.WithTimeout(ctx, d.cfg.QueryTimeout)
	defer cancel()

	startedAt := d.now()
	queryID, err := d.client.StartQuery(queryCtx, StartQueryInput{
		SQL:                query.CanonicalSQL(),
		WorkGroup:          d.cfg.WorkGroup,
		Catalog:            d.cfg.Catalog,
		Database:           d.cfg.Database,
		OutputLocation:     d.cfg.OutputLocation,
		ResultReuseEnabled: false,
	})
	if err != nil {
		return result, fmt.Errorf("start Athena query: %w", err)
	}
	completed := false
	defer func() {
		if completed {
			return
		}
		stopCtx, stopCancel := context.WithTimeout(context.Background(), stopTimeout)
		defer stopCancel()
		_ = d.client.StopQuery(stopCtx, queryID)
	}()

	var execution QueryExecution
	for {
		execution, err = d.client.GetQuery(queryCtx, queryID)
		if err != nil {
			return result, fmt.Errorf("get Athena query %s: %w", queryID, err)
		}
		switch execution.State {
		case QueryStateQueued, QueryStateRunning:
			if err := d.sleep(queryCtx, d.cfg.PollInterval); err != nil {
				return result, err
			}
		case QueryStateSucceeded:
			completed = true
			goto queryComplete
		case QueryStateFailed, QueryStateCancelled:
			completed = true
			return result, fmt.Errorf("athena query %s ended in state %s: %s", queryID, execution.State, execution.StateChangeReason)
		default:
			return result, fmt.Errorf("athena query %s returned unknown state %q", queryID, execution.State)
		}
	}

queryComplete:
	if execution.Statistics.ResultReused {
		return result, fmt.Errorf("athena query %s reused a previous result despite result reuse being disabled", queryID)
	}
	if !outputWithinRoot(execution.OutputLocation, d.cfg.OutputLocation) {
		return result, fmt.Errorf("athena query output %q is outside configured output location %q", execution.OutputLocation, d.cfg.OutputLocation)
	}

	rows, err := d.countRows(queryCtx, queryID)
	if err != nil {
		return result, err
	}
	result.Rows = rows
	result.Duration = d.now().Sub(startedAt)
	result.ServiceMetrics = &perfcore.ServiceMetrics{
		QueueDuration:    execution.Statistics.QueueDuration,
		PlanningDuration: execution.Statistics.PlanningDuration,
		EngineDuration:   execution.Statistics.EngineDuration,
		ServiceDuration:  execution.Statistics.ServiceDuration,
		BytesScanned:     execution.Statistics.BytesScanned,
		DPUCount:         execution.Statistics.DPUCount,
		ResultReused:     execution.Statistics.ResultReused,
		EngineVersion:    execution.EngineVersion,
	}
	return result, nil
}

func (d *Driver) countRows(ctx context.Context, queryID string) (int64, error) {
	var rows int64
	var nextToken string
	firstPage := true
	for {
		page, err := d.client.GetResults(ctx, queryID, nextToken)
		if err != nil {
			return 0, fmt.Errorf("get Athena query results %s: %w", queryID, err)
		}
		pageRows := page.RowCount
		if firstPage && pageRows > 0 {
			pageRows-- // Athena returns the column header as the first result row.
		}
		rows += pageRows
		firstPage = false
		if page.NextToken == "" {
			return rows, nil
		}
		nextToken = page.NextToken
	}
}

func (d *Driver) Close() error { return nil }

func sleepWithContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func outputWithinRoot(outputLocation, configuredRoot string) bool {
	root := strings.TrimSuffix(configuredRoot, "/") + "/"
	return strings.HasPrefix(outputLocation, root)
}

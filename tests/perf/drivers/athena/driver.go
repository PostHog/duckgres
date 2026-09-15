package athena

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awsathena "github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"

	perfcore "github.com/posthog/duckgres/tests/perf/core"
)

const (
	defaultCatalog      = "AwsDataCatalog"
	defaultPollInterval = 500 * time.Millisecond
	defaultQueryTimeout = 30 * time.Minute
	stopTimeout         = 5 * time.Second
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

type athenaAPI interface {
	StartQueryExecution(context.Context, *awsathena.StartQueryExecutionInput, ...func(*awsathena.Options)) (*awsathena.StartQueryExecutionOutput, error)
	GetQueryExecution(context.Context, *awsathena.GetQueryExecutionInput, ...func(*awsathena.Options)) (*awsathena.GetQueryExecutionOutput, error)
	GetQueryResults(context.Context, *awsathena.GetQueryResultsInput, ...func(*awsathena.Options)) (*awsathena.GetQueryResultsOutput, error)
	StopQueryExecution(context.Context, *awsathena.StopQueryExecutionInput, ...func(*awsathena.Options)) (*awsathena.StopQueryExecutionOutput, error)
}

type Driver struct {
	client athenaAPI
	cfg    ConnectionConfig
	now    func() time.Time
	sleep  func(context.Context, time.Duration) error
}

func New(ctx context.Context, cfg ConnectionConfig) (*Driver, error) {
	options := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		options = append(options, awsconfig.WithRegion(cfg.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration for Athena: %w", err)
	}
	return newWithClient(awsathena.NewFromConfig(awsCfg), cfg)
}

func newWithClient(client athenaAPI, cfg ConnectionConfig) (*Driver, error) {
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
	return &Driver{client: client, cfg: cfg, now: time.Now, sleep: sleepWithContext}, nil
}

func (d *Driver) Protocol() perfcore.Protocol { return perfcore.ProtocolAthena }

func (d *Driver) Execute(ctx context.Context, query perfcore.Query, args []any) (perfcore.ExecutionResult, error) {
	return d.execute(ctx, query, args, nil)
}

func (d *Driver) ReadResults(ctx context.Context, query perfcore.Query, args []any) ([][]*string, error) {
	values := make([][]*string, 0)
	_, err := d.execute(ctx, query, args, func(row athenatypes.Row) {
		cells := make([]*string, len(row.Data))
		for i, datum := range row.Data {
			cells[i] = datum.VarCharValue
		}
		values = append(values, cells)
	})
	return values, err
}

func (d *Driver) execute(ctx context.Context, query perfcore.Query, args []any, collect func(athenatypes.Row)) (result perfcore.ExecutionResult, err error) {
	if len(args) > 0 {
		return result, fmt.Errorf("athena perf queries do not support positional parameters")
	}
	sqlText, err := query.SQLFor(d.Protocol())
	if err != nil {
		return result, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, d.cfg.QueryTimeout)
	defer cancel()

	startedAt := d.now()
	defer func() { result.Duration = d.now().Sub(startedAt) }()
	started, err := d.client.StartQueryExecution(queryCtx, &awsathena.StartQueryExecutionInput{
		QueryString: aws.String(sqlText),
		WorkGroup:   aws.String(d.cfg.WorkGroup),
		QueryExecutionContext: &athenatypes.QueryExecutionContext{
			Catalog: aws.String(d.cfg.Catalog), Database: aws.String(d.cfg.Database),
		},
		ResultConfiguration: &athenatypes.ResultConfiguration{OutputLocation: aws.String(d.cfg.OutputLocation)},
		ResultReuseConfiguration: &athenatypes.ResultReuseConfiguration{
			ResultReuseByAgeConfiguration: &athenatypes.ResultReuseByAgeConfiguration{Enabled: false},
		},
	})
	if err != nil {
		return result, fmt.Errorf("start Athena query: %w", err)
	}
	if started == nil || aws.ToString(started.QueryExecutionId) == "" {
		return result, fmt.Errorf("athena returned an empty query execution ID")
	}
	queryID := started.QueryExecutionId
	completed := false
	defer func() {
		if completed {
			return
		}
		stopCtx, stopCancel := context.WithTimeout(context.Background(), stopTimeout)
		defer stopCancel()
		_, _ = d.client.StopQueryExecution(stopCtx, &awsathena.StopQueryExecutionInput{QueryExecutionId: queryID})
	}()

	var execution *athenatypes.QueryExecution
	for {
		output, err := d.client.GetQueryExecution(queryCtx, &awsathena.GetQueryExecutionInput{QueryExecutionId: queryID})
		if err != nil {
			return result, fmt.Errorf("get Athena query %s: %w", *queryID, err)
		}
		if output == nil || output.QueryExecution == nil || output.QueryExecution.Status == nil {
			return result, fmt.Errorf("athena returned incomplete execution state for query %s", *queryID)
		}
		execution = output.QueryExecution
		switch execution.Status.State {
		case athenatypes.QueryExecutionStateQueued, athenatypes.QueryExecutionStateRunning:
			if err := d.sleep(queryCtx, d.cfg.PollInterval); err != nil {
				return result, err
			}
		case athenatypes.QueryExecutionStateSucceeded, athenatypes.QueryExecutionStateFailed, athenatypes.QueryExecutionStateCancelled:
			completed = true
			goto queryComplete
		default:
			return result, fmt.Errorf("athena query %s returned unknown state %q", *queryID, execution.Status.State)
		}
	}

queryComplete:
	// Preserve final service statistics even if execution or result retrieval fails.
	result.ServiceMetrics = serviceMetrics(execution)
	if execution.Status.State != athenatypes.QueryExecutionStateSucceeded {
		return result, fmt.Errorf("athena query %s ended in state %s: %s", *queryID, execution.Status.State, aws.ToString(execution.Status.StateChangeReason))
	}
	if result.ServiceMetrics != nil && result.ServiceMetrics.ResultReused {
		return result, fmt.Errorf("athena query %s reused a previous result despite result reuse being disabled", *queryID)
	}
	var outputLocation string
	if execution.ResultConfiguration != nil {
		outputLocation = aws.ToString(execution.ResultConfiguration.OutputLocation)
	}
	if !outputWithinRoot(outputLocation, d.cfg.OutputLocation) {
		return result, fmt.Errorf("athena query output %q is outside configured output location %q", outputLocation, d.cfg.OutputLocation)
	}

	rows, err := d.readRows(queryCtx, *queryID, collect)
	if err != nil {
		return result, err
	}
	result.Rows = rows
	return result, nil
}

func serviceMetrics(execution *athenatypes.QueryExecution) *perfcore.ServiceMetrics {
	statistics := execution.Statistics
	if statistics == nil {
		return nil
	}
	metrics := &perfcore.ServiceMetrics{
		QueueDuration:    time.Duration(aws.ToInt64(statistics.QueryQueueTimeInMillis)) * time.Millisecond,
		PlanningDuration: time.Duration(aws.ToInt64(statistics.QueryPlanningTimeInMillis)) * time.Millisecond,
		EngineDuration:   time.Duration(aws.ToInt64(statistics.EngineExecutionTimeInMillis)) * time.Millisecond,
		ServiceDuration:  time.Duration(aws.ToInt64(statistics.TotalExecutionTimeInMillis)) * time.Millisecond,
		BytesScanned:     aws.ToInt64(statistics.DataScannedInBytes),
		DPUCount:         aws.ToFloat64(statistics.DpuCount),
	}
	if statistics.ResultReuseInformation != nil {
		metrics.ResultReused = statistics.ResultReuseInformation.ReusedPreviousResult
	}
	if execution.EngineVersion != nil {
		metrics.EngineVersion = aws.ToString(execution.EngineVersion.EffectiveEngineVersion)
	}
	return metrics
}

func (d *Driver) readRows(ctx context.Context, queryID string, collect func(athenatypes.Row)) (int64, error) {
	var rows int64
	input := &awsathena.GetQueryResultsInput{QueryExecutionId: aws.String(queryID), MaxResults: aws.Int32(1000)}
	firstPage := true
	for {
		page, err := d.client.GetQueryResults(ctx, input)
		if err != nil {
			return 0, fmt.Errorf("get Athena query results %s: %w", queryID, err)
		}
		if page == nil {
			return 0, fmt.Errorf("athena returned nil result page")
		}
		var pageRows int64
		if page.ResultSet != nil {
			pageRows = int64(len(page.ResultSet.Rows))
		}
		if firstPage && pageRows > 0 {
			pageRows-- // Athena returns the column header as the first result row.
		}
		if collect != nil && page.ResultSet != nil {
			start := 0
			if firstPage && len(page.ResultSet.Rows) > 0 {
				start = 1
			}
			for _, row := range page.ResultSet.Rows[start:] {
				collect(row)
			}
		}
		rows += pageRows
		firstPage = false
		if aws.ToString(page.NextToken) == "" {
			return rows, nil
		}
		input.NextToken = page.NextToken
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

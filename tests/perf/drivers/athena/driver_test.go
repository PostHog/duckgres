package athena

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsathena "github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"

	perfcore "github.com/posthog/duckgres/tests/perf/core"
)

func TestDriverExecutesOnDemandQueryWithoutResultReuseAndCountsAllRows(t *testing.T) {
	client := &fakeClient{
		executions: []*athenatypes.QueryExecution{
			{Status: &athenatypes.QueryExecutionStatus{State: athenatypes.QueryExecutionStateQueued}},
			{Status: &athenatypes.QueryExecutionStatus{State: athenatypes.QueryExecutionStateRunning}},
			terminalExecution(athenatypes.QueryExecutionStateSucceeded),
		},
		resultPages: []*awsathena.GetQueryResultsOutput{
			{ResultSet: &athenatypes.ResultSet{Rows: make([]athenatypes.Row, 3)}, NextToken: aws.String("page-2")},
			{ResultSet: &athenatypes.ResultSet{Rows: make([]athenatypes.Row, 2)}},
		},
	}
	driver := testDriver(t, client)
	now := time.Unix(1700000000, 0)
	driver.now = func() time.Time {
		now = now.Add(time.Second)
		return now
	}
	driver.sleep = func(context.Context, time.Duration) error { return nil }

	result, err := driver.Execute(context.Background(), perfcore.Query{PGWireSQL: "SELECT * FROM events"}, nil)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Rows != 4 {
		t.Fatalf("rows = %d, want 4 data rows excluding the Athena header", result.Rows)
	}
	if result.Duration != time.Second {
		t.Fatalf("duration = %s, want end-to-end duration of 1s", result.Duration)
	}
	wantInput := &awsathena.StartQueryExecutionInput{
		QueryString: aws.String("SELECT * FROM events"),
		WorkGroup:   aws.String("benchmark"),
		QueryExecutionContext: &athenatypes.QueryExecutionContext{
			Catalog: aws.String("AwsDataCatalog"), Database: aws.String("benchmark_frozen"),
		},
		ResultConfiguration: &athenatypes.ResultConfiguration{OutputLocation: aws.String("s3://benchmark-results/run/")},
		ResultReuseConfiguration: &athenatypes.ResultReuseConfiguration{
			ResultReuseByAgeConfiguration: &athenatypes.ResultReuseByAgeConfiguration{Enabled: false},
		},
	}
	if !reflect.DeepEqual(client.startInput, wantInput) {
		t.Fatalf("start input = %+v, want %+v", client.startInput, wantInput)
	}
	if got, want := client.resultTokens, []string{"", "page-2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("result page tokens = %v, want %v", got, want)
	}
	if client.stopCalls != 0 {
		t.Fatalf("StopQueryExecution calls = %d, want 0 after success", client.stopCalls)
	}
	assertServiceMetrics(t, result.ServiceMetrics)
}

func TestDriverStopsAthenaQueryWhenContextIsCancelled(t *testing.T) {
	client := &fakeClient{executions: []*athenatypes.QueryExecution{{
		Status: &athenatypes.QueryExecutionStatus{State: athenatypes.QueryExecutionStateRunning},
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	driver := testDriver(t, client)
	driver.sleep = func(context.Context, time.Duration) error {
		cancel()
		return ctx.Err()
	}

	_, err := driver.Execute(ctx, perfcore.Query{PGWireSQL: "SELECT 1"}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute error = %v, want context.Canceled", err)
	}
	if client.stopCalls != 1 || client.stoppedID != "query-1" {
		t.Fatalf("StopQueryExecution = %d calls for %q, want one call for query-1", client.stopCalls, client.stoppedID)
	}
	if client.stopContextErr != nil {
		t.Fatalf("stop context already cancelled: %v", client.stopContextErr)
	}
}

func TestDriverUsesRenderedDialectSQL(t *testing.T) {
	client := &fakeClient{executions: []*athenatypes.QueryExecution{terminalExecution(athenatypes.QueryExecutionStateSucceeded)}, resultPages: []*awsathena.GetQueryResultsOutput{{ResultSet: &athenatypes.ResultSet{Rows: make([]athenatypes.Row, 1)}}}}
	driver := testDriver(t, client)
	const native = `SELECT json_extract_scalar(properties, '$["$browser"]') FROM events`
	_, err := driver.Execute(context.Background(), perfcore.Query{PGWireSQL: "SELECT fallback", AthenaSQL: native}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(client.startInput.QueryString) != native {
		t.Fatalf("query = %s", aws.ToString(client.startInput.QueryString))
	}
}

func TestDriverPreservesMetricsWhenExecutionEndsWithError(t *testing.T) {
	for _, state := range []athenatypes.QueryExecutionState{
		athenatypes.QueryExecutionStateFailed, athenatypes.QueryExecutionStateCancelled, athenatypes.QueryExecutionStateSucceeded,
	} {
		t.Run(string(state), func(t *testing.T) {
			client := &fakeClient{executions: []*athenatypes.QueryExecution{terminalExecution(state)}}
			// No result pages: the succeeded case fails while downloading results.
			driver := testDriver(t, client)
			result, err := driver.Execute(context.Background(), perfcore.Query{PGWireSQL: "SELECT 1"}, nil)
			wantError := "scan limit exceeded"
			if state == athenatypes.QueryExecutionStateSucceeded {
				wantError = "get Athena query results query-1"
			}
			if err == nil || !strings.Contains(err.Error(), wantError) {
				t.Fatalf("Execute error = %v, want %q", err, wantError)
			}
			assertServiceMetrics(t, result.ServiceMetrics)
			if result.Duration <= 0 {
				t.Fatalf("duration = %s, want elapsed time despite error", result.Duration)
			}
			if client.stopCalls != 0 {
				t.Fatal("terminal execution should not be stopped again")
			}
		})
	}
}

func TestDriverRejectsResultOutsideConfiguredOutputRoot(t *testing.T) {
	execution := terminalExecution(athenatypes.QueryExecutionStateSucceeded)
	execution.ResultConfiguration.OutputLocation = aws.String("s3://unexpected-bucket/query.csv")
	driver := testDriver(t, &fakeClient{executions: []*athenatypes.QueryExecution{execution}})
	result, err := driver.Execute(context.Background(), perfcore.Query{PGWireSQL: "SELECT 1"}, nil)
	if err == nil || !strings.Contains(err.Error(), "outside configured output location") {
		t.Fatalf("Execute error = %v, want output-location validation error", err)
	}
	assertServiceMetrics(t, result.ServiceMetrics)
}

func TestDriverRejectsUnexpectedlyReusedAthenaResult(t *testing.T) {
	execution := terminalExecution(athenatypes.QueryExecutionStateSucceeded)
	execution.Statistics.ResultReuseInformation = &athenatypes.ResultReuseInformation{ReusedPreviousResult: true}
	driver := testDriver(t, &fakeClient{executions: []*athenatypes.QueryExecution{execution}})
	result, err := driver.Execute(context.Background(), perfcore.Query{PGWireSQL: "SELECT 1"}, nil)
	if err == nil || !strings.Contains(err.Error(), "reused a previous result") {
		t.Fatalf("Execute error = %v, want invalid result-reuse error", err)
	}
	if result.ServiceMetrics == nil || !result.ServiceMetrics.ResultReused {
		t.Fatal("rejected reuse should still be recorded in service metrics")
	}
}

func TestDriverHandlesMissingExecutionFields(t *testing.T) {
	for _, tc := range []struct {
		name      string
		execution *athenatypes.QueryExecution
		wantError string
		wantStops int
	}{
		{"execution", nil, "incomplete execution state", 1},
		{"status", &athenatypes.QueryExecution{}, "incomplete execution state", 1},
		{"output", &athenatypes.QueryExecution{Status: &athenatypes.QueryExecutionStatus{State: athenatypes.QueryExecutionStateSucceeded}}, "outside configured output location", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeClient{executions: []*athenatypes.QueryExecution{tc.execution}}
			driver := testDriver(t, client)
			result, err := driver.Execute(context.Background(), perfcore.Query{PGWireSQL: "SELECT 1"}, nil)
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("Execute error = %v, want %q", err, tc.wantError)
			}
			if result.ServiceMetrics != nil {
				t.Fatal("missing statistics should not become zero-valued metrics")
			}
			if client.stopCalls != tc.wantStops {
				t.Fatalf("stop calls = %d, want %d", client.stopCalls, tc.wantStops)
			}
		})
	}
}

func testDriver(t *testing.T, client athenaAPI) *Driver {
	t.Helper()
	driver, err := newWithClient(client, ConnectionConfig{
		WorkGroup: "benchmark", Database: "benchmark_frozen",
		OutputLocation: "s3://benchmark-results/run/",
	})
	if err != nil {
		t.Fatal(err)
	}
	return driver
}

func terminalExecution(state athenatypes.QueryExecutionState) *athenatypes.QueryExecution {
	return &athenatypes.QueryExecution{
		Status:              &athenatypes.QueryExecutionStatus{State: state, StateChangeReason: aws.String("scan limit exceeded")},
		ResultConfiguration: &athenatypes.ResultConfiguration{OutputLocation: aws.String("s3://benchmark-results/run/query.csv")},
		EngineVersion:       &athenatypes.EngineVersion{EffectiveEngineVersion: aws.String("Athena engine version 3")},
		Statistics: &athenatypes.QueryExecutionStatistics{
			QueryQueueTimeInMillis: aws.Int64(120), QueryPlanningTimeInMillis: aws.Int64(80),
			EngineExecutionTimeInMillis: aws.Int64(2000), TotalExecutionTimeInMillis: aws.Int64(2300),
			DataScannedInBytes: aws.Int64(4096), DpuCount: aws.Float64(10),
		},
	}
}

func assertServiceMetrics(t *testing.T, got *perfcore.ServiceMetrics) {
	t.Helper()
	want := &perfcore.ServiceMetrics{
		QueueDuration: 120 * time.Millisecond, PlanningDuration: 80 * time.Millisecond,
		EngineDuration: 2 * time.Second, ServiceDuration: 2300 * time.Millisecond,
		BytesScanned: 4096, DPUCount: 10, EngineVersion: "Athena engine version 3",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("service metrics = %+v, want %+v", got, want)
	}
}

type fakeClient struct {
	startInput     *awsathena.StartQueryExecutionInput
	executions     []*athenatypes.QueryExecution
	resultPages    []*awsathena.GetQueryResultsOutput
	resultTokens   []string
	stopCalls      int
	stoppedID      string
	stopContextErr error
}

func (f *fakeClient) StartQueryExecution(_ context.Context, input *awsathena.StartQueryExecutionInput, _ ...func(*awsathena.Options)) (*awsathena.StartQueryExecutionOutput, error) {
	f.startInput = input
	return &awsathena.StartQueryExecutionOutput{QueryExecutionId: aws.String("query-1")}, nil
}

func (f *fakeClient) GetQueryExecution(_ context.Context, input *awsathena.GetQueryExecutionInput, _ ...func(*awsathena.Options)) (*awsathena.GetQueryExecutionOutput, error) {
	if aws.ToString(input.QueryExecutionId) != "query-1" {
		return nil, errors.New("unexpected query ID")
	}
	if len(f.executions) == 0 {
		return nil, errors.New("unexpected GetQueryExecution")
	}
	execution := f.executions[0]
	if len(f.executions) > 1 {
		f.executions = f.executions[1:]
	}
	return &awsathena.GetQueryExecutionOutput{QueryExecution: execution}, nil
}

func (f *fakeClient) GetQueryResults(_ context.Context, input *awsathena.GetQueryResultsInput, _ ...func(*awsathena.Options)) (*awsathena.GetQueryResultsOutput, error) {
	if aws.ToString(input.QueryExecutionId) != "query-1" || aws.ToInt32(input.MaxResults) != 1000 {
		return nil, errors.New("unexpected GetQueryResults input")
	}
	f.resultTokens = append(f.resultTokens, aws.ToString(input.NextToken))
	if len(f.resultPages) == 0 {
		return nil, errors.New("unexpected GetQueryResults")
	}
	page := f.resultPages[0]
	f.resultPages = f.resultPages[1:]
	return page, nil
}

func (f *fakeClient) StopQueryExecution(ctx context.Context, input *awsathena.StopQueryExecutionInput, _ ...func(*awsathena.Options)) (*awsathena.StopQueryExecutionOutput, error) {
	f.stopCalls++
	f.stoppedID = aws.ToString(input.QueryExecutionId)
	f.stopContextErr = ctx.Err()
	return &awsathena.StopQueryExecutionOutput{}, nil
}

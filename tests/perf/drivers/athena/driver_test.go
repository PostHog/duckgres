package athena

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	perfcore "github.com/posthog/duckgres/tests/perf/core"
)

func TestDriverExecutesOnDemandQueryWithoutResultReuseAndCountsAllRows(t *testing.T) {
	client := &fakeClient{
		executions: []QueryExecution{
			{State: QueryStateQueued},
			{State: QueryStateRunning},
			{
				State:          QueryStateSucceeded,
				OutputLocation: "s3://benchmark-results/run/query.csv",
				EngineVersion:  "Athena engine version 3",
				Statistics: QueryStatistics{
					QueueDuration:    120 * time.Millisecond,
					PlanningDuration: 80 * time.Millisecond,
					EngineDuration:   2 * time.Second,
					ServiceDuration:  2*time.Second + 300*time.Millisecond,
					BytesScanned:     4096,
				},
			},
		},
		resultPages: []ResultPage{
			{RowCount: 3, NextToken: "page-2"}, // header + two data rows
			{RowCount: 2},
		},
	}
	now := time.Unix(1700000000, 0)
	driver, err := NewWithClient(client, ConnectionConfig{
		WorkGroup:      "benchmark",
		Catalog:        "AwsDataCatalog",
		Database:       "benchmark_frozen",
		OutputLocation: "s3://benchmark-results/run/",
		PollInterval:   time.Millisecond,
		QueryTimeout:   time.Minute,
	}, DriverOptions{
		Now: func() time.Time {
			now = now.Add(time.Second)
			return now
		},
		Sleep: func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewWithClient returned error: %v", err)
	}

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
	if got, want := client.startInput, (StartQueryInput{
		SQL:                "SELECT * FROM events",
		WorkGroup:          "benchmark",
		Catalog:            "AwsDataCatalog",
		Database:           "benchmark_frozen",
		OutputLocation:     "s3://benchmark-results/run/",
		ResultReuseEnabled: false,
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("start input = %+v, want %+v", got, want)
	}
	if got, want := client.resultTokens, []string{"", "page-2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("result page tokens = %v, want %v", got, want)
	}
	if client.stopCalls != 0 {
		t.Fatalf("StopQuery calls = %d, want 0 after success", client.stopCalls)
	}
	if result.ServiceMetrics == nil || result.ServiceMetrics.BytesScanned != 4096 || result.ServiceMetrics.EngineVersion != "Athena engine version 3" {
		t.Fatalf("service metrics = %+v, want Athena execution statistics", result.ServiceMetrics)
	}
}

func TestDriverStopsAthenaQueryWhenContextIsCancelled(t *testing.T) {
	client := &fakeClient{executions: []QueryExecution{{State: QueryStateRunning}}}
	ctx, cancel := context.WithCancel(context.Background())
	driver, err := NewWithClient(client, ConnectionConfig{
		WorkGroup:      "benchmark",
		Database:       "benchmark_frozen",
		OutputLocation: "s3://benchmark-results/run/",
		PollInterval:   time.Millisecond,
		QueryTimeout:   time.Minute,
	}, DriverOptions{
		Sleep: func(context.Context, time.Duration) error {
			cancel()
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatalf("NewWithClient returned error: %v", err)
	}

	_, err = driver.Execute(ctx, perfcore.Query{PGWireSQL: "SELECT 1"}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute error = %v, want context.Canceled", err)
	}
	if client.stopCalls != 1 || client.stoppedID != "query-1" {
		t.Fatalf("StopQuery = %d calls for %q, want one call for query-1", client.stopCalls, client.stoppedID)
	}
}

func TestDriverReturnsAthenaFailureReason(t *testing.T) {
	client := &fakeClient{executions: []QueryExecution{{
		State:             QueryStateFailed,
		StateChangeReason: "scan limit exceeded",
	}}}
	driver, err := NewWithClient(client, ConnectionConfig{
		WorkGroup:      "benchmark",
		Database:       "benchmark_frozen",
		OutputLocation: "s3://benchmark-results/run/",
	}, DriverOptions{})
	if err != nil {
		t.Fatalf("NewWithClient returned error: %v", err)
	}

	_, err = driver.Execute(context.Background(), perfcore.Query{PGWireSQL: "SELECT 1"}, nil)
	if err == nil || !strings.Contains(err.Error(), "scan limit exceeded") {
		t.Fatalf("Execute error = %v, want Athena failure reason", err)
	}
}

func TestDriverRejectsResultOutsideConfiguredOutputRoot(t *testing.T) {
	client := &fakeClient{executions: []QueryExecution{{
		State:          QueryStateSucceeded,
		OutputLocation: "s3://unexpected-bucket/query.csv",
	}}}
	driver, err := NewWithClient(client, ConnectionConfig{
		WorkGroup:      "benchmark",
		Database:       "benchmark_frozen",
		OutputLocation: "s3://benchmark-results/run/",
	}, DriverOptions{})
	if err != nil {
		t.Fatalf("NewWithClient returned error: %v", err)
	}

	_, err = driver.Execute(context.Background(), perfcore.Query{PGWireSQL: "SELECT 1"}, nil)
	if err == nil || !strings.Contains(err.Error(), "outside configured output location") {
		t.Fatalf("Execute error = %v, want output-location validation error", err)
	}
}

func TestDriverRejectsUnexpectedlyReusedAthenaResult(t *testing.T) {
	client := &fakeClient{executions: []QueryExecution{{
		State:          QueryStateSucceeded,
		OutputLocation: "s3://benchmark-results/run/query.csv",
		Statistics:     QueryStatistics{ResultReused: true},
	}}}
	driver, err := NewWithClient(client, ConnectionConfig{
		WorkGroup:      "benchmark",
		Database:       "benchmark_frozen",
		OutputLocation: "s3://benchmark-results/run/",
	}, DriverOptions{})
	if err != nil {
		t.Fatalf("NewWithClient returned error: %v", err)
	}

	_, err = driver.Execute(context.Background(), perfcore.Query{PGWireSQL: "SELECT 1"}, nil)
	if err == nil || !strings.Contains(err.Error(), "reused a previous result") {
		t.Fatalf("Execute error = %v, want invalid result-reuse error", err)
	}
}

type fakeClient struct {
	startInput   StartQueryInput
	executions   []QueryExecution
	resultPages  []ResultPage
	resultTokens []string
	stopCalls    int
	stoppedID    string
}

func (f *fakeClient) StartQuery(_ context.Context, input StartQueryInput) (string, error) {
	f.startInput = input
	return "query-1", nil
}

func (f *fakeClient) GetQuery(_ context.Context, _ string) (QueryExecution, error) {
	if len(f.executions) == 0 {
		return QueryExecution{}, errors.New("unexpected GetQuery")
	}
	execution := f.executions[0]
	if len(f.executions) > 1 {
		f.executions = f.executions[1:]
	}
	return execution, nil
}

func (f *fakeClient) GetResults(_ context.Context, _ string, nextToken string) (ResultPage, error) {
	f.resultTokens = append(f.resultTokens, nextToken)
	if len(f.resultPages) == 0 {
		return ResultPage{}, errors.New("unexpected GetResults")
	}
	page := f.resultPages[0]
	f.resultPages = f.resultPages[1:]
	return page, nil
}

func (f *fakeClient) StopQuery(_ context.Context, queryID string) error {
	f.stopCalls++
	f.stoppedID = queryID
	return nil
}

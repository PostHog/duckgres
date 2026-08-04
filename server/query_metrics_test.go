package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func serverCounterVecValue(t *testing.T, cv *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	counter, err := cv.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("counter labels %v: %v", labels, err)
	}
	m := &dto.Metric{}
	if err := counter.Write(m); err != nil {
		t.Fatalf("counter write labels %v: %v", labels, err)
	}
	return m.GetCounter().GetValue()
}

func serverHistogramVecSampleCount(t *testing.T, hv *prometheus.HistogramVec, labels ...string) uint64 {
	t.Helper()
	observer, err := hv.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("histogram labels %v: %v", labels, err)
	}
	h, ok := observer.(prometheus.Histogram)
	if !ok {
		t.Fatalf("expected prometheus.Histogram, got %T", observer)
	}
	m := &dto.Metric{}
	if err := h.Write(m); err != nil {
		t.Fatalf("histogram write labels %v: %v", labels, err)
	}
	return m.GetHistogram().GetSampleCount()
}

func TestObserveQueryResultIncrementsCanonicalCounter(t *testing.T) {
	org := "query-metrics-outcomes"

	successBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusSuccess), string(queryReasonNone))
	errorBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusError), string(queryReasonSystem))
	canceledBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusFailure), string(queryReasonCanceled))

	observeQueryResult(org, queryStatusSuccess, queryReasonNone)
	observeQueryResult(org, queryStatusError, queryReasonSystem)
	observeQueryResult(org, queryStatusFailure, queryReasonCanceled)

	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusSuccess), string(queryReasonNone)); got != successBefore+1 {
		t.Fatalf("success query total = %v, want %v", got, successBefore+1)
	}
	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusError), string(queryReasonSystem)); got != errorBefore+1 {
		t.Fatalf("error query total = %v, want %v", got, errorBefore+1)
	}
	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusFailure), string(queryReasonCanceled)); got != canceledBefore+1 {
		t.Fatalf("canceled query total = %v, want %v", got, canceledBefore+1)
	}
}

func TestQueryResultClassificationUsesStatusAndReason(t *testing.T) {
	tests := []struct {
		name       string
		category   string
		wantStatus queryStatus
		wantReason queryReason
	}{
		{name: "user", category: "user", wantStatus: queryStatusFailure, wantReason: queryReasonUser},
		{name: "conflict", category: "conflict", wantStatus: queryStatusFailure, wantReason: queryReasonConflict},
		{name: "metadata", category: "metadata_connection_lost", wantStatus: queryStatusError, wantReason: queryReasonMetadataConnectionLost},
		{name: "system", category: "system", wantStatus: queryStatusError, wantReason: queryReasonSystem},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, reason := queryResultFromErrorCategory(tt.category)
			if status != tt.wantStatus || reason != tt.wantReason {
				t.Fatalf("result = (%q, %q), want (%q, %q)", status, reason, tt.wantStatus, tt.wantReason)
			}
		})
	}
}

func TestQueryResultFromSQLState(t *testing.T) {
	tests := []struct {
		code       string
		wantStatus queryStatus
		wantReason queryReason
	}{
		{code: "42601", wantStatus: queryStatusFailure, wantReason: queryReasonUser},
		{code: "57014", wantStatus: queryStatusFailure, wantReason: queryReasonCanceled},
		{code: "XX000", wantStatus: queryStatusError, wantReason: queryReasonSystem},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			status, reason := queryResultFromErrorCode(tt.code)
			if status != tt.wantStatus || reason != tt.wantReason {
				t.Fatalf("result = (%q, %q), want (%q, %q)", status, reason, tt.wantStatus, tt.wantReason)
			}
		})
	}
}

func TestFinishQueryMetricsClassifiesCanceledStatusSeparately(t *testing.T) {
	org := "query-metrics-canceled"
	c := &clientConn{orgID: org}
	scope := c.beginQueryMetrics(time.Now())

	successBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusSuccess), string(queryReasonNone))
	canceledBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusFailure), string(queryReasonCanceled))
	durationBefore := serverHistogramVecSampleCount(t, queryDurationHistogram, org)

	c.lastErrorCode = "57014"
	c.errorResponsesSent = scope.errorResponsesSent + 1
	c.finishQueryMetrics(scope)

	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusSuccess), string(queryReasonNone)); got != successBefore {
		t.Fatalf("success query total = %v, want unchanged %v", got, successBefore)
	}
	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusFailure), string(queryReasonCanceled)); got != canceledBefore+1 {
		t.Fatalf("canceled query total = %v, want %v", got, canceledBefore+1)
	}
	if got := serverHistogramVecSampleCount(t, queryDurationHistogram, org); got != durationBefore+1 {
		t.Fatalf("duration samples = %v, want %v", got, durationBefore+1)
	}
}

func TestFinishQueryMetricsClassifiesSuccessAndErrorStatusesAndReasons(t *testing.T) {
	successOrg := "query-metrics-finish-success"
	successConn := &clientConn{orgID: successOrg}
	successScope := successConn.beginQueryMetrics(time.Now())
	successBefore := serverCounterVecValue(t, queryTotalCounter, successOrg, string(queryStatusSuccess), string(queryReasonNone))
	successDurationBefore := serverHistogramVecSampleCount(t, queryDurationHistogram, successOrg)

	successConn.finishQueryMetrics(successScope)

	if got := serverCounterVecValue(t, queryTotalCounter, successOrg, string(queryStatusSuccess), string(queryReasonNone)); got != successBefore+1 {
		t.Fatalf("success query total = %v, want %v", got, successBefore+1)
	}
	if got := serverHistogramVecSampleCount(t, queryDurationHistogram, successOrg); got != successDurationBefore+1 {
		t.Fatalf("success duration samples = %v, want %v", got, successDurationBefore+1)
	}

	errorOrg := "query-metrics-finish-error"
	errorConn := &clientConn{orgID: errorOrg}
	errorScope := errorConn.beginQueryMetrics(time.Now())
	errorBefore := serverCounterVecValue(t, queryTotalCounter, errorOrg, string(queryStatusFailure), string(queryReasonUser))

	errorConn.lastErrorCode = "42P01"
	errorConn.errorResponsesSent = errorScope.errorResponsesSent + 1
	errorConn.finishQueryMetrics(errorScope)

	if got := serverCounterVecValue(t, queryTotalCounter, errorOrg, string(queryStatusFailure), string(queryReasonUser)); got != errorBefore+1 {
		t.Fatalf("error query total = %v, want %v", got, errorBefore+1)
	}
}

func TestQueryMetricsWrapSimpleAndExtendedEntryPoints(t *testing.T) {
	t.Run("simple success", func(t *testing.T) {
		org := "query-metrics-simple-success"
		c, cleanup := newLifecycleClientConn(t)
		defer cleanup()
		c.orgID = org
		c.executor = &lifecycleExecutor{execResult: emptyExecResult{}}

		successBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusSuccess), string(queryReasonNone))
		durationBefore := serverHistogramVecSampleCount(t, queryDurationHistogram, org)

		if err := c.handleQuery([]byte("UPDATE foo SET x = 1\x00")); err != nil {
			t.Fatalf("handleQuery: %v", err)
		}

		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusSuccess), string(queryReasonNone)); got != successBefore+1 {
			t.Fatalf("success query total = %v, want %v", got, successBefore+1)
		}
		if got := serverHistogramVecSampleCount(t, queryDurationHistogram, org); got != durationBefore+1 {
			t.Fatalf("duration samples = %v, want %v", got, durationBefore+1)
		}
	})

	t.Run("extended error", func(t *testing.T) {
		org := "query-metrics-extended-error"
		c, cleanup := newLifecycleClientConn(t)
		defer cleanup()
		c.orgID = org
		c.executor = &lifecycleExecutor{execErr: errors.New("Catalog Error: table does not exist")}
		c.portals["p1"] = &portal{stmt: &preparedStmt{
			query:          "UPDATE missing SET x = 1",
			convertedQuery: "UPDATE missing SET x = 1",
		}}

		errorBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusFailure), string(queryReasonUser))
		durationBefore := serverHistogramVecSampleCount(t, queryDurationHistogram, org)

		body := append([]byte("p1"), 0)
		body = append(body, 0, 0, 0, 0)
		c.handleExecute(body)

		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusFailure), string(queryReasonUser)); got != errorBefore+1 {
			t.Fatalf("error query total = %v, want %v", got, errorBefore+1)
		}
		if got := serverHistogramVecSampleCount(t, queryDurationHistogram, org); got != durationBefore+1 {
			t.Fatalf("duration samples = %v, want %v", got, durationBefore+1)
		}
	})
}

func TestQueryMetricsCountExtendedParseQueryErrorsWithoutDuration(t *testing.T) {
	org := "query-metrics-extended-parse-error"
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()
	c.orgID = org
	c.executor = &pipelineRecordingExecutor{}

	errorBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusFailure), string(queryReasonUser))
	durationBefore := serverHistogramVecSampleCount(t, queryDurationHistogram, org)

	var body bytes.Buffer
	body.WriteByte(0) // unnamed statement
	body.WriteString(badParseSQL)
	body.WriteByte(0)
	if err := binary.Write(&body, binary.BigEndian, int16(0)); err != nil {
		t.Fatalf("write param count: %v", err)
	}

	c.handleParse(body.Bytes())

	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusFailure), string(queryReasonUser)); got != errorBefore+1 {
		t.Fatalf("error query total = %v, want %v", got, errorBefore+1)
	}
	if got := serverHistogramVecSampleCount(t, queryDurationHistogram, org); got != durationBefore {
		t.Fatalf("duration samples = %v, want unchanged %v", got, durationBefore)
	}
}

func TestQueryMetricsClassifiesSimpleWireWriteFailureAsError(t *testing.T) {
	org := "query-metrics-wire-write-error"
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()
	c.orgID = org
	c.writer = bufio.NewWriterSize(failingWriter{err: errors.New("write tcp: broken pipe")}, 16)
	c.executor = &lifecycleExecutor{
		queryRows: &streamingRowSet{
			cols:      []string{"c"},
			colTypers: []ColumnTyper{stringColumnTyper{}},
			rows:      [][]any{{"hello"}},
		},
	}

	successBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusSuccess), string(queryReasonNone))
	errorBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusError), string(queryReasonSystem))
	durationBefore := serverHistogramVecSampleCount(t, queryDurationHistogram, org)

	if err := c.handleQuery([]byte("SELECT 'hello'\x00")); err == nil {
		t.Fatal("handleQuery returned nil error, want pgwire write failure")
	}

	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusSuccess), string(queryReasonNone)); got != successBefore {
		t.Fatalf("success query total = %v, want unchanged %v", got, successBefore)
	}
	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusError), string(queryReasonSystem)); got != errorBefore+1 {
		t.Fatalf("error query total = %v, want %v", got, errorBefore+1)
	}
	if got := serverHistogramVecSampleCount(t, queryDurationHistogram, org); got != durationBefore+1 {
		t.Fatalf("duration samples = %v, want %v", got, durationBefore+1)
	}
}

func TestQueryMetricsClassifiesTerminalWireWriteFailureAsError(t *testing.T) {
	t.Run("simple exec command complete", func(t *testing.T) {
		org := "query-metrics-simple-terminal-write-error"
		c, cleanup := newLifecycleClientConn(t)
		defer cleanup()
		c.orgID = org
		c.writer = bufio.NewWriterSize(failingWriter{err: errors.New("write tcp: broken pipe")}, 16)
		c.executor = &lifecycleExecutor{execResult: &fakeExecResult{rowsAffected: 123456789012345678}}

		successBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusSuccess), string(queryReasonNone))
		errorBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusError), string(queryReasonSystem))

		if err := c.handleQuery([]byte("UPDATE foo SET x = 1\x00")); err != nil {
			t.Fatalf("handleQuery: %v", err)
		}

		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusSuccess), string(queryReasonNone)); got != successBefore {
			t.Fatalf("success query total = %v, want unchanged %v", got, successBefore)
		}
		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusError), string(queryReasonSystem)); got != errorBefore+1 {
			t.Fatalf("error query total = %v, want %v", got, errorBefore+1)
		}
	})

	t.Run("extended exec command complete", func(t *testing.T) {
		org := "query-metrics-extended-terminal-write-error"
		c, cleanup := newLifecycleClientConn(t)
		defer cleanup()
		c.orgID = org
		c.writer = bufio.NewWriterSize(failingWriter{err: errors.New("write tcp: broken pipe")}, 16)
		c.executor = &lifecycleExecutor{execResult: &fakeExecResult{rowsAffected: 123456789012345678}}
		c.portals["p1"] = &portal{stmt: &preparedStmt{
			query:          "UPDATE foo SET x = 1",
			convertedQuery: "UPDATE foo SET x = 1",
		}}

		successBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusSuccess), string(queryReasonNone))
		errorBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusError), string(queryReasonSystem))

		body := append([]byte("p1"), 0)
		body = append(body, 0, 0, 0, 0)
		c.handleExecute(body)

		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusSuccess), string(queryReasonNone)); got != successBefore {
			t.Fatalf("success query total = %v, want unchanged %v", got, successBefore)
		}
		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusError), string(queryReasonSystem)); got != errorBefore+1 {
			t.Fatalf("error query total = %v, want %v", got, errorBefore+1)
		}
	})

	t.Run("extended exec buffered terminal flush", func(t *testing.T) {
		org := "query-metrics-extended-buffered-terminal-write-error"
		c, cleanup := newLifecycleClientConn(t)
		defer cleanup()
		c.orgID = org
		c.writer = bufio.NewWriterSize(failingWriter{err: errors.New("write tcp: broken pipe")}, 4096)
		c.executor = &lifecycleExecutor{execResult: emptyExecResult{}}
		c.portals["p1"] = &portal{stmt: &preparedStmt{
			query:          "UPDATE foo SET x = 1",
			convertedQuery: "UPDATE foo SET x = 1",
		}}

		successBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusSuccess), string(queryReasonNone))
		errorBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusError), string(queryReasonSystem))

		body := append([]byte("p1"), 0)
		body = append(body, 0, 0, 0, 0)
		c.handleExecute(body)

		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusSuccess), string(queryReasonNone)); got != successBefore {
			t.Fatalf("success query total = %v, want unchanged %v", got, successBefore)
		}
		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryStatusError), string(queryReasonSystem)); got != errorBefore+1 {
			t.Fatalf("error query total = %v, want %v", got, errorBefore+1)
		}
	})
}

func TestQueryErrorsLegacyMetricRemoved(t *testing.T) {
	observeQueryResult("query-metrics-no-legacy-errors", queryStatusError, queryReasonSystem)

	metrics, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, metric := range metrics {
		if metric.GetName() == "duckgres_query_errors_total" {
			t.Fatalf("legacy duckgres_query_errors_total metric is still registered")
		}
	}
}

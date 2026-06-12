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

func TestObserveQueryOutcomeIncrementsCanonicalCounter(t *testing.T) {
	org := "query-metrics-outcomes"

	successBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeSuccess))
	errorBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeError))
	canceledBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeCanceled))

	observeQueryOutcome(org, queryOutcomeSuccess)
	observeQueryOutcome(org, queryOutcomeError)
	observeQueryOutcome(org, queryOutcomeCanceled)

	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeSuccess)); got != successBefore+1 {
		t.Fatalf("success query total = %v, want %v", got, successBefore+1)
	}
	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeError)); got != errorBefore+1 {
		t.Fatalf("error query total = %v, want %v", got, errorBefore+1)
	}
	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeCanceled)); got != canceledBefore+1 {
		t.Fatalf("canceled query total = %v, want %v", got, canceledBefore+1)
	}
}

func TestFinishQueryMetricsClassifiesCanceledOutcomeSeparately(t *testing.T) {
	org := "query-metrics-canceled"
	c := &clientConn{orgID: org}
	scope := c.beginQueryMetrics(time.Now())

	successBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeSuccess))
	canceledBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeCanceled))
	durationBefore := serverHistogramVecSampleCount(t, queryDurationHistogram, org)

	c.lastErrorCode = "57014"
	c.errorResponsesSent = scope.errorResponsesSent + 1
	c.finishQueryMetrics(scope)

	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeSuccess)); got != successBefore {
		t.Fatalf("success query total = %v, want unchanged %v", got, successBefore)
	}
	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeCanceled)); got != canceledBefore+1 {
		t.Fatalf("canceled query total = %v, want %v", got, canceledBefore+1)
	}
	if got := serverHistogramVecSampleCount(t, queryDurationHistogram, org); got != durationBefore+1 {
		t.Fatalf("duration samples = %v, want %v", got, durationBefore+1)
	}
}

func TestFinishQueryMetricsClassifiesSuccessAndErrorOutcomes(t *testing.T) {
	successOrg := "query-metrics-finish-success"
	successConn := &clientConn{orgID: successOrg}
	successScope := successConn.beginQueryMetrics(time.Now())
	successBefore := serverCounterVecValue(t, queryTotalCounter, successOrg, string(queryOutcomeSuccess))
	successDurationBefore := serverHistogramVecSampleCount(t, queryDurationHistogram, successOrg)

	successConn.finishQueryMetrics(successScope)

	if got := serverCounterVecValue(t, queryTotalCounter, successOrg, string(queryOutcomeSuccess)); got != successBefore+1 {
		t.Fatalf("success query total = %v, want %v", got, successBefore+1)
	}
	if got := serverHistogramVecSampleCount(t, queryDurationHistogram, successOrg); got != successDurationBefore+1 {
		t.Fatalf("success duration samples = %v, want %v", got, successDurationBefore+1)
	}

	errorOrg := "query-metrics-finish-error"
	errorConn := &clientConn{orgID: errorOrg}
	errorScope := errorConn.beginQueryMetrics(time.Now())
	errorBefore := serverCounterVecValue(t, queryTotalCounter, errorOrg, string(queryOutcomeError))

	errorConn.lastErrorCode = "42P01"
	errorConn.errorResponsesSent = errorScope.errorResponsesSent + 1
	errorConn.finishQueryMetrics(errorScope)

	if got := serverCounterVecValue(t, queryTotalCounter, errorOrg, string(queryOutcomeError)); got != errorBefore+1 {
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

		successBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeSuccess))
		durationBefore := serverHistogramVecSampleCount(t, queryDurationHistogram, org)

		if err := c.handleQuery([]byte("UPDATE foo SET x = 1\x00")); err != nil {
			t.Fatalf("handleQuery: %v", err)
		}

		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeSuccess)); got != successBefore+1 {
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

		errorBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeError))
		durationBefore := serverHistogramVecSampleCount(t, queryDurationHistogram, org)

		body := append([]byte("p1"), 0)
		body = append(body, 0, 0, 0, 0)
		c.handleExecute(body)

		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeError)); got != errorBefore+1 {
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

	errorBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeError))
	durationBefore := serverHistogramVecSampleCount(t, queryDurationHistogram, org)

	var body bytes.Buffer
	body.WriteByte(0) // unnamed statement
	body.WriteString(badParseSQL)
	body.WriteByte(0)
	if err := binary.Write(&body, binary.BigEndian, int16(0)); err != nil {
		t.Fatalf("write param count: %v", err)
	}

	c.handleParse(body.Bytes())

	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeError)); got != errorBefore+1 {
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

	successBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeSuccess))
	errorBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeError))
	durationBefore := serverHistogramVecSampleCount(t, queryDurationHistogram, org)

	if err := c.handleQuery([]byte("SELECT 'hello'\x00")); err == nil {
		t.Fatal("handleQuery returned nil error, want pgwire write failure")
	}

	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeSuccess)); got != successBefore {
		t.Fatalf("success query total = %v, want unchanged %v", got, successBefore)
	}
	if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeError)); got != errorBefore+1 {
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

		successBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeSuccess))
		errorBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeError))

		if err := c.handleQuery([]byte("UPDATE foo SET x = 1\x00")); err != nil {
			t.Fatalf("handleQuery: %v", err)
		}

		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeSuccess)); got != successBefore {
			t.Fatalf("success query total = %v, want unchanged %v", got, successBefore)
		}
		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeError)); got != errorBefore+1 {
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

		successBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeSuccess))
		errorBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeError))

		body := append([]byte("p1"), 0)
		body = append(body, 0, 0, 0, 0)
		c.handleExecute(body)

		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeSuccess)); got != successBefore {
			t.Fatalf("success query total = %v, want unchanged %v", got, successBefore)
		}
		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeError)); got != errorBefore+1 {
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

		successBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeSuccess))
		errorBefore := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeError))

		body := append([]byte("p1"), 0)
		body = append(body, 0, 0, 0, 0)
		c.handleExecute(body)

		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeSuccess)); got != successBefore {
			t.Fatalf("success query total = %v, want unchanged %v", got, successBefore)
		}
		if got := serverCounterVecValue(t, queryTotalCounter, org, string(queryOutcomeError)); got != errorBefore+1 {
			t.Fatalf("error query total = %v, want %v", got, errorBefore+1)
		}
	})
}

func TestQueryErrorsLegacyMetricRemoved(t *testing.T) {
	observeQueryOutcome("query-metrics-no-legacy-errors", queryOutcomeError)

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

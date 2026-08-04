package observe

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func gaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := gauge.Write(m); err != nil {
		t.Fatalf("gauge write: %v", err)
	}
	return m.GetGauge().GetValue()
}

func counterValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := counter.Write(m); err != nil {
		t.Fatalf("counter write: %v", err)
	}
	return m.GetCounter().GetValue()
}

func counterVecValue(t *testing.T, counterVec *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	counter, err := counterVec.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("counter labels %v: %v", labels, err)
	}
	return counterValue(t, counter)
}

func histogramSampleCount(t *testing.T, histogram prometheus.Histogram) uint64 {
	t.Helper()
	m := &dto.Metric{}
	if err := histogram.Write(m); err != nil {
		t.Fatalf("histogram write: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

func connDurationSample(t *testing.T, org string) (count uint64, sum float64) {
	t.Helper()
	observer, err := connectionDurationHistogram.GetMetricWithLabelValues(org)
	if err != nil {
		t.Fatalf("histogram labels %q: %v", org, err)
	}
	m := &dto.Metric{}
	if err := observer.(interface{ Write(*dto.Metric) error }).Write(m); err != nil {
		t.Fatalf("histogram write labels %q: %v", org, err)
	}
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

// ObserveConnectionDuration must record exactly one sample per call and add the
// observed seconds to the per-org sum — _sum is the load-bearing series for
// exact total connection-seconds (no scrape-integral bias). Verifies org
// labelling is honoured so per-org slicing works.
func TestObserveConnectionDurationRecordsPerOrgSample(t *testing.T) {
	const org = "conn-duration-test-org"
	countBefore, sumBefore := connDurationSample(t, org)

	ObserveConnectionDuration(org, 12.5)
	ObserveConnectionDuration(org, 7.5)

	countAfter, sumAfter := connDurationSample(t, org)
	if countAfter != countBefore+2 {
		t.Fatalf("sample count = %d, want %d", countAfter, countBefore+2)
	}
	if got := sumAfter - sumBefore; got != 20.0 {
		t.Fatalf("sum delta = %v, want 20", got)
	}

	// A different org must not see those samples (label isolation).
	if c, _ := connDurationSample(t, "conn-duration-other-org"); c != 0 {
		t.Fatalf("unrelated org sample count = %d, want 0", c)
	}
}

func TestSessionStartScopeRecordsExactlyOnce(t *testing.T) {
	const org = "session-start-test-org"
	before := sessionStartSampleCount(t, org, "postgres", "success")
	terminalBefore := postgresSessionStartCount(t, org, "success", SessionStartReasonNone)
	failureBefore := postgresSessionStartCount(t, org, "failure", SessionStartReasonControlPlane)

	scope := BeginSessionStart(org, "postgres")
	scope.Finish("success", SessionStartReasonControlPlane)
	scope.Finish("error", SessionStartReasonControlPlane)

	if got := sessionStartSampleCount(t, org, "postgres", "success"); got != before+1 {
		t.Fatalf("success sample count = %d, want %d", got, before+1)
	}
	if got := sessionStartSampleCount(t, org, "postgres", "error"); got != 0 {
		t.Fatalf("error sample count = %d, want 0", got)
	}
	if got := postgresSessionStartCount(t, org, "success", SessionStartReasonNone); got != terminalBefore+1 {
		t.Fatalf("terminal success count = %v, want %v", got, terminalBefore+1)
	}
	if got := postgresSessionStartCount(t, org, "failure", SessionStartReasonControlPlane); got != failureBefore {
		t.Fatalf("second Finish changed terminal classification: count = %v, want %v", got, failureBefore)
	}
}

func TestPostgresSessionStartCounterUsesCanonicalReasonLabel(t *testing.T) {
	descriptions := make(chan *prometheus.Desc, 1)
	postgresSessionStartCounter.Describe(descriptions)
	description := <-descriptions

	if got := description.String(); !strings.Contains(got, "variableLabels: {org,outcome,reason}") {
		t.Fatalf("postgres session start labels = %q, want org, outcome, reason", got)
	}
}

func TestSessionStartScopeRecordsPostgresTerminalFailure(t *testing.T) {
	const org = "session-start-terminal-failure-org"
	before := postgresSessionStartCount(t, org, "failure", SessionStartReasonMetadataStore)
	histogramBefore := sessionStartSampleCount(t, org, "postgres", "timeout")

	BeginSessionStart(org, "postgres").Finish("timeout", SessionStartReasonMetadataStore)

	if got := postgresSessionStartCount(t, org, "failure", SessionStartReasonMetadataStore); got != before+1 {
		t.Fatalf("terminal failure count = %v, want %v", got, before+1)
	}
	if got := sessionStartSampleCount(t, org, "postgres", "timeout"); got != histogramBefore+1 {
		t.Fatalf("timeout histogram count = %d, want %d", got, histogramBefore+1)
	}
}

func TestSessionStartScopeNormalizesUnsafeReasons(t *testing.T) {
	tests := []struct {
		name        string
		outcome     string
		reason      string
		wantOutcome string
		wantReason  string
	}{
		{
			name:        "success always uses none",
			outcome:     "success",
			reason:      SessionStartReasonWorker,
			wantOutcome: "success",
			wantReason:  SessionStartReasonNone,
		},
		{
			name:        "failure cannot use none",
			outcome:     "error",
			reason:      SessionStartReasonNone,
			wantOutcome: "failure",
			wantReason:  SessionStartReasonUnknown,
		},
		{
			name:        "unknown reason stays non-pageable",
			outcome:     "error",
			reason:      "new-unclassified-path",
			wantOutcome: "failure",
			wantReason:  SessionStartReasonUnknown,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			org := fmt.Sprintf("session-start-normalization-%d", i)
			before := postgresSessionStartCount(t, org, tt.wantOutcome, tt.wantReason)

			BeginSessionStart(org, "postgres").Finish(tt.outcome, tt.reason)

			if got := postgresSessionStartCount(t, org, tt.wantOutcome, tt.wantReason); got != before+1 {
				t.Fatalf("normalized terminal count = %v, want %v", got, before+1)
			}
		})
	}
}

func TestSessionStartScopeDoesNotExportFlightTerminalCounter(t *testing.T) {
	const org = "session-start-obsolete-flight-org"
	before := postgresSessionStartCount(t, org, "failure", SessionStartReasonMetadataStore)

	BeginSessionStart(org, "flight").Finish("error", SessionStartReasonMetadataStore)

	if got := postgresSessionStartCount(t, org, "failure", SessionStartReasonMetadataStore); got != before {
		t.Fatalf("obsolete Flight path changed PostgreSQL terminal count: got %v, want %v", got, before)
	}
}

func postgresSessionStartCount(t *testing.T, org, outcome, reason string) float64 {
	t.Helper()
	return counterVecValue(t, postgresSessionStartCounter, org, outcome, reason)
}

func sessionStartSampleCount(t *testing.T, org, protocol, outcome string) uint64 {
	t.Helper()
	observer, err := sessionStartDurationHistogram.GetMetricWithLabelValues(org, protocol, outcome)
	if err != nil {
		t.Fatalf("session start histogram labels: %v", err)
	}
	metric := &dto.Metric{}
	if err := observer.(interface{ Write(*dto.Metric) error }).Write(metric); err != nil {
		t.Fatalf("session start histogram write: %v", err)
	}
	return metric.GetHistogram().GetSampleCount()
}

func TestQueryLogMetricsHelpersRecordBufferFlushAndDrops(t *testing.T) {
	enqueuedBefore := counterValue(t, queryLogEnqueuedEntriesTotal)
	flushedBefore := counterValue(t, queryLogFlushedEntriesTotal)
	droppedFullBefore := counterVecValue(t, queryLogDroppedEntriesTotal, "buffer_full")
	droppedFlushBefore := counterVecValue(t, queryLogDroppedEntriesTotal, "flush_error")
	errorsBefore := counterValue(t, queryLogFlushErrorsTotal)
	flushSamplesBefore := histogramSampleCount(t, queryLogFlushDurationHistogram)

	SetQueryLogBufferedEntries(7)
	if got := gaugeValue(t, queryLogBufferedEntries); got != 7 {
		t.Fatalf("buffered entries gauge = %v, want 7", got)
	}

	IncQueryLogEnqueuedEntries()
	AddQueryLogFlushedEntries(3)
	AddQueryLogDroppedEntries("buffer_full", 2)
	AddQueryLogDroppedEntries("flush_error", 4)
	IncQueryLogFlushErrors()
	ObserveQueryLogFlushDuration(125 * time.Millisecond)

	if got := counterValue(t, queryLogEnqueuedEntriesTotal) - enqueuedBefore; got != 1 {
		t.Fatalf("enqueued delta = %v, want 1", got)
	}
	if got := counterValue(t, queryLogFlushedEntriesTotal) - flushedBefore; got != 3 {
		t.Fatalf("flushed delta = %v, want 3", got)
	}
	if got := counterVecValue(t, queryLogDroppedEntriesTotal, "buffer_full") - droppedFullBefore; got != 2 {
		t.Fatalf("buffer_full dropped delta = %v, want 2", got)
	}
	if got := counterVecValue(t, queryLogDroppedEntriesTotal, "flush_error") - droppedFlushBefore; got != 4 {
		t.Fatalf("flush_error dropped delta = %v, want 4", got)
	}
	if got := counterValue(t, queryLogFlushErrorsTotal) - errorsBefore; got != 1 {
		t.Fatalf("flush errors delta = %v, want 1", got)
	}
	if got := histogramSampleCount(t, queryLogFlushDurationHistogram) - flushSamplesBefore; got != 1 {
		t.Fatalf("flush duration sample delta = %d, want 1", got)
	}
}

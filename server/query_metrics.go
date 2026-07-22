package server

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type queryOutcome string

const (
	queryOutcomeSuccess  queryOutcome = "success"
	queryOutcomeError    queryOutcome = "error"
	queryOutcomeCanceled queryOutcome = "canceled"
)

var queryTotalCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_query_total",
	Help: "Total number of non-empty query attempts partitioned by terminal outcome.",
}, []string{"org", "outcome"})

type queryMetricsScope struct {
	start              time.Time
	errorResponsesSent uint64
	outcome            queryOutcome
	previous           *queryMetricsScope
}

func (c *clientConn) beginQueryMetrics(start time.Time) *queryMetricsScope {
	scope := &queryMetricsScope{
		start:              start,
		errorResponsesSent: c.errorResponsesSent,
		outcome:            queryOutcomeSuccess,
		previous:           c.activeQueryMetrics,
	}
	c.activeQueryMetrics = scope
	return scope
}

func (c *clientConn) finishQueryMetrics(scope *queryMetricsScope) {
	if c.writer != nil {
		if err := c.flushWriter(); err != nil {
			scope.markError(err)
		}
	}

	queryDurationHistogram.WithLabelValues(c.orgID).Observe(time.Since(scope.start).Seconds())

	outcome := scope.outcome
	if outcome == queryOutcomeSuccess && c.errorResponsesSent > scope.errorResponsesSent {
		outcome = queryOutcomeFromErrorCode(c.lastErrorCode)
	}
	observeQueryOutcome(c.orgID, outcome)

	if c.activeQueryMetrics == scope {
		c.activeQueryMetrics = scope.previous
	}
}

func (s *queryMetricsScope) markError(err error) {
	if s == nil {
		return
	}
	if isQueryCancelled(err) {
		s.outcome = queryOutcomeCanceled
		return
	}
	s.outcome = queryOutcomeError
}

func (c *clientConn) markActiveQueryMetricsError(err error) {
	if c == nil || c.activeQueryMetrics == nil {
		return
	}
	c.activeQueryMetrics.markError(err)
}

func queryOutcomeFromErrorCode(code string) queryOutcome {
	if code == "57014" {
		return queryOutcomeCanceled
	}
	return queryOutcomeError
}

func observeQueryOutcome(org string, outcome queryOutcome) {
	switch outcome {
	case queryOutcomeSuccess, queryOutcomeError, queryOutcomeCanceled:
	default:
		outcome = queryOutcomeError
	}

	queryTotalCounter.WithLabelValues(org, string(outcome)).Inc()
}

func (c *clientConn) observeExtendedParseQueryError(code, message string) {
	c.sendError("ERROR", code, message)
	observeQueryOutcome(c.orgID, queryOutcomeError)
}

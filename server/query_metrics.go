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
	client             *clientQueryLifecycle
}

// clientQueryLifecycle is the logical pgwire operation that owns a
// queryMetricsScope. It intentionally lives with the metric scope so handled
// sendError paths and returned Go errors have one terminal owner.
//
// Physical worker statements can be retried, rewritten, or generated many
// times below this scope. They must never create another client lifecycle or
// product-analytics event.
type clientQueryLifecycle struct {
	query       string
	protocol    string
	querySource string
	traceID     string

	error        error
	errorCode    string
	errorMessage string
	finished     bool
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
	if scope == nil {
		return
	}
	if c.writer != nil {
		if err := c.flushWriter(); err != nil {
			scope.markError(err)
		}
	}

	queryDurationHistogram.WithLabelValues(c.orgID).Observe(time.Since(scope.start).Seconds())

	outcome := scope.terminalOutcome(c.errorResponsesSent, c.lastErrorCode)
	observeQueryOutcome(c.orgID, outcome)
	c.finishClientQuery(scope, outcome)

	if c.activeQueryMetrics == scope {
		c.activeQueryMetrics = scope.previous
	}
}

func (s *queryMetricsScope) markError(err error) {
	if s == nil {
		return
	}
	s.recordClientError(err)
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

func (c *clientConn) recordActiveClientQueryError(err error) {
	if c == nil || c.activeQueryMetrics == nil {
		return
	}
	c.activeQueryMetrics.recordClientError(err)
}

func (c *clientConn) recordActiveClientQueryErrorResponse(code, message string) {
	if c == nil || c.activeQueryMetrics == nil {
		return
	}
	c.activeQueryMetrics.recordClientErrorResponse(code, message)
}

func (s *queryMetricsScope) recordClientError(err error) {
	if s == nil || s.client == nil || err == nil {
		return
	}
	if s.client.error == nil {
		s.client.error = err
	}
}

func (s *queryMetricsScope) recordClientErrorResponse(code, message string) {
	if s == nil || s.client == nil {
		return
	}
	if s.client.errorCode == "" {
		s.client.errorCode = code
	}
	if s.client.errorMessage == "" {
		s.client.errorMessage = message
	}
}

func (s *queryMetricsScope) terminalOutcome(errorResponsesSent uint64, lastErrorCode string) queryOutcome {
	if s == nil {
		return queryOutcomeError
	}
	outcome := s.outcome
	if outcome == queryOutcomeSuccess && errorResponsesSent > s.errorResponsesSent {
		code := lastErrorCode
		if s.client != nil && s.client.errorCode != "" {
			code = s.client.errorCode
		}
		outcome = queryOutcomeFromErrorCode(code)
	}
	return outcome
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

package server

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type queryStatus string
type queryReason string

const (
	queryStatusSuccess queryStatus = "success"
	queryStatusFailure queryStatus = "failure"
	queryStatusError   queryStatus = "error"

	queryReasonNone                   queryReason = "none"
	queryReasonUser                   queryReason = "user"
	queryReasonCanceled               queryReason = "canceled"
	queryReasonConflict               queryReason = "conflict"
	queryReasonMetadataConnectionLost queryReason = "metadata_connection_lost"
	queryReasonSystem                 queryReason = "system"
)

var queryTotalCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_query_total",
	Help: "Total number of non-empty query attempts partitioned by terminal status and reason.",
}, []string{"org", "status", "reason"})

type queryMetricsScope struct {
	start              time.Time
	errorResponsesSent uint64
	status             queryStatus
	reason             queryReason
	previous           *queryMetricsScope
}

func (c *clientConn) beginQueryMetrics(start time.Time) *queryMetricsScope {
	scope := &queryMetricsScope{
		start:              start,
		errorResponsesSent: c.errorResponsesSent,
		status:             queryStatusSuccess,
		reason:             queryReasonNone,
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

	status, reason := scope.status, scope.reason
	if status == queryStatusSuccess && c.errorResponsesSent > scope.errorResponsesSent {
		status, reason = queryResultFromErrorCode(c.lastErrorCode)
	}
	observeQueryResult(c.orgID, status, reason)

	if c.activeQueryMetrics == scope {
		c.activeQueryMetrics = scope.previous
	}
}

func (s *queryMetricsScope) markError(err error) {
	if s == nil {
		return
	}
	if isQueryCancelled(err) {
		s.status = queryStatusFailure
		s.reason = queryReasonCanceled
		return
	}
	s.status, s.reason = queryResultFromErrorCategory(queryErrorCategory(err))
}

func (s *queryMetricsScope) markErrorCategory(category string) {
	if s == nil {
		return
	}
	s.status, s.reason = queryResultFromErrorCategory(category)
}

func (c *clientConn) markActiveQueryMetricsError(err error) {
	if c == nil || c.activeQueryMetrics == nil {
		return
	}
	c.activeQueryMetrics.markError(err)
}

func (c *clientConn) markActiveQueryMetricsErrorCategory(category string) {
	if c == nil || c.activeQueryMetrics == nil {
		return
	}
	c.activeQueryMetrics.markErrorCategory(category)
}

func queryResultFromErrorCode(code string) (queryStatus, queryReason) {
	if code == "57014" {
		return queryStatusFailure, queryReasonCanceled
	}
	if isUserQueryErrorCode(code) {
		return queryStatusFailure, queryReasonUser
	}
	return queryStatusError, queryReasonSystem
}

func queryResultFromErrorCategory(category string) (queryStatus, queryReason) {
	switch category {
	case "user":
		return queryStatusFailure, queryReasonUser
	case "conflict":
		return queryStatusFailure, queryReasonConflict
	case "metadata_connection_lost":
		return queryStatusError, queryReasonMetadataConnectionLost
	default:
		return queryStatusError, queryReasonSystem
	}
}

func observeQueryResult(org string, status queryStatus, reason queryReason) {
	switch status {
	case queryStatusSuccess:
		reason = queryReasonNone
	case queryStatusFailure:
		switch reason {
		case queryReasonUser, queryReasonCanceled, queryReasonConflict:
		default:
			status = queryStatusError
			reason = queryReasonSystem
		}
	case queryStatusError:
		switch reason {
		case queryReasonMetadataConnectionLost, queryReasonSystem:
		default:
			reason = queryReasonSystem
		}
	default:
		status = queryStatusError
		reason = queryReasonSystem
	}

	queryTotalCounter.WithLabelValues(org, string(status), string(reason)).Inc()
}

func (c *clientConn) observeExtendedParseQueryError(code, message string) {
	c.sendError("ERROR", code, message)
	status, reason := queryResultFromErrorCode(code)
	observeQueryResult(c.orgID, status, reason)
}

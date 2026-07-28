package server

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/posthog/duckgres/server/querymeta"
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

// queryMetricsScope is the per-query observation record. It began life holding
// only the terminal Prometheus classification; it is now also where per-query
// metadata is stamped by whichever layer knows it (protocol, transpile, exec,
// results) and read back by logQuery. Keeping one scope per query means the
// query log and duckgres_query_total cannot disagree about an outcome.
//
// Scopes nest (a batched simple query runs statements inside an outer scope),
// so `previous` restores the parent on finish.
type queryMetricsScope struct {
	start              time.Time
	errorResponsesSent uint64
	status             queryStatus
	reason             queryReason
	previous           *queryMetricsScope

	// queryID is the statement's UUIDv7. Every query-log event for this
	// statement carries it, so a QueryStart row and its terminal row join on
	// it, and an operator can trace one client complaint through logs, spans,
	// and the query log.
	queryID string
	// parentQueryID and statementIndex place this statement inside the inbound
	// protocol message it came from. Set only for statements of a batched
	// simple query, which run under a nested scope.
	parentQueryID  string
	statementIndex int
	// queryText is the redacted SQL this scope covers, used by the QueryStart
	// event. For a batched simple query it is the individual statement, not the
	// whole message.
	queryText string
	// execStarted records that the statement reached an engine. It is what
	// separates ExceptionBeforeStart from ExceptionWhileProcessing, and it is
	// set at the single point where a query becomes cancellable
	// (queryContextInner) so every execution path — simple, batched, extended,
	// COPY, cursor — marks it without its own call.
	execStarted bool
	// startLogged guards QueryStart emission so a statement that takes several
	// cancellable contexts (COPY, cursor FETCH) still logs exactly one start.
	startLogged bool
	// metadata is what the statement touches (querymeta). Computed at most once
	// per statement — the start and terminal events share it.
	metadata     querymeta.Metadata
	metadataDone bool
}

func (c *clientConn) beginQueryMetrics(start time.Time) *queryMetricsScope {
	scope := &queryMetricsScope{
		start:              start,
		errorResponsesSent: c.errorResponsesSent,
		status:             queryStatusSuccess,
		reason:             queryReasonNone,
		previous:           c.activeQueryMetrics,
		queryID:            newQueryID(),
	}
	c.activeQueryMetrics = scope
	return scope
}

// beginStatementMetrics opens a nested scope for one statement of a batched
// simple query. The statement gets its own query ID — a QueryStart and its
// terminal must pair one-to-one — while parentQueryID and statementIndex keep
// the batch reconstructable.
//
// Close it with endStatementScope, NOT finishQueryMetrics: the enclosing Query
// message owns the metrics for the whole batch (see endStatementScope).
func (c *clientConn) beginStatementMetrics(start time.Time, index int, queryText string) *queryMetricsScope {
	parentID := ""
	if c.activeQueryMetrics != nil {
		parentID = c.activeQueryMetrics.queryID
	}
	scope := c.beginQueryMetrics(start)
	scope.parentQueryID = parentID
	scope.statementIndex = index
	scope.queryText = queryText
	return scope
}

// endStatementScope closes a nested statement scope without emitting metrics.
//
// Metrics stay owned by the enclosing Query message, which is what they have
// always counted: routing them through finishQueryMetrics would turn one
// duckgres_query_total increment per message into one per statement plus one
// for the message, silently changing the meaning of an existing metric and
// double-counting every batch. It would also flush the wire buffer between
// statements of a batch, which the protocol path deliberately does not do.
func (c *clientConn) endStatementScope(scope *queryMetricsScope) {
	if scope == nil {
		return
	}
	if c.activeQueryMetrics == scope {
		c.activeQueryMetrics = scope.previous
	}
}

// markExecStarted records that the statement reached an engine, and emits its
// QueryStart event the first time. Both happen here because this is the same
// instant: the query has become cancellable, so it is live from the client's
// point of view, and any failure from now on is ExceptionWhileProcessing.
func (c *clientConn) markExecStarted() {
	scope := c.activeQueryMetrics
	if scope == nil {
		return
	}
	scope.execStarted = true
	if scope.startLogged {
		return
	}
	scope.startLogged = true
	c.logQueryStart(scope)
}

// currentQueryID returns the active statement's query ID.
//
// A logQuery from a path that never opened a scope gets a freshly minted ID
// rather than an empty one: the ID's job is to identify an event uniquely, and
// inheriting the previous statement's ID would silently merge two statements in
// the log. Reusing a stale scope's value is the failure mode this guards
// against — see the same hazard documented on lastProfilingSummary.
func (c *clientConn) currentQueryID() string {
	if c == nil || c.activeQueryMetrics == nil || c.activeQueryMetrics.queryID == "" {
		return newQueryID()
	}
	return c.activeQueryMetrics.queryID
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

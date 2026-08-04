package server

// Query-log event types, matching ClickHouse's system.query_log `type` column
// so a duckgres query log can be read — or exported — with the same vocabulary.
// ClickHouse encodes these as Enum8; we store the names, because existing rows
// already carry them and the DuckLake view is nicer to read. The codes are kept
// here so an export can map straight across.
const (
	// QueryEventStart is emitted when a statement begins executing. Its
	// terminal counterpart shares the same query_id. A QueryStart with no
	// terminal is how a query that never came back — worker OOM-killed, pod
	// evicted — becomes visible; without it such a query leaves no trace at
	// all.
	QueryEventStart = "QueryStart"
	// QueryEventFinish is a statement that completed and returned to the client.
	QueryEventFinish = "QueryFinish"
	// QueryEventExceptionBeforeStart is a statement that failed BEFORE
	// EXECUTION BEGAN: auth or policy denial, a transpile error, a failure to
	// obtain a worker — and, in practice most often, an extended-protocol
	// Describe whose prepare the engine rejected (a binder error). That last
	// case is why the boundary is "execution began", not "an engine saw it":
	// Describe hands the statement to the worker to learn its result schema, so
	// the engine does see it, and it still never runs. ClickHouse draws the
	// line the same way — analysis-time failures are ExceptionBeforeStart.
	// There is no QueryStart for these, by definition.
	QueryEventExceptionBeforeStart = "ExceptionBeforeStart"
	// QueryEventExceptionWhileProcessing is a statement that failed after
	// execution began.
	QueryEventExceptionWhileProcessing = "ExceptionWhileProcessing"
)

// queryEventCode maps an event type to ClickHouse's Enum8 value.
func queryEventCode(eventType string) uint8 {
	switch eventType {
	case QueryEventStart:
		return 1
	case QueryEventFinish:
		return 2
	case QueryEventExceptionBeforeStart:
		return 3
	case QueryEventExceptionWhileProcessing:
		return 4
	default:
		return 0
	}
}

// terminalQueryEventType classifies a completed statement.
//
// execStarted is what separates the two exception types: a statement that
// failed before execution began is ExceptionBeforeStart, and it has no
// QueryStart row to pair with. Callers with no observation scope pass
// execStarted=true, which keeps the pre-existing behaviour of labelling every
// failure ExceptionWhileProcessing rather than inventing a
// never-started claim we cannot support.
func terminalQueryEventType(errCode string, execStarted bool) string {
	if errCode == "" {
		return QueryEventFinish
	}
	if execStarted {
		return QueryEventExceptionWhileProcessing
	}
	return QueryEventExceptionBeforeStart
}

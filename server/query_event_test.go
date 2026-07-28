package server

import (
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/server/usersecrets"
)

func TestTerminalQueryEventType(t *testing.T) {
	cases := []struct {
		name        string
		errCode     string
		execStarted bool
		want        string
	}{
		{"success", "", true, QueryEventFinish},
		{"success without exec", "", false, QueryEventFinish},
		{"failure after exec began", "42P01", true, QueryEventExceptionWhileProcessing},
		{"failure before any engine saw it", "42601", false, QueryEventExceptionBeforeStart},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := terminalQueryEventType(tc.errCode, tc.execStarted); got != tc.want {
				t.Fatalf("terminalQueryEventType(%q, %v) = %q, want %q", tc.errCode, tc.execStarted, got, tc.want)
			}
		})
	}
}

// TestQueryEventCodesMatchClickHouse pins the Enum8 mapping so an export to a
// ClickHouse-shaped table stays a straight translation.
func TestQueryEventCodesMatchClickHouse(t *testing.T) {
	want := map[string]uint8{
		QueryEventStart:                    1,
		QueryEventFinish:                   2,
		QueryEventExceptionBeforeStart:     3,
		QueryEventExceptionWhileProcessing: 4,
	}
	for eventType, code := range want {
		if got := queryEventCode(eventType); got != code {
			t.Fatalf("queryEventCode(%q) = %d, want %d", eventType, got, code)
		}
	}
	if got := queryEventCode("Nonsense"); got != 0 {
		t.Fatalf("unknown event type should map to 0, got %d", got)
	}
}

func TestLeadingSQLKeyword(t *testing.T) {
	cases := map[string]string{
		"SELECT 1":                    "SELECT",
		"  \n\tselect 1":              "SELECT",
		"/* comment */ INSERT INTO t": "INSERT",
		"-- lead\nUPDATE t SET a = 1": "UPDATE",
		"(SELECT 1)":                  "SELECT",
		"BEGIN;":                      "BEGIN",
		"SET search_path = main":      "SET",
		"count(*)":                    "COUNT",
		"":                            "",
	}
	for query, want := range cases {
		if got := leadingSQLKeyword(query); got != want {
			t.Fatalf("leadingSQLKeyword(%q) = %q, want %q", query, got, want)
		}
	}
}

func TestQueryStartEventsPolicy(t *testing.T) {
	dataStatements := []string{
		"SELECT * FROM events",
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET a = 1",
		"DELETE FROM t",
		"CREATE TABLE t (a INT)",
		"COPY t FROM STDIN",
	}
	chatter := []string{
		"BEGIN", "COMMIT", "ROLLBACK", "SET search_path = main",
		"RESET ALL", "SHOW server_version", "DISCARD ALL", "CLOSE c",
	}

	for _, query := range dataStatements {
		if !QueryStartEventsData.enabled(query) {
			t.Fatalf("data policy should log a start event for %q", query)
		}
		if !QueryStartEventsAll.enabled(query) {
			t.Fatalf("all policy should log a start event for %q", query)
		}
		if QueryStartEventsOff.enabled(query) {
			t.Fatalf("off policy should log nothing, logged %q", query)
		}
	}
	for _, query := range chatter {
		if QueryStartEventsData.enabled(query) {
			t.Fatalf("data policy should skip client chatter %q", query)
		}
		if !QueryStartEventsAll.enabled(query) {
			t.Fatalf("all policy should still log %q", query)
		}
	}
	if QueryStartEventsData.enabled("") || QueryStartEventsAll.enabled("") {
		t.Fatal("an empty statement must never emit a start event")
	}
}

func TestNormalizeQueryStartEventsFallsBackToDefault(t *testing.T) {
	cases := map[string]QueryStartEvents{
		"all":      QueryStartEventsAll,
		"ALL":      QueryStartEventsAll,
		" off ":    QueryStartEventsOff,
		"data":     QueryStartEventsData,
		"":         QueryStartEventsData,
		"nonsense": QueryStartEventsData,
	}
	for input, want := range cases {
		if got := NormalizeQueryStartEvents(input); got != want {
			t.Fatalf("NormalizeQueryStartEvents(%q) = %q, want %q", input, got, want)
		}
	}
}

// TestLogQueryStartPairsWithTerminal is the core of the event model: one
// statement produces a QueryStart and exactly one terminal event, and they
// share a query_id so they can be joined.
func TestLogQueryStartPairsWithTerminal(t *testing.T) {
	c, _, cleanup := newFeedbackClientConn(t)
	defer cleanup()
	exec := &captureQueryLogExecutor{}
	c.executor = exec
	c.server.cfg.QueryLog.Enabled = true
	c.server.cfg.QueryLog.StartEvents = QueryStartEventsData

	start := time.Unix(1700000000, 0).UTC()
	scope := c.beginQueryMetrics(start)
	scope.queryText = "SELECT * FROM events"
	c.markExecStarted()
	// A second cancellable context (COPY, cursor FETCH) must not emit a second
	// start event.
	c.markExecStarted()
	c.logQuery(start, "SELECT * FROM events", "", "SELECT", 3, 0, "", "", "simple")
	c.finishQueryMetrics(scope)

	if len(exec.entries) != 2 {
		t.Fatalf("expected a start and a terminal event, got %d", len(exec.entries))
	}
	startEvent, terminal := exec.entries[0], exec.entries[1]
	if startEvent.Type != QueryEventStart {
		t.Fatalf("first event type = %q, want %q", startEvent.Type, QueryEventStart)
	}
	if terminal.Type != QueryEventFinish {
		t.Fatalf("terminal event type = %q, want %q", terminal.Type, QueryEventFinish)
	}
	if startEvent.QueryID != terminal.QueryID || startEvent.QueryID == "" {
		t.Fatalf("start/terminal must share a query_id: %q vs %q", startEvent.QueryID, terminal.QueryID)
	}
	if !startEvent.EventTime.Equal(terminal.EventTime) {
		t.Fatalf("both events carry the statement start time: %s vs %s", startEvent.EventTime, terminal.EventTime)
	}
	if startEvent.QueryDurationMs != 0 {
		t.Fatalf("a start event has no duration yet, got %d", startEvent.QueryDurationMs)
	}
	if startEvent.ResultRows != 0 {
		t.Fatalf("a start event has no resource counts yet, got %d rows", startEvent.ResultRows)
	}
	if terminal.ResultRows != 3 {
		t.Fatalf("terminal result rows = %d, want 3", terminal.ResultRows)
	}
}

// TestLogQueryExceptionBeforeStart covers the failure that never reached an
// engine: it must be distinguishable from a failure mid-execution, and it must
// have no start event to pair with.
func TestLogQueryExceptionBeforeStart(t *testing.T) {
	c, _, cleanup := newFeedbackClientConn(t)
	defer cleanup()
	exec := &captureQueryLogExecutor{}
	c.executor = exec
	c.server.cfg.QueryLog.Enabled = true
	c.server.cfg.QueryLog.StartEvents = QueryStartEventsData

	start := time.Unix(1700000000, 0).UTC()
	scope := c.beginQueryMetrics(start)
	scope.queryText = "SELECT * FROM events"
	// No markExecStarted: the statement was rejected during transpile.
	c.logQuery(start, "SELECT * FROM events", "", "SELECT", 0, 0, "42601", "syntax error", "simple")
	c.finishQueryMetrics(scope)

	if len(exec.entries) != 1 {
		t.Fatalf("expected only a terminal event, got %d", len(exec.entries))
	}
	if got := exec.entries[0].Type; got != QueryEventExceptionBeforeStart {
		t.Fatalf("event type = %q, want %q", got, QueryEventExceptionBeforeStart)
	}
}

// TestLogQueryWithoutScopeAssumesExecStarted keeps the pre-existing
// classification for paths with no observation scope: claiming a statement
// never started is a stronger assertion than the evidence supports.
func TestLogQueryWithoutScopeAssumesExecStarted(t *testing.T) {
	c, _, cleanup := newFeedbackClientConn(t)
	defer cleanup()
	exec := &captureQueryLogExecutor{}
	c.executor = exec
	c.server.cfg.QueryLog.Enabled = true

	c.logQuery(time.Unix(1700000000, 0).UTC(), "SELECT 1", "", "SELECT", 0, 0, "XX000", "boom", "simple")

	if len(exec.entries) != 1 {
		t.Fatalf("expected one entry, got %d", len(exec.entries))
	}
	if got := exec.entries[0].Type; got != QueryEventExceptionWhileProcessing {
		t.Fatalf("event type = %q, want %q", got, QueryEventExceptionWhileProcessing)
	}
}

// TestLogQueryStartSkipsSelfReferentialQueries stops the query-log poll from
// logging itself, which would recurse.
func TestLogQueryStartSkipsSelfReferentialQueries(t *testing.T) {
	c, _, cleanup := newFeedbackClientConn(t)
	defer cleanup()
	exec := &captureQueryLogExecutor{}
	c.executor = exec
	c.server.cfg.QueryLog.Enabled = true
	c.server.cfg.QueryLog.StartEvents = QueryStartEventsAll

	scope := c.beginQueryMetrics(time.Unix(1700000000, 0).UTC())
	scope.queryText = "SELECT count(*) FROM ducklake.system.query_log"
	c.markExecStarted()
	c.finishQueryMetrics(scope)

	if len(exec.entries) != 0 {
		t.Fatalf("a query naming the query log must not be logged, got %d entries", len(exec.entries))
	}
}

// TestLogQueryStartRedactsSecretDDL: the scope carries already-redacted text,
// and the start event must not undo that.
func TestLogQueryStartRedactsSecretDDL(t *testing.T) {
	c, _, cleanup := newFeedbackClientConn(t)
	defer cleanup()
	exec := &captureQueryLogExecutor{}
	c.executor = exec
	c.server.cfg.QueryLog.Enabled = true
	c.server.cfg.QueryLog.StartEvents = QueryStartEventsAll

	scope := c.beginStatementMetrics(time.Unix(1700000000, 0).UTC(), 0,
		usersecrets.RedactForLog("CREATE PERSISTENT SECRET s (TYPE s3, KEY_ID 'AKIAEXAMPLE', SECRET 'topsecret')"))
	c.markExecStarted()
	c.finishQueryMetrics(scope)

	if len(exec.entries) != 1 {
		t.Fatalf("expected one start event, got %d", len(exec.entries))
	}
	if strings.Contains(exec.entries[0].Query, "topsecret") {
		t.Fatalf("start event leaked secret material: %q", exec.entries[0].Query)
	}
}

// TestExecutionPathEmitsQueryStart drives a real execution path rather than
// calling markExecStarted directly. It is the test that validates the choke
// point: QueryStart hangs off queryContextInner — where a statement becomes
// cancellable — so every path reaching an engine emits one without its own
// call. If someone adds an execution path that bypasses queryContext, its
// statements go missing from the start-event stream and this pattern is how
// that gets caught.
func TestExecutionPathEmitsQueryStart(t *testing.T) {
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()
	exec := &captureQueryLogExecutor{}
	c.executor = exec
	c.username = "alice"
	c.server.cfg.QueryLog.Enabled = true
	c.server.cfg.QueryLog.StartEvents = QueryStartEventsData

	scope := c.beginQueryMetrics(time.Now())
	scope.queryText = "UPDATE foo SET x = 1"
	if err := c.executeQueryDirect("UPDATE foo SET x = 1", "UPDATE"); err != nil {
		t.Fatalf("executeQueryDirect: %v", err)
	}
	defer c.finishQueryMetrics(scope)

	if len(exec.entries) == 0 {
		t.Fatal("execution path logged nothing")
	}
	if got := exec.entries[0].Type; got != QueryEventStart {
		t.Fatalf("first event from the execution path = %q, want %q", got, QueryEventStart)
	}
	if !scope.execStarted {
		t.Fatal("reaching an engine must mark the statement as started")
	}
	// Having reached an engine, a later failure is ExceptionWhileProcessing.
	c.logQuery(scope.start, "UPDATE foo SET x = 1", "", "UPDATE", 0, 0, "XX000", "boom", "simple")
	last := exec.entries[len(exec.entries)-1]
	if last.Type != QueryEventExceptionWhileProcessing {
		t.Fatalf("post-exec failure type = %q, want %q", last.Type, QueryEventExceptionWhileProcessing)
	}
	if last.QueryID != exec.entries[0].QueryID {
		t.Fatalf("terminal must share the start event's query_id: %q vs %q", last.QueryID, exec.entries[0].QueryID)
	}
}

// TestBeginStatementMetricsLinksBatchStatements: statements of one batched
// simple query get their own IDs but stay reconstructable as a batch.
func TestBeginStatementMetricsLinksBatchStatements(t *testing.T) {
	c, _, cleanup := newFeedbackClientConn(t)
	defer cleanup()
	exec := &captureQueryLogExecutor{}
	c.executor = exec
	c.server.cfg.QueryLog.Enabled = true
	c.server.cfg.QueryLog.StartEvents = QueryStartEventsOff

	start := time.Unix(1700000000, 0).UTC()
	message := c.beginQueryMetrics(start)
	message.queryText = "SELECT 1; SELECT 2"

	for i, sql := range []string{"SELECT 1", "SELECT 2"} {
		statement := c.beginStatementMetrics(start, i, sql)
		c.logQuery(start, sql, "", "SELECT", 1, 0, "", "", "simple-batch")
		c.finishQueryMetrics(statement)
	}
	c.finishQueryMetrics(message)

	if len(exec.entries) != 2 {
		t.Fatalf("expected one entry per statement, got %d", len(exec.entries))
	}
	first, second := exec.entries[0], exec.entries[1]
	if first.QueryID == second.QueryID {
		t.Fatal("each statement of a batch needs its own query_id")
	}
	if first.ParentQueryID != message.queryID || second.ParentQueryID != message.queryID {
		t.Fatalf("statements must link to the message id %q, got %q and %q",
			message.queryID, first.ParentQueryID, second.ParentQueryID)
	}
	if first.StatementIndex != 0 || second.StatementIndex != 1 {
		t.Fatalf("statement indexes = %d, %d; want 0, 1", first.StatementIndex, second.StatementIndex)
	}
}

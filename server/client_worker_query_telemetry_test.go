package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/internal/analytics"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// clientWorkerTelemetryEvent captures the product events emitted while a
// handler runs. It keeps these lifecycle tests independent from the older
// worker-helper analytics tests.
type clientWorkerTelemetryEvent struct {
	event string
	orgID string
	props map[string]any
}

type clientWorkerTelemetryTracker struct {
	events []clientWorkerTelemetryEvent
}

func (t *clientWorkerTelemetryTracker) Capture(event, orgID string, props map[string]any) {
	copyProps := make(map[string]any, len(props))
	for key, value := range props {
		copyProps[key] = value
	}
	t.events = append(t.events, clientWorkerTelemetryEvent{
		event: event,
		orgID: orgID,
		props: copyProps,
	})
}

func (*clientWorkerTelemetryTracker) Close() {}

func installClientWorkerTelemetryTracker(t *testing.T) *clientWorkerTelemetryTracker {
	t.Helper()

	tracker := &clientWorkerTelemetryTracker{}
	previous := analytics.Default()
	analytics.SetDefault(tracker)
	t.Cleanup(func() { analytics.SetDefault(previous) })
	return tracker
}

func captureClientWorkerTelemetryLogs(t *testing.T) *bytes.Buffer {
	t.Helper()

	previous := slog.Default()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

func installClientWorkerTelemetryTracing(t *testing.T) {
	t.Helper()

	previous := otel.GetTracerProvider()
	provider := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
}

func clientWorkerTelemetryLogLines(output, message string) []string {
	needle := `msg="` + message + `"`
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if strings.Contains(line, needle) {
			lines = append(lines, line)
		}
	}
	return lines
}

func clientWorkerTelemetryLogLinesWithAttr(output, message, attr string) []string {
	var lines []string
	for _, line := range clientWorkerTelemetryLogLines(output, message) {
		if strings.Contains(line, attr) {
			lines = append(lines, line)
		}
	}
	return lines
}

func requireClientWorkerTelemetryLogCount(t *testing.T, output, message string, want int) []string {
	t.Helper()

	lines := clientWorkerTelemetryLogLines(output, message)
	if got := len(lines); got != want {
		t.Errorf("%q log count = %d, want %d; logs:\n%s", message, got, want, output)
	}
	return lines
}

func assertClientWorkerTelemetryAttrs(t *testing.T, lines []string, attrs ...string) {
	t.Helper()

	for _, line := range lines {
		for _, attr := range attrs {
			if !strings.Contains(line, attr) {
				t.Errorf("log line missing %q:\n%s", attr, line)
			}
		}
	}
}

func clientWorkerTelemetryEvents(tracker *clientWorkerTelemetryTracker, event string) []clientWorkerTelemetryEvent {
	var events []clientWorkerTelemetryEvent
	for _, captured := range tracker.events {
		if captured.event == event {
			events = append(events, captured)
		}
	}
	return events
}

func requireClientWorkerTelemetryEventCount(t *testing.T, tracker *clientWorkerTelemetryTracker, event string, want int) []clientWorkerTelemetryEvent {
	t.Helper()

	events := clientWorkerTelemetryEvents(tracker, event)
	if got := len(events); got != want {
		t.Errorf("%q analytics event count = %d, want %d", event, got, want)
	}
	return events
}

var clientWorkerTelemetryTraceID = regexp.MustCompile(`(?:^|\s)trace_id=("[^"]*"|\S+)`)

func clientWorkerTelemetryTraceIDFromLog(t *testing.T, line string) string {
	t.Helper()

	match := clientWorkerTelemetryTraceID.FindStringSubmatch(line)
	if len(match) != 2 {
		t.Errorf("log line has no trace_id:\n%s", line)
		return ""
	}
	traceID := strings.Trim(match[1], `"`)
	if traceID == "" {
		t.Errorf("log line has an empty trace_id:\n%s", line)
	}
	return traceID
}

func TestClientWorkerTelemetrySimpleQuery(t *testing.T) {
	logs := captureClientWorkerTelemetryLogs(t)
	tracker := installClientWorkerTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.username = "telemetry-user"
	c.orgID = "telemetry-org"
	c.executor = &lifecycleExecutor{execResult: emptyExecResult{}}
	const query = "UPDATE widgets SET enabled = true"

	if err := c.handleQuery([]byte(query + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	output := logs.String()
	clientStarts := requireClientWorkerTelemetryLogCount(t, output, "Client query started.", 1)
	clientFinishes := requireClientWorkerTelemetryLogCount(t, output, "Client query finished.", 1)
	workerStarts := requireClientWorkerTelemetryLogCount(t, output, "Worker statement started.", 1)
	workerFinishes := requireClientWorkerTelemetryLogCount(t, output, "Worker statement finished.", 1)

	assertClientWorkerTelemetryAttrs(t, clientStarts, "scope=client", "protocol=simple", `query="`+query+`"`)
	assertClientWorkerTelemetryAttrs(t, clientFinishes, "scope=client", "protocol=simple", "outcome=success")
	assertClientWorkerTelemetryAttrs(t, workerStarts, "scope=worker", "origin=client")
	assertClientWorkerTelemetryAttrs(t, workerFinishes, "scope=worker", "origin=client")

	initiated := requireClientWorkerTelemetryEventCount(t, tracker, "query_initiated", 1)
	if len(initiated) == 1 && initiated[0].orgID != c.orgID {
		t.Errorf("query_initiated org = %q, want %q", initiated[0].orgID, c.orgID)
	}
	requireClientWorkerTelemetryEventCount(t, tracker, "query_failed", 0)
}

func TestClientWorkerTelemetryExtendedExecuteUsesQueryTrace(t *testing.T) {
	installClientWorkerTelemetryTracing(t)
	logs := captureClientWorkerTelemetryLogs(t)
	tracker := installClientWorkerTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.username = "telemetry-user"
	c.orgID = "telemetry-org"
	c.executor = &lifecycleExecutor{execResult: emptyExecResult{}}
	const query = "UPDATE trace_target SET value = 1"
	c.portals["telemetry-portal"] = &portal{stmt: &preparedStmt{
		query:          query,
		convertedQuery: query,
	}}
	body := append([]byte("telemetry-portal"), 0)
	body = append(body, 0, 0, 0, 0)

	c.handleExecute(body)

	output := logs.String()
	clientStarts := requireClientWorkerTelemetryLogCount(t, output, "Client query started.", 1)
	clientFinishes := requireClientWorkerTelemetryLogCount(t, output, "Client query finished.", 1)
	requireClientWorkerTelemetryLogCount(t, output, "Worker statement started.", 1)
	requireClientWorkerTelemetryLogCount(t, output, "Worker statement finished.", 1)
	assertClientWorkerTelemetryAttrs(t, clientStarts, "scope=client", "protocol=extended")
	assertClientWorkerTelemetryAttrs(t, clientFinishes, "scope=client", "protocol=extended", "outcome=success")

	initiated := requireClientWorkerTelemetryEventCount(t, tracker, "query_initiated", 1)
	if len(clientStarts) != 1 || len(clientFinishes) != 1 || len(initiated) != 1 {
		return
	}

	startTraceID := clientWorkerTelemetryTraceIDFromLog(t, clientStarts[0])
	finishTraceID := clientWorkerTelemetryTraceIDFromLog(t, clientFinishes[0])
	analyticsTraceID, ok := initiated[0].props["trace_id"].(string)
	if !ok || analyticsTraceID == "" {
		t.Errorf("query_initiated trace_id = %#v, want non-empty string", initiated[0].props["trace_id"])
		return
	}
	if startTraceID != finishTraceID || startTraceID != analyticsTraceID {
		t.Errorf("extended lifecycle trace IDs = start %q, finish %q, analytics %q; want one matching non-empty query trace",
			startTraceID, finishTraceID, analyticsTraceID)
	}
}

func TestClientWorkerTelemetryServerHandledSimpleQuery(t *testing.T) {
	logs := captureClientWorkerTelemetryLogs(t)
	tracker := installClientWorkerTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.username = "telemetry-user"
	c.orgID = "telemetry-org"
	const query = "SHOW duckgres.query_source"

	if err := c.handleQuery([]byte(query + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	output := logs.String()
	clientStarts := requireClientWorkerTelemetryLogCount(t, output, "Client query started.", 1)
	clientFinishes := requireClientWorkerTelemetryLogCount(t, output, "Client query finished.", 1)
	requireClientWorkerTelemetryLogCount(t, output, "Worker statement started.", 0)
	requireClientWorkerTelemetryLogCount(t, output, "Worker statement finished.", 0)
	assertClientWorkerTelemetryAttrs(t, clientStarts, "scope=client", "protocol=simple", `query="`+query+`"`)
	assertClientWorkerTelemetryAttrs(t, clientFinishes, "scope=client", "protocol=simple", "outcome=success")
	requireClientWorkerTelemetryEventCount(t, tracker, "query_initiated", 1)
	requireClientWorkerTelemetryEventCount(t, tracker, "query_failed", 0)
}

func TestClientWorkerTelemetrySimpleBatch(t *testing.T) {
	logs := captureClientWorkerTelemetryLogs(t)
	tracker := installClientWorkerTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.username = "telemetry-user"
	c.orgID = "telemetry-org"
	c.executor = &lifecycleExecutor{execResult: emptyExecResult{}}
	const query = "UPDATE batch_target SET value = 1; UPDATE batch_target SET value = 2"

	if err := c.handleQuery([]byte(query + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	output := logs.String()
	clientStarts := requireClientWorkerTelemetryLogCount(t, output, "Client query started.", 1)
	clientFinishes := requireClientWorkerTelemetryLogCount(t, output, "Client query finished.", 1)
	workerStarts := requireClientWorkerTelemetryLogCount(t, output, "Worker statement started.", 2)
	workerFinishes := requireClientWorkerTelemetryLogCount(t, output, "Worker statement finished.", 2)
	assertClientWorkerTelemetryAttrs(t, clientStarts, "scope=client", "protocol=simple", `query="`+query+`"`)
	assertClientWorkerTelemetryAttrs(t, clientFinishes, "scope=client", "outcome=success")
	assertClientWorkerTelemetryAttrs(t, workerStarts, "scope=worker", "origin=client", "operation=simple_batch_statement")
	assertClientWorkerTelemetryAttrs(t, workerFinishes, "scope=worker", "origin=client", "operation=simple_batch_statement")
	requireClientWorkerTelemetryEventCount(t, tracker, "query_initiated", 1)
	requireClientWorkerTelemetryEventCount(t, tracker, "query_failed", 0)
}

func TestClientWorkerTelemetryFailureHasOneTerminalAnalyticsEvent(t *testing.T) {
	logs := captureClientWorkerTelemetryLogs(t)
	tracker := installClientWorkerTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.username = "telemetry-user"
	c.orgID = "telemetry-org"
	c.executor = &lifecycleExecutor{execErr: errors.New("Catalog Error: Table with name missing does not exist")}

	if err := c.handleQuery([]byte("UPDATE missing SET value = 1\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	output := logs.String()
	requireClientWorkerTelemetryLogCount(t, output, "Client query started.", 1)
	clientFinishes := requireClientWorkerTelemetryLogCount(t, output, "Client query finished.", 1)
	assertClientWorkerTelemetryAttrs(t, clientFinishes, "scope=client", "outcome=error")
	requireClientWorkerTelemetryEventCount(t, tracker, "query_initiated", 1)
	failed := requireClientWorkerTelemetryEventCount(t, tracker, "query_failed", 1)
	if len(failed) == 1 && failed[0].props["error_category"] != "user" {
		t.Errorf("query_failed error_category = %#v, want user", failed[0].props["error_category"])
	}
}

func TestClientWorkerTelemetryCancellationHasOneCanceledFinish(t *testing.T) {
	logs := captureClientWorkerTelemetryLogs(t)
	tracker := installClientWorkerTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.username = "telemetry-user"
	c.orgID = "telemetry-org"
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.cancel()
	c.executor = &lifecycleExecutor{execErr: context.Canceled}

	if err := c.handleQuery([]byte("UPDATE cancelled_target SET value = 1\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	output := logs.String()
	requireClientWorkerTelemetryLogCount(t, output, "Client query started.", 1)
	clientFinishes := requireClientWorkerTelemetryLogCount(t, output, "Client query finished.", 1)
	assertClientWorkerTelemetryAttrs(t, clientFinishes, "scope=client", "outcome=canceled", "sqlstate=57014")
	requireClientWorkerTelemetryEventCount(t, tracker, "query_initiated", 1)
	requireClientWorkerTelemetryEventCount(t, tracker, "query_failed", 0)
}

// cancellationMetadataRowSet simulates a worker whose result metadata fails
// only after Query has successfully opened a cursor.
type cancellationMetadataRowSet struct{}

func (*cancellationMetadataRowSet) Columns() ([]string, error)          { return nil, context.Canceled }
func (*cancellationMetadataRowSet) ColumnTypes() ([]ColumnTyper, error) { return nil, nil }
func (*cancellationMetadataRowSet) Next() bool                          { return false }
func (*cancellationMetadataRowSet) Scan(...any) error                   { return nil }
func (*cancellationMetadataRowSet) Close() error                        { return nil }
func (*cancellationMetadataRowSet) Err() error                          { return nil }

func TestClientWorkerTelemetryCancellationDuringResultMetadata(t *testing.T) {
	for _, protocol := range []string{"simple", "extended"} {
		t.Run(protocol, func(t *testing.T) {
			logs := captureClientWorkerTelemetryLogs(t)
			tracker := installClientWorkerTelemetryTracker(t)
			c, cleanup := newLifecycleClientConn(t)
			defer cleanup()

			c.username = "telemetry-user"
			c.orgID = "telemetry-org"
			c.ctx, c.cancel = context.WithCancel(context.Background())
			c.cancel()
			c.executor = &lifecycleExecutor{queryRows: &cancellationMetadataRowSet{}}

			const query = "SELECT * FROM cancellation_target"
			if protocol == "simple" {
				if err := c.handleQuery([]byte(query + "\x00")); err != nil {
					t.Fatalf("handleQuery: %v", err)
				}
			} else {
				c.portals["telemetry-portal"] = &portal{stmt: &preparedStmt{
					query:          query,
					convertedQuery: query,
				}}
				body := append([]byte("telemetry-portal"), 0)
				body = append(body, 0, 0, 0, 0)
				c.handleExecute(body)
			}

			output := logs.String()
			clientFinishes := requireClientWorkerTelemetryLogCount(t, output, "Client query finished.", 1)
			assertClientWorkerTelemetryAttrs(t, clientFinishes, "outcome=canceled", "sqlstate=57014")
			requireClientWorkerTelemetryEventCount(t, tracker, "query_initiated", 1)
			requireClientWorkerTelemetryEventCount(t, tracker, "query_failed", 0)
		})
	}
}

type compatibilityFallbackTelemetryExecutor struct {
	noopProfiling
	fallbackErr error
}

func (e *compatibilityFallbackTelemetryExecutor) execute(query string) (ExecResult, error) {
	switch {
	case strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "ALTER TABLE"):
		return nil, errors.New("Binder Error: cannot use ALTER TABLE on a view because it is not a table")
	case strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "ALTER VIEW"):
		if e.fallbackErr != nil {
			return nil, e.fallbackErr
		}
		return emptyExecResult{}, nil
	default:
		return nil, errors.New("unexpected compatibility fallback query: " + query)
	}
}

func (e *compatibilityFallbackTelemetryExecutor) QueryContext(context.Context, string, ...any) (RowSet, error) {
	return nil, errors.New("not implemented")
}
func (e *compatibilityFallbackTelemetryExecutor) ExecContext(_ context.Context, query string, _ ...any) (ExecResult, error) {
	return e.execute(query)
}
func (e *compatibilityFallbackTelemetryExecutor) Query(string, ...any) (RowSet, error) {
	return nil, errors.New("not implemented")
}
func (e *compatibilityFallbackTelemetryExecutor) Exec(query string, _ ...any) (ExecResult, error) {
	return e.execute(query)
}
func (*compatibilityFallbackTelemetryExecutor) ConnContext(context.Context) (RawConn, error) {
	return nil, errors.New("not implemented")
}
func (*compatibilityFallbackTelemetryExecutor) PingContext(context.Context) error { return nil }
func (*compatibilityFallbackTelemetryExecutor) Close() error                      { return nil }

func TestClientWorkerTelemetrySeparatesCompatibilityFallbackWorkerStatement(t *testing.T) {
	logs := captureClientWorkerTelemetryLogs(t)
	tracker := installClientWorkerTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.username = "telemetry-user"
	c.orgID = "telemetry-org"
	c.executor = &compatibilityFallbackTelemetryExecutor{}
	const query = "ALTER TABLE some_view RENAME TO renamed_view"

	if err := c.handleQuery([]byte(query + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	output := logs.String()
	requireClientWorkerTelemetryLogCount(t, output, "Client query started.", 1)
	clientFinishes := requireClientWorkerTelemetryLogCount(t, output, "Client query finished.", 1)
	assertClientWorkerTelemetryAttrs(t, clientFinishes, "outcome=success")

	clientWorkerStarts := clientWorkerTelemetryLogLinesWithAttr(output, "Worker statement started.", "origin=client")
	clientWorkerFinishes := clientWorkerTelemetryLogLinesWithAttr(output, "Worker statement finished.", "origin=client")
	if got := len(clientWorkerStarts); got != 1 {
		t.Fatalf("client-derived worker starts = %d, want 1; logs:\n%s", got, output)
	}
	if got := len(clientWorkerFinishes); got != 1 {
		t.Fatalf("client-derived worker finishes = %d, want 1; logs:\n%s", got, output)
	}
	assertClientWorkerTelemetryAttrs(t, clientWorkerFinishes, "query=", "error_code=", "error=")

	fallbackStarts := clientWorkerTelemetryLogLinesWithAttr(output, "Worker statement started.", "origin=rewrite")
	fallbackFinishes := clientWorkerTelemetryLogLinesWithAttr(output, "Worker statement finished.", "origin=rewrite")
	if got := len(fallbackStarts); got != 1 {
		t.Fatalf("fallback worker starts = %d, want 1; logs:\n%s", got, output)
	}
	if got := len(fallbackFinishes); got != 1 {
		t.Fatalf("fallback worker finishes = %d, want 1; logs:\n%s", got, output)
	}
	assertClientWorkerTelemetryAttrs(t, fallbackStarts, "operation=compatibility_fallback")
	assertClientWorkerTelemetryAttrs(t, fallbackFinishes, "operation=compatibility_fallback")
	for _, line := range append(fallbackStarts, fallbackFinishes...) {
		if strings.Contains(line, "query=") || strings.Contains(line, "ALTER VIEW") {
			t.Errorf("generated fallback worker event leaked rewritten SQL:\n%s", line)
		}
	}
	requireClientWorkerTelemetryEventCount(t, tracker, "query_initiated", 1)
	requireClientWorkerTelemetryEventCount(t, tracker, "query_failed", 0)
}

func TestClientWorkerTelemetryKeepsFallbackFailureDetailsOutOfLogicalTelemetry(t *testing.T) {
	logs := captureClientWorkerTelemetryLogs(t)
	tracker := installClientWorkerTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	const generatedMarker = "generated-compatibility-rewrite-marker"
	const query = "ALTER TABLE some_view RENAME TO renamed_view"
	sink := &copyFallbackTelemetryQueryLogSink{}
	c.server.queryLogSink = sink
	c.executor = &compatibilityFallbackTelemetryExecutor{
		fallbackErr: errors.New("Binder Error: " + generatedMarker + " in ALTER VIEW generated_internal_view"),
	}

	if err := c.handleQuery([]byte(query + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	output := logs.String()
	clientFinishes := requireClientWorkerTelemetryLogCount(t, output, "Client query finished.", 1)
	assertClientWorkerTelemetryAttrs(t, clientFinishes, "outcome=error")
	if strings.Contains(output, generatedMarker) {
		t.Errorf("logical telemetry leaked generated fallback details:\n%s", output)
	}
	if got := len(sink.entries); got != 1 {
		t.Fatalf("durable query-log entries = %d, want 1", got)
	} else if strings.Contains(sink.entries[0].Exception, generatedMarker) {
		t.Errorf("durable query log leaked generated fallback details: %#v", sink.entries[0])
	}
	requireClientWorkerTelemetryEventCount(t, tracker, "query_initiated", 1)
	requireClientWorkerTelemetryEventCount(t, tracker, "query_failed", 1)
}

func TestDirectRewriteKeepsGeneratedSQLAndErrorsOutOfLogicalTelemetry(t *testing.T) {
	const query = "USE ducklake"
	const generatedMarker = "direct-rewrite-sensitive-marker"

	tests := []struct {
		name     string
		protocol string
		run      func(*testing.T, *clientConn, bool)
	}{
		{
			name:     "simple",
			protocol: "simple",
			run: func(t *testing.T, c *clientConn, _ bool) {
				t.Helper()
				if err := c.handleQuery([]byte(query + "\x00")); err != nil {
					t.Fatalf("handleQuery: %v", err)
				}
			},
		},
		{
			name:     "extended",
			protocol: "extended",
			run: func(t *testing.T, c *clientConn, _ bool) {
				t.Helper()
				c.portals["direct-rewrite-portal"] = &portal{stmt: &preparedStmt{
					query:          query,
					convertedQuery: "USE ducklake.main",
					workerOrigin:   workerOriginRewrite,
				}}
				body := append([]byte("direct-rewrite-portal"), 0)
				body = append(body, 0, 0, 0, 0)
				c.handleExecute(body)
			},
		},
		{
			name:     "simple_batch_statement",
			protocol: "simple-batch",
			run: func(t *testing.T, c *clientConn, wantFailure bool) {
				t.Helper()
				errSent, err := c.executeSingleStatement(query)
				if err != nil {
					t.Fatalf("executeSingleStatement: %v", err)
				}
				if errSent != wantFailure {
					t.Fatalf("executeSingleStatement errSent = %t, want %t", errSent, wantFailure)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, outcome := range []struct {
				name        string
				wantFailure bool
			}{
				{name: "success"},
				{name: "failure", wantFailure: true},
			} {
				t.Run(outcome.name, func(t *testing.T) {
					logs := captureClientWorkerTelemetryLogs(t)
					c, cleanup := newLifecycleClientConn(t)
					defer cleanup()

					c.catalogUseRewrite = true
					sink := &copyFallbackTelemetryQueryLogSink{}
					c.server.queryLogSink = sink
					if outcome.wantFailure {
						c.executor = &lifecycleExecutor{execErr: errors.New("Binder Error: " + generatedMarker + " in USE ducklake.main")}
					} else {
						c.executor = &lifecycleExecutor{execResult: emptyExecResult{}}
					}

					tc.run(t, c, outcome.wantFailure)

					if strings.Contains(logs.String(), "USE ducklake.main") {
						t.Errorf("logical telemetry leaked generated direct-rewrite SQL:\n%s", logs.String())
					}
					if got := len(sink.entries); got != 1 {
						t.Fatalf("durable query-log entries = %d, want 1", got)
					}
					entry := sink.entries[0]
					if entry.Query != query || entry.Protocol != tc.protocol {
						t.Errorf("durable entry = %#v, want original %s query", entry, tc.protocol)
					}
					if entry.TranspiledQuery != nil || entry.IsTranspiled {
						t.Errorf("durable entry persisted generated direct-rewrite SQL: %#v", entry)
					}
					if outcome.wantFailure {
						if entry.ExceptionCode == "" {
							t.Errorf("durable failure entry = %#v, want SQLSTATE", entry)
						}
						if strings.Contains(entry.Exception, generatedMarker) {
							t.Errorf("durable query log leaked generated direct-rewrite details: %#v", entry)
						}
						if strings.Contains(logs.String(), generatedMarker) {
							t.Errorf("logical telemetry leaked generated direct-rewrite details:\n%s", logs.String())
						}
					}
				})
			}
		})
	}
}

func TestMultiStatementRewriteKeepsOneLogicalDurableRecord(t *testing.T) {
	const query = "ALTER TABLE telemetry_target ADD COLUMN x INT, ADD COLUMN y TEXT"

	t.Run("simple", func(t *testing.T) {
		c, cleanup := newLifecycleClientConn(t)
		defer cleanup()
		executor := &lifecycleExecutor{execResult: emptyExecResult{}}
		c.executor = executor
		c.server.cfg.AlwaysDuckLake = true
		sink := &copyFallbackTelemetryQueryLogSink{}
		c.server.queryLogSink = sink

		if err := c.handleQuery([]byte(query + "\x00")); err != nil {
			t.Fatalf("handleQuery: %v", err)
		}
		if got := executor.execCalls.Load(); got < 3 {
			t.Fatalf("rewrite exec calls = %d, want multiple generated statements", got)
		}
		if got := len(sink.entries); got != 1 {
			t.Fatalf("durable query-log entries = %d, want 1", got)
		}
		entry := sink.entries[0]
		if entry.Query != query || entry.Protocol != "simple" {
			t.Errorf("durable entry = %#v, want original simple query", entry)
		}
		if entry.TranspiledQuery != nil || entry.IsTranspiled {
			t.Errorf("durable entry persisted generated rewrite SQL: %#v", entry)
		}
	})

	t.Run("extended", func(t *testing.T) {
		c, cleanup := newLifecycleClientConn(t)
		defer cleanup()
		c.executor = &lifecycleExecutor{execResult: emptyExecResult{}}
		sink := &copyFallbackTelemetryQueryLogSink{}
		c.server.queryLogSink = sink
		c.portals["rewrite-portal"] = &portal{stmt: &preparedStmt{
			query: query,
			statements: []string{
				"BEGIN",
				"ALTER TABLE telemetry_target ADD COLUMN IF NOT EXISTS x INT",
				"ALTER TABLE telemetry_target ADD COLUMN IF NOT EXISTS y INT",
			},
			cleanupStatements: []string{"COMMIT"},
		}}

		body := append([]byte("rewrite-portal"), 0)
		body = append(body, 0, 0, 0, 0)
		c.handleExecute(body)

		if got := len(sink.entries); got != 1 {
			t.Fatalf("durable query-log entries = %d, want 1", got)
		}
		entry := sink.entries[0]
		if entry.Query != query || entry.Protocol != "extended" {
			t.Errorf("durable entry = %#v, want original extended query", entry)
		}
		if entry.TranspiledQuery != nil || entry.IsTranspiled {
			t.Errorf("durable entry persisted generated rewrite SQL: %#v", entry)
		}
	})
}

func TestMultiStatementStreamingCancellationKeepsWorkerSQLState(t *testing.T) {
	for _, protocol := range []string{"simple", "extended"} {
		t.Run(protocol, func(t *testing.T) {
			logs := captureClientWorkerTelemetryLogs(t)
			c, cleanup := newLifecycleClientConn(t)
			defer cleanup()

			c.ctx, c.cancel = context.WithCancel(context.Background())
			c.cancel()
			c.executor = &lifecycleExecutor{queryRows: &cancellationMetadataRowSet{}}

			if protocol == "simple" {
				if err := c.executeMultiStatement(time.Now(), "WITH rewritten AS (SELECT 1) SELECT * FROM rewritten", []string{"SELECT 1"}, nil); err != nil {
					t.Fatalf("executeMultiStatement: %v", err)
				}
			} else {
				c.executeMultiStatementExtended(time.Now(), "WITH rewritten AS (SELECT 1) SELECT * FROM rewritten", []string{"SELECT 1"}, nil, nil, nil, false)
			}

			workerFinishes := clientWorkerTelemetryLogLinesWithAttr(logs.String(), "Worker statement finished.", "operation=rewrite_final")
			if got := len(workerFinishes); got != 1 {
				t.Fatalf("rewrite-final worker finishes = %d, want 1; logs:\n%s", got, logs.String())
			}
			assertClientWorkerTelemetryAttrs(t, workerFinishes, "error_code=57014")
		})
	}
}

type multiStatementContextExecutor struct {
	noopProfiling
	execCalls        int
	execContextCalls int
}

func (e *multiStatementContextExecutor) QueryContext(ctx context.Context, _ string, _ ...any) (RowSet, error) {
	return nil, ctx.Err()
}
func (e *multiStatementContextExecutor) ExecContext(ctx context.Context, _ string, _ ...any) (ExecResult, error) {
	e.execContextCalls++
	return nil, ctx.Err()
}
func (*multiStatementContextExecutor) Query(string, ...any) (RowSet, error) {
	return nil, errors.New("multi-statement rewrite used bare Query")
}
func (e *multiStatementContextExecutor) Exec(string, ...any) (ExecResult, error) {
	e.execCalls++
	return emptyExecResult{}, nil
}
func (*multiStatementContextExecutor) ConnContext(context.Context) (RawConn, error) {
	return nil, errors.New("not implemented")
}
func (*multiStatementContextExecutor) PingContext(context.Context) error { return nil }
func (*multiStatementContextExecutor) Close() error                      { return nil }

func TestMultiStatementRewriteUsesLogicalQueryContext(t *testing.T) {
	logs := captureClientWorkerTelemetryLogs(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.cancel()
	executor := &multiStatementContextExecutor{}
	c.executor = executor

	if err := c.executeMultiStatement(time.Now(), "ALTER TABLE telemetry_target ADD COLUMN value INT", []string{"ALTER TABLE telemetry_target ADD COLUMN value INT"}, nil); err != nil {
		t.Fatalf("executeMultiStatement: %v", err)
	}
	if executor.execCalls != 0 {
		t.Fatalf("bare generated Exec calls = %d, want 0", executor.execCalls)
	}
	if executor.execContextCalls != 1 {
		t.Fatalf("context-aware generated Exec calls = %d, want 1", executor.execContextCalls)
	}
	workerFinishes := clientWorkerTelemetryLogLinesWithAttr(logs.String(), "Worker statement finished.", "operation=rewrite_final")
	if got := len(workerFinishes); got != 1 {
		t.Fatalf("rewrite-final worker finishes = %d, want 1; logs:\n%s", got, logs.String())
	}
	assertClientWorkerTelemetryAttrs(t, workerFinishes, "error_code=57014")
}

type batchCancellationRowSet struct{}

func (*batchCancellationRowSet) Columns() ([]string, error) {
	return []string{"value"}, nil
}
func (*batchCancellationRowSet) ColumnTypes() ([]ColumnTyper, error) {
	return []ColumnTyper{staticColumnType("INTEGER")}, nil
}
func (*batchCancellationRowSet) Next() bool        { return false }
func (*batchCancellationRowSet) Scan(...any) error { return nil }
func (*batchCancellationRowSet) Close() error      { return nil }
func (*batchCancellationRowSet) Err() error        { return context.Canceled }

func TestSimpleQueryBatchKeepsOneLogicalDurableRecord(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		c, cleanup := newLifecycleClientConn(t)
		defer cleanup()
		c.executor = &lifecycleExecutor{execResult: emptyExecResult{}}
		sink := &copyFallbackTelemetryQueryLogSink{}
		c.server.queryLogSink = sink

		const query = "CREATE TABLE batch_one (value INT); CREATE TABLE batch_two (value INT)"
		if err := c.handleQuery([]byte(query + "\x00")); err != nil {
			t.Fatalf("handleQuery: %v", err)
		}
		if got := len(sink.entries); got != 1 {
			t.Fatalf("durable query-log entries = %d, want 1", got)
		}
		entry := sink.entries[0]
		if entry.Query != query || entry.Protocol != "simple" || entry.ExceptionCode != "" {
			t.Errorf("durable batch entry = %#v, want one successful original query", entry)
		}
	})

	t.Run("late_cancellation", func(t *testing.T) {
		logs := captureClientWorkerTelemetryLogs(t)
		tracker := installClientWorkerTelemetryTracker(t)
		c, cleanup := newLifecycleClientConn(t)
		defer cleanup()
		c.ctx, c.cancel = context.WithCancel(context.Background())
		c.cancel()
		c.executor = &lifecycleExecutor{queryRows: &batchCancellationRowSet{}}
		sink := &copyFallbackTelemetryQueryLogSink{}
		c.server.queryLogSink = sink

		const query = "SELECT 1; SELECT 2"
		if err := c.handleQuery([]byte(query + "\x00")); err != nil {
			t.Fatalf("handleQuery: %v", err)
		}
		if got := len(sink.entries); got != 1 {
			t.Fatalf("durable query-log entries = %d, want 1", got)
		} else if entry := sink.entries[0]; entry.ExceptionCode != "57014" {
			t.Errorf("durable batch SQLSTATE = %q, want 57014", entry.ExceptionCode)
		}
		clientFinishes := requireClientWorkerTelemetryLogCount(t, logs.String(), "Client query finished.", 1)
		assertClientWorkerTelemetryAttrs(t, clientFinishes, "outcome=canceled", "sqlstate=57014")
		requireClientWorkerTelemetryEventCount(t, tracker, "query_initiated", 1)
		requireClientWorkerTelemetryEventCount(t, tracker, "query_failed", 0)
	})
}

type telemetryFailingWriter struct{}

func (telemetryFailingWriter) Write([]byte) (int, error) {
	return 0, errors.New("simulated client write failure")
}

func TestMultiStatementDurableLogReflectsTransportFailure(t *testing.T) {
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()
	c.writer = bufio.NewWriter(telemetryFailingWriter{})
	c.executor = &lifecycleExecutor{execResult: emptyExecResult{}}
	sink := &copyFallbackTelemetryQueryLogSink{}
	c.server.queryLogSink = sink

	if err := c.executeMultiStatement(time.Now(), "ALTER TABLE telemetry_target ADD COLUMN value INT", []string{"ALTER TABLE telemetry_target ADD COLUMN value INT"}, nil); err == nil {
		t.Fatal("executeMultiStatement succeeded despite client write failure")
	}
	if got := len(sink.entries); got != 1 {
		t.Fatalf("durable query-log entries = %d, want 1", got)
	} else if entry := sink.entries[0]; entry.ExceptionCode == "" || entry.Type != "ExceptionWhileProcessing" {
		t.Errorf("durable entry = %#v, want terminal transport exception", entry)
	}
}

package server

import (
	"bytes"
	"context"
	"log/slog"
	"regexp"
	"strings"
	"testing"

	"github.com/posthog/duckgres/internal/analytics"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type clientQueryTelemetryEvent struct {
	event string
	orgID string
	props map[string]any
}

type clientQueryTelemetryTracker struct {
	events []clientQueryTelemetryEvent
}

func (t *clientQueryTelemetryTracker) Capture(event, orgID string, props map[string]any) {
	copyProps := make(map[string]any, len(props))
	for key, value := range props {
		copyProps[key] = value
	}
	t.events = append(t.events, clientQueryTelemetryEvent{
		event: event,
		orgID: orgID,
		props: copyProps,
	})
}

func (*clientQueryTelemetryTracker) Close() {}

func installClientQueryTelemetryTracker(t *testing.T) *clientQueryTelemetryTracker {
	t.Helper()

	tracker := &clientQueryTelemetryTracker{}
	previous := analytics.Default()
	analytics.SetDefault(tracker)
	t.Cleanup(func() { analytics.SetDefault(previous) })
	return tracker
}

// captureClientWorkerTelemetryLogs is also used by conn_results_test.go.
func captureClientWorkerTelemetryLogs(t *testing.T) *bytes.Buffer {
	t.Helper()

	previous := slog.Default()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

func installClientQueryTelemetryTracing(t *testing.T) {
	t.Helper()

	previous := otel.GetTracerProvider()
	provider := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
}

func telemetryLogLines(output, message string) []string {
	needle := `msg="` + message + `"`
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if strings.Contains(line, needle) {
			lines = append(lines, line)
		}
	}
	return lines
}

func telemetryLogLinesWithAttr(output, message, attr string) []string {
	var lines []string
	for _, line := range telemetryLogLines(output, message) {
		if strings.Contains(line, attr) {
			lines = append(lines, line)
		}
	}
	return lines
}

func requireTelemetryLogCount(t *testing.T, output, message string, want int) []string {
	t.Helper()

	lines := telemetryLogLines(output, message)
	if got := len(lines); got != want {
		t.Errorf("%q log count = %d, want %d; logs:\n%s", message, got, want, output)
	}
	return lines
}

func assertTelemetryAttrs(t *testing.T, lines []string, attrs ...string) {
	t.Helper()

	for _, line := range lines {
		for _, attr := range attrs {
			if !strings.Contains(line, attr) {
				t.Errorf("log line missing %q:\n%s", attr, line)
			}
		}
	}
}

func requireSingleClientReceived(t *testing.T, output string) []string {
	t.Helper()

	received := requireTelemetryLogCount(t, output, "Client query received.", 1)
	requireTelemetryLogCount(t, output, "Client query started.", 0)
	requireTelemetryLogCount(t, output, "Client query finished.", 0)
	return received
}

func telemetryEvents(tracker *clientQueryTelemetryTracker, event string) []clientQueryTelemetryEvent {
	var events []clientQueryTelemetryEvent
	for _, captured := range tracker.events {
		if captured.event == event {
			events = append(events, captured)
		}
	}
	return events
}

func requireTelemetryEventCount(t *testing.T, tracker *clientQueryTelemetryTracker, event string, want int) []clientQueryTelemetryEvent {
	t.Helper()

	events := telemetryEvents(tracker, event)
	if got := len(events); got != want {
		t.Errorf("%q analytics event count = %d, want %d", event, got, want)
	}
	return events
}

var telemetryTraceID = regexp.MustCompile(`(?:^|\s)trace_id=("[^"]*"|\S+)`)

func traceIDFromTelemetryLog(t *testing.T, line string) string {
	t.Helper()

	match := telemetryTraceID.FindStringSubmatch(line)
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

func TestClientQueryReceivedSimpleSeparatesWorkerLifecycle(t *testing.T) {
	logs := captureClientWorkerTelemetryLogs(t)
	tracker := installClientQueryTelemetryTracker(t)
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
	received := requireSingleClientReceived(t, output)
	workerStarts := requireTelemetryLogCount(t, output, "Worker statement started.", 1)
	workerFinishes := requireTelemetryLogCount(t, output, "Worker statement finished.", 1)
	assertTelemetryAttrs(t, received, "scope=client", "protocol=simple", `query="`+query+`"`)
	assertTelemetryAttrs(t, workerStarts, "scope=worker", "origin=client")
	assertTelemetryAttrs(t, workerFinishes, "scope=worker", "origin=client")

	initiated := requireTelemetryEventCount(t, tracker, "query_initiated", 1)
	if len(initiated) == 1 && initiated[0].orgID != c.orgID {
		t.Errorf("query_initiated org = %q, want %q", initiated[0].orgID, c.orgID)
	}
}

func TestClientQueryReceivedExtendedUsesQueryTrace(t *testing.T) {
	installClientQueryTelemetryTracing(t)
	logs := captureClientWorkerTelemetryLogs(t)
	tracker := installClientQueryTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.username = "telemetry-user"
	c.orgID = "telemetry-org"
	c.executor = &lifecycleExecutor{execResult: emptyExecResult{}}
	const query = "UPDATE client_trace_target SET value = 1"
	const convertedQuery = "UPDATE worker_trace_target SET value = 1"
	c.portals["telemetry-portal"] = &portal{stmt: &preparedStmt{
		query:          query,
		convertedQuery: convertedQuery,
	}}
	body := append([]byte("telemetry-portal"), 0)
	body = append(body, 0, 0, 0, 0)

	c.handleExecute(body)

	output := logs.String()
	received := requireSingleClientReceived(t, output)
	workerStarts := requireTelemetryLogCount(t, output, "Worker statement started.", 1)
	workerFinishes := requireTelemetryLogCount(t, output, "Worker statement finished.", 1)
	assertTelemetryAttrs(t, received, "scope=client", "protocol=extended", `query="`+query+`"`)
	assertTelemetryAttrs(t, workerStarts, "scope=worker", "origin=transpiled", `query="`+convertedQuery+`"`)
	assertTelemetryAttrs(t, workerFinishes, "scope=worker", "origin=transpiled", `query="`+convertedQuery+`"`)
	if len(received) == 1 && strings.Contains(received[0], convertedQuery) {
		t.Errorf("client event contains worker SQL:\n%s", received[0])
	}
	for _, line := range append(workerStarts, workerFinishes...) {
		if strings.Contains(line, query) {
			t.Errorf("worker event contains original client SQL:\n%s", line)
		}
	}
	initiated := requireTelemetryEventCount(t, tracker, "query_initiated", 1)

	if len(received) != 1 || len(workerStarts) != 1 || len(workerFinishes) != 1 || len(initiated) != 1 {
		return
	}
	receivedTraceID := traceIDFromTelemetryLog(t, received[0])
	workerStartTraceID := traceIDFromTelemetryLog(t, workerStarts[0])
	workerFinishTraceID := traceIDFromTelemetryLog(t, workerFinishes[0])
	analyticsTraceID, ok := initiated[0].props["trace_id"].(string)
	if !ok || analyticsTraceID == "" {
		t.Errorf("query_initiated trace_id = %#v, want non-empty string", initiated[0].props["trace_id"])
		return
	}
	if receivedTraceID != workerStartTraceID || receivedTraceID != workerFinishTraceID || receivedTraceID != analyticsTraceID {
		t.Errorf("trace IDs = received %q, worker start %q, worker finish %q, analytics %q; want one query trace",
			receivedTraceID, workerStartTraceID, workerFinishTraceID, analyticsTraceID)
	}
}

func TestClientQueryReceivedExtendedDirectRewriteClassifiedAsRewrite(t *testing.T) {
	logs := captureClientWorkerTelemetryLogs(t)
	tracker := installClientQueryTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.catalogUseRewrite = true
	executor := &passthroughRecordingExecutor{}
	c.executor = executor

	const query = "USE ducklake"
	parseBody := append([]byte("rewrite-stmt\x00"+query+"\x00"), 0, 0)
	c.handleParse(parseBody)
	if _, ok := c.stmts["rewrite-stmt"]; !ok {
		t.Fatalf("Parse did not retain the rewritten statement")
	}

	c.handleBind(bindMessageBody(
		"rewrite-portal",
		"rewrite-stmt",
		int16(0), // parameter format count
		int16(0), // parameter count
		int16(0), // result format count
	))
	if _, ok := c.portals["rewrite-portal"]; !ok {
		t.Fatalf("Bind did not create the rewrite portal")
	}

	executeBody := append([]byte("rewrite-portal"), 0)
	executeBody = append(executeBody, 0, 0, 0, 0)
	c.handleExecute(executeBody)

	output := logs.String()
	received := requireSingleClientReceived(t, output)
	assertTelemetryAttrs(t, received, "scope=client", "protocol=extended", `query="`+query+`"`)
	workerStarts := requireTelemetryLogCount(t, output, "Worker statement started.", 1)
	workerFinishes := requireTelemetryLogCount(t, output, "Worker statement finished.", 1)
	workerLines := append(workerStarts, workerFinishes...)
	assertTelemetryAttrs(t, workerLines, "scope=worker", "origin=rewrite", "operation=execute")
	for _, line := range workerLines {
		if strings.Contains(line, " query=") || strings.Contains(line, "ducklake.main") {
			t.Errorf("generated direct rewrite exposed worker SQL:\n%s", line)
		}
	}
	if len(executor.execQueries) == 0 || executor.execQueries[len(executor.execQueries)-1] != "USE ducklake.main" {
		t.Fatalf("last executor query = %q, want %q", executor.execQueries, "USE ducklake.main")
	}
	requireTelemetryEventCount(t, tracker, "query_initiated", 1)
}

func TestCopyOutWorkerTelemetryDistinguishesQueryAndTableSources(t *testing.T) {
	tests := []struct {
		name          string
		outerQuery    string
		workerQuery   string
		wantOrigin    string
		wantSource    string
		wantQueryText bool
	}{
		{
			name:          "query source logs client-derived worker SQL",
			outerQuery:    "COPY (SELECT 1 AS copy_query_marker) TO STDOUT",
			workerQuery:   "SELECT 1 AS copy_query_marker",
			wantOrigin:    "client",
			wantSource:    "query",
			wantQueryText: true,
		},
		{
			name:        "table source keeps generated SQL private",
			outerQuery:  "COPY copy_table_private_marker TO STDOUT",
			workerQuery: "SELECT * FROM copy_table_private_marker",
			wantOrigin:  "copy",
			wantSource:  "table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := captureClientWorkerTelemetryLogs(t)
			c, _, cleanup := newFeedbackClientConn(t)
			defer cleanup()
			c.passthrough = true

			var executedQuery string
			c.executor = &feedbackExecutor{queryFn: func(query string, _ ...any) (RowSet, error) {
				executedQuery = query
				return &feedbackErrRows{}, nil
			}}

			if err := c.handleCopyOut(tt.outerQuery, strings.ToUpper(tt.outerQuery)); err != nil {
				t.Fatalf("handleCopyOut: %v", err)
			}
			if executedQuery != tt.workerQuery {
				t.Fatalf("executor query = %q, want %q", executedQuery, tt.workerQuery)
			}

			workerStarts := requireTelemetryLogCount(t, logs.String(), "Worker statement started.", 1)
			workerFinishes := requireTelemetryLogCount(t, logs.String(), "Worker statement finished.", 1)
			workerLines := append(workerStarts, workerFinishes...)
			assertTelemetryAttrs(t, workerLines,
				"scope=worker",
				"origin="+tt.wantOrigin,
				"operation=copy_out_select",
				"source_kind="+tt.wantSource,
			)
			for _, line := range workerLines {
				if strings.Contains(line, tt.outerQuery) {
					t.Errorf("worker event exposed outer COPY query:\n%s", line)
				}
				if tt.wantQueryText {
					if !strings.Contains(line, `query="`+tt.workerQuery+`"`) {
						t.Errorf("worker event omitted query-source SQL:\n%s", line)
					}
				} else if strings.Contains(line, " query=") || strings.Contains(line, "copy_table_private_marker") {
					t.Errorf("generated table-source event exposed SQL or identifiers:\n%s", line)
				}
			}
		})
	}
}

func TestNestedBeginDoesNotLogWorkerStatement(t *testing.T) {
	t.Run("extended", func(t *testing.T) {
		logs := captureClientWorkerTelemetryLogs(t)
		tracker := installClientQueryTelemetryTracker(t)
		c, cleanup := newLifecycleClientConn(t)
		defer cleanup()

		c.txStatus = txStatusTransaction
		c.executor = &lifecycleExecutor{execResult: emptyExecResult{}}
		c.portals["nested-begin"] = &portal{stmt: &preparedStmt{query: "BEGIN", convertedQuery: "BEGIN"}}
		body := append([]byte("nested-begin"), 0)
		body = append(body, 0, 0, 0, 0)

		c.handleExecute(body)

		requireSingleClientReceived(t, logs.String())
		requireTelemetryLogCount(t, logs.String(), "Worker statement started.", 0)
		requireTelemetryLogCount(t, logs.String(), "Worker statement finished.", 0)
		requireTelemetryEventCount(t, tracker, "query_initiated", 1)
	})

	t.Run("simple batch statement", func(t *testing.T) {
		logs := captureClientWorkerTelemetryLogs(t)
		c, cleanup := newLifecycleClientConn(t)
		defer cleanup()

		c.txStatus = txStatusTransaction
		c.executor = &lifecycleExecutor{execResult: emptyExecResult{}}
		errSent, fatalErr := c.executeSingleStatement("BEGIN")
		if errSent || fatalErr != nil {
			t.Fatalf("executeSingleStatement(BEGIN) = (%v, %v), want (false, nil)", errSent, fatalErr)
		}
		requireTelemetryLogCount(t, logs.String(), "Worker statement started.", 0)
		requireTelemetryLogCount(t, logs.String(), "Worker statement finished.", 0)
	})
}

func TestCompatibilityFallbackLogsGeneratedWorkerStatement(t *testing.T) {
	logs := captureClientWorkerTelemetryLogs(t)
	tracker := installClientQueryTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	const originalQuery = "ALTER TABLE some_view RENAME TO renamed_view"
	const fallbackQuery = "ALTER VIEW some_view RENAME TO renamed_view"
	c.executor = &abortedExecAlterViewRecoveryExecutor{
		originalQuery: originalQuery,
		rewritten:     fallbackQuery,
	}

	if err := c.handleQuery([]byte(originalQuery + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	var fallbackLines []string
	for _, message := range []string{"Worker statement started.", "Worker statement finished."} {
		for _, line := range telemetryLogLines(logs.String(), message) {
			if strings.Contains(line, "operation=compatibility_fallback") {
				fallbackLines = append(fallbackLines, line)
			}
		}
	}
	if len(fallbackLines) != 2 {
		t.Fatalf("compatibility fallback log lines = %d, want start and finish; logs:\n%s", len(fallbackLines), logs.String())
	}
	assertTelemetryAttrs(t, fallbackLines, "scope=worker", "origin=rewrite", "operation=compatibility_fallback")
	for _, line := range fallbackLines {
		if strings.Contains(line, fallbackQuery) || strings.Contains(line, " query=") {
			t.Errorf("generated fallback event exposed SQL:\n%s", line)
		}
	}
	requireTelemetryEventCount(t, tracker, "query_initiated", 1)
}

func TestClientQueryReceivedServerHandledHasNoWorker(t *testing.T) {
	logs := captureClientWorkerTelemetryLogs(t)
	tracker := installClientQueryTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.username = "telemetry-user"
	c.orgID = "telemetry-org"
	const query = "SHOW duckgres.query_source"

	if err := c.handleQuery([]byte(query + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	output := logs.String()
	received := requireSingleClientReceived(t, output)
	assertTelemetryAttrs(t, received, "scope=client", "protocol=simple", `query="`+query+`"`)
	requireTelemetryLogCount(t, output, "Worker statement started.", 0)
	requireTelemetryLogCount(t, output, "Worker statement finished.", 0)
	requireTelemetryEventCount(t, tracker, "query_initiated", 1)
}

func TestRejectedQueryIsNotNewlyLogged(t *testing.T) {
	logs := captureClientWorkerTelemetryLogs(t)
	tracker := installClientQueryTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.queryAccessPolicy = &QueryAccessPolicy{ReadOnly: true}
	const query = "CREATE SECRET rejected_secret (TYPE S3, KEY_ID 'private-key-marker', SECRET 'private-secret-marker')"

	if err := c.handleQuery([]byte(query + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	output := logs.String()
	requireTelemetryLogCount(t, output, "Client query received.", 0)
	requireTelemetryLogCount(t, output, "Worker statement started.", 0)
	requireTelemetryLogCount(t, output, "Worker statement finished.", 0)
	requireTelemetryEventCount(t, tracker, "query_initiated", 0)
	for _, marker := range []string{"private-key-marker", "private-secret-marker"} {
		if strings.Contains(output, marker) {
			t.Errorf("rejected query leaked %q:\n%s", marker, output)
		}
	}
}

func TestClientQueryReceivedBoundsAndRedactsText(t *testing.T) {
	t.Run("bounded", func(t *testing.T) {
		logs := captureClientWorkerTelemetryLogs(t)
		tracker := installClientQueryTelemetryTracker(t)
		c, cleanup := newLifecycleClientConn(t)
		defer cleanup()

		c.executor = &lifecycleExecutor{execResult: emptyExecResult{}}
		const tailMarker = "client-query-tail-marker"
		query := "UPDATE telemetry_bound SET value = 1 /*" + strings.Repeat("q", maxQueryLength) + tailMarker + "*/"

		if err := c.handleQuery([]byte(query + "\x00")); err != nil {
			t.Fatalf("handleQuery: %v", err)
		}

		received := requireSingleClientReceived(t, logs.String())
		if len(received) == 1 && strings.Contains(received[0], tailMarker) {
			t.Errorf("client query event retained text beyond the bound:\n%s", received[0])
		}
		requireTelemetryEventCount(t, tracker, "query_initiated", 1)
	})

	t.Run("redacted", func(t *testing.T) {
		logs := captureClientWorkerTelemetryLogs(t)
		tracker := installClientQueryTelemetryTracker(t)
		c, cleanup := newLifecycleClientConn(t)
		defer cleanup()

		const query = "CREATE SECRET safe_name (TYPE S3, KEY_ID 'private-key-marker', SECRET 'private-secret-marker')"
		c.logClientQueryReceived(context.Background(), "simple", query)

		output := logs.String()
		received := requireSingleClientReceived(t, output)
		assertTelemetryAttrs(t, received, "scope=client", "protocol=simple", "(…redacted)")
		for _, marker := range []string{"private-key-marker", "private-secret-marker"} {
			if strings.Contains(output, marker) {
				t.Errorf("client query telemetry leaked %q:\n%s", marker, output)
			}
		}
		requireTelemetryEventCount(t, tracker, "query_initiated", 1)
	})
}

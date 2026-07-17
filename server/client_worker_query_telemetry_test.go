package server

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"testing"

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

package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/posthog/duckgres/internal/analytics"
	"github.com/posthog/duckgres/server/wire"
)

// copyFallbackTelemetryTracker records product analytics independently of the
// existing lifecycle tests, because this regression asserts that generated
// binary-COPY batches never count as client query attempts.
type copyFallbackTelemetryTracker struct {
	events []copyFallbackTelemetryEvent
}

type copyFallbackTelemetryEvent struct {
	event string
	orgID string
	props map[string]any
}

func (t *copyFallbackTelemetryTracker) Capture(event, orgID string, props map[string]any) {
	copyProps := make(map[string]any, len(props))
	for key, value := range props {
		copyProps[key] = value
	}
	t.events = append(t.events, copyFallbackTelemetryEvent{
		event: event,
		orgID: orgID,
		props: copyProps,
	})
}

func (*copyFallbackTelemetryTracker) Close() {}

func installCopyFallbackTelemetryTracker(t *testing.T) *copyFallbackTelemetryTracker {
	t.Helper()

	tracker := &copyFallbackTelemetryTracker{}
	previous := analytics.Default()
	analytics.SetDefault(tracker)
	t.Cleanup(func() { analytics.SetDefault(previous) })
	return tracker
}

func captureCopyFallbackTelemetryLogs(t *testing.T) *bytes.Buffer {
	t.Helper()

	previous := slog.Default()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

func copyFallbackTelemetryLogLines(output, message string) []string {
	needle := `msg="` + message + `"`
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if strings.Contains(line, needle) {
			lines = append(lines, line)
		}
	}
	return lines
}

func copyFallbackTelemetryLogLinesWithAttr(output, message, attr string) []string {
	var lines []string
	for _, line := range copyFallbackTelemetryLogLines(output, message) {
		if strings.Contains(line, attr) {
			lines = append(lines, line)
		}
	}
	return lines
}

func copyFallbackTelemetryLogPreview(output string) string {
	const maxPreviewBytes = 4096
	if len(output) <= maxPreviewBytes {
		return output
	}
	return output[:maxPreviewBytes] + "\n... log output truncated ..."
}

func assertCopyFallbackTelemetryLogCount(t *testing.T, output, message string, want int) []string {
	t.Helper()

	lines := copyFallbackTelemetryLogLines(output, message)
	if got := len(lines); got != want {
		t.Errorf("%q log count = %d, want %d; logs:\n%s", message, got, want, copyFallbackTelemetryLogPreview(output))
	}
	return lines
}

func assertCopyFallbackTelemetryAttrs(t *testing.T, line string, attrs ...string) {
	t.Helper()

	for _, attr := range attrs {
		if !strings.Contains(line, attr) {
			t.Errorf("log line missing %q:\n%s", attr, line)
		}
	}
}

func copyFallbackTelemetryEventCount(tracker *copyFallbackTelemetryTracker, event string) int {
	count := 0
	for _, captured := range tracker.events {
		if captured.event == event {
			count++
		}
	}
	return count
}

type copyFallbackTelemetryQueryLogSink struct {
	entries []QueryLogEntry
}

func (s *copyFallbackTelemetryQueryLogSink) Log(entry QueryLogEntry) {
	s.entries = append(s.entries, entry)
}

func (*copyFallbackTelemetryQueryLogSink) StopContext(context.Context) error { return nil }

type copyFallbackTelemetryRowSet struct {
	columns []string
	types   []ColumnTyper
}

func (r *copyFallbackTelemetryRowSet) Columns() ([]string, error) {
	return append([]string(nil), r.columns...), nil
}

func (r *copyFallbackTelemetryRowSet) ColumnTypes() ([]ColumnTyper, error) {
	return append([]ColumnTyper(nil), r.types...), nil
}

func (*copyFallbackTelemetryRowSet) Next() bool        { return false }
func (*copyFallbackTelemetryRowSet) Scan(...any) error { return nil }
func (*copyFallbackTelemetryRowSet) Close() error      { return nil }
func (*copyFallbackTelemetryRowSet) Err() error        { return nil }

type copyFallbackTelemetryColumnType string

func (t copyFallbackTelemetryColumnType) DatabaseTypeName() string { return string(t) }

type copyFallbackTelemetryExecCall struct {
	query string
	args  []any
}

type copyFallbackTelemetryExecResult struct {
	rows int64
}

func (r copyFallbackTelemetryExecResult) RowsAffected() (int64, error) { return r.rows, nil }

// copyFallbackTelemetryReadObserver snapshots logs when handleCopyInBinary
// first consumes wire bytes. That establishes the client start before payload
// buffering, which is earlier than the first fallback batch Exec.
type copyFallbackTelemetryReadObserver struct {
	reader       *bytes.Reader
	logs         *bytes.Buffer
	firstReadLog string
}

func (r *copyFallbackTelemetryReadObserver) Read(p []byte) (int, error) {
	if r.firstReadLog == "" && r.logs != nil {
		r.firstReadLog = r.logs.String()
	}
	return r.reader.Read(p)
}

// copyFallbackTelemetryExecutor answers the schema probe and records each
// generated fallback batch. The first Exec snapshot proves the client start
// is emitted before binary COPY has finished buffering and reaches a worker.
type copyFallbackTelemetryExecutor struct {
	columns []string
	types   []ColumnTyper
	logs    *bytes.Buffer

	execCalls    []copyFallbackTelemetryExecCall
	firstExecLog string
}

func (e *copyFallbackTelemetryExecutor) QueryContext(_ context.Context, query string, args ...any) (RowSet, error) {
	return e.Query(query, args...)
}

func (e *copyFallbackTelemetryExecutor) ExecContext(_ context.Context, query string, args ...any) (ExecResult, error) {
	return e.Exec(query, args...)
}

func (e *copyFallbackTelemetryExecutor) Query(string, ...any) (RowSet, error) {
	return &copyFallbackTelemetryRowSet{
		columns: append([]string(nil), e.columns...),
		types:   append([]ColumnTyper(nil), e.types...),
	}, nil
}

func (e *copyFallbackTelemetryExecutor) Exec(query string, args ...any) (ExecResult, error) {
	if len(e.execCalls) == 0 && e.logs != nil {
		e.firstExecLog = e.logs.String()
	}

	call := copyFallbackTelemetryExecCall{
		query: query,
		args:  append([]any(nil), args...),
	}
	e.execCalls = append(e.execCalls, call)

	var rows int64
	if len(e.columns) > 0 {
		rows = int64(len(args) / len(e.columns))
	}
	return copyFallbackTelemetryExecResult{rows: rows}, nil
}

func (*copyFallbackTelemetryExecutor) ConnContext(context.Context) (RawConn, error) { return nil, nil }
func (*copyFallbackTelemetryExecutor) PingContext(context.Context) error            { return nil }
func (*copyFallbackTelemetryExecutor) Close() error                                 { return nil }
func (*copyFallbackTelemetryExecutor) LastProfilingOutput() string                  { return "" }

func copyFallbackTelemetryWriteInt16(buf *bytes.Buffer, value int16) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], uint16(value))
	_, _ = buf.Write(encoded[:])
}

func copyFallbackTelemetryWriteInt32(buf *bytes.Buffer, value int32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(value))
	_, _ = buf.Write(encoded[:])
}

func copyFallbackTelemetryBinaryPayload(rowCount, columnCount int, firstValue string) []byte {
	var payload bytes.Buffer
	_, _ = payload.Write([]byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xFF, '\r', '\n', 0x00})
	copyFallbackTelemetryWriteInt32(&payload, 0) // flags
	copyFallbackTelemetryWriteInt32(&payload, 0) // extension area length

	for row := 0; row < rowCount; row++ {
		copyFallbackTelemetryWriteInt16(&payload, int16(columnCount))
		for column := 0; column < columnCount; column++ {
			value := "x"
			if row == 0 && column == 0 {
				value = firstValue
			}
			copyFallbackTelemetryWriteInt32(&payload, int32(len(value)))
			_, _ = payload.WriteString(value)
		}
	}

	copyFallbackTelemetryWriteInt16(&payload, -1) // binary COPY trailer
	return payload.Bytes()
}

// TestBinaryCopyFallbackTelemetrySeparatesClientAndBatchWorkerLifecycles is
// the COPY regression for the logical-vs-physical telemetry split. The column
// subset forces binary COPY down the generated multi-row INSERT fallback, and
// 1,001 rows make the fallback run two batches (the first has 27,000 params).
func TestBinaryCopyFallbackTelemetrySeparatesClientAndBatchWorkerLifecycles(t *testing.T) {
	const (
		batchSize      = 1000
		columnCount    = 27
		rowCount       = batchSize + 1
		argumentMarker = "copy-fallback-argument-value"
	)

	logs := captureCopyFallbackTelemetryLogs(t)
	tracker := installCopyFallbackTelemetryTracker(t)
	queryLogSink := &copyFallbackTelemetryQueryLogSink{}

	columns := make([]string, columnCount)
	types := make([]ColumnTyper, columnCount)
	for i := range columns {
		columns[i] = fmt.Sprintf("column_%02d", i+1)
		types[i] = copyFallbackTelemetryColumnType("VARCHAR")
	}

	query := "COPY telemetry_target (" + strings.Join(columns, ", ") + ") FROM STDIN WITH (FORMAT binary)"
	payload := copyFallbackTelemetryBinaryPayload(rowCount, columnCount, argumentMarker)
	var inbound bytes.Buffer
	if err := wire.WriteMessage(&inbound, wire.MsgCopyData, payload); err != nil {
		t.Fatalf("write CopyData: %v", err)
	}
	if err := wire.WriteMessage(&inbound, wire.MsgCopyDone, nil); err != nil {
		t.Fatalf("write CopyDone: %v", err)
	}
	inboundReader := &copyFallbackTelemetryReadObserver{
		reader: bytes.NewReader(inbound.Bytes()),
		logs:   logs,
	}

	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() {
		_ = serverSide.Close()
		_ = clientSide.Close()
	})

	executor := &copyFallbackTelemetryExecutor{
		columns: columns,
		types:   types,
		logs:    logs,
	}
	c := &clientConn{
		server:   &Server{queryLogSink: queryLogSink},
		conn:     serverSide,
		reader:   bufio.NewReader(inboundReader),
		writer:   bufio.NewWriter(&bytes.Buffer{}),
		executor: executor,
		username: "telemetry-user",
		orgID:    "telemetry-org",
		txStatus: txStatusIdle,
		ctx:      context.Background(),
		cursors:  map[string]*cursorState{},
		portals:  map[string]*portal{},
		stmts:    map[string]*preparedStmt{},
	}

	if err := c.handleQuery([]byte(query + "\x00")); err != nil {
		t.Fatalf("handleQuery(binary COPY): %v", err)
	}

	if got := len(executor.execCalls); got != 2 {
		t.Fatalf("generated fallback Exec calls = %d, want 2", got)
	}
	if got := len(executor.execCalls[0].args); got != batchSize*columnCount {
		t.Fatalf("first fallback argument count = %d, want %d", got, batchSize*columnCount)
	}
	if got := len(executor.execCalls[1].args); got != columnCount {
		t.Fatalf("second fallback argument count = %d, want %d", got, columnCount)
	}
	if got := executor.execCalls[0].args[0]; got != argumentMarker {
		t.Fatalf("first fallback argument = %#v, want %q", got, argumentMarker)
	}
	if !strings.Contains(executor.execCalls[0].query, "$27000") {
		t.Fatalf("first generated fallback SQL did not contain $27000: %q", executor.execCalls[0].query)
	}

	// The logical client start must exist before binary CopyData is read, and
	// still be present before the first generated batch reaches the executor;
	// otherwise an OOM while buffering binary COPY has no observable outer
	// operation.
	if !strings.Contains(inboundReader.firstReadLog, `msg="Client query started."`) {
		t.Errorf("missing client start before binary CopyData buffering; logs at first read:\n%s", copyFallbackTelemetryLogPreview(inboundReader.firstReadLog))
	}
	if !strings.Contains(executor.firstExecLog, `msg="Client query started."`) {
		t.Errorf("missing client start before first fallback batch; logs at first Exec:\n%s", copyFallbackTelemetryLogPreview(executor.firstExecLog))
	}

	output := logs.String()
	clientStarts := assertCopyFallbackTelemetryLogCount(t, output, "Client query started.", 1)
	clientFinishes := assertCopyFallbackTelemetryLogCount(t, output, "Client query finished.", 1)
	assertCopyFallbackTelemetryLogCount(t, output, "Query started.", 0)
	assertCopyFallbackTelemetryLogCount(t, output, "Query finished.", 0)

	for _, line := range clientStarts {
		assertCopyFallbackTelemetryAttrs(t, line, "scope=client", "protocol=simple", `query="`+query+`"`)
	}
	for _, line := range clientFinishes {
		assertCopyFallbackTelemetryAttrs(t, line, "scope=client", "protocol=simple", "outcome=success")
	}

	workerStarts := copyFallbackTelemetryLogLinesWithAttr(output, "Worker statement started.", "origin=copy_fallback")
	workerFinishes := copyFallbackTelemetryLogLinesWithAttr(output, "Worker statement finished.", "origin=copy_fallback")
	if got := len(workerStarts); got != 2 {
		t.Errorf("copy_fallback worker starts = %d, want 2; logs:\n%s", got, copyFallbackTelemetryLogPreview(output))
	}
	if got := len(workerFinishes); got != 2 {
		t.Errorf("copy_fallback worker finishes = %d, want 2; logs:\n%s", got, copyFallbackTelemetryLogPreview(output))
	}

	expectedBatches := []struct {
		rowStart, rowEnd, batchSize, parameterCount string
	}{
		{rowStart: "1", rowEnd: "1000", batchSize: "1000", parameterCount: "27000"},
		{rowStart: "1001", rowEnd: "1001", batchSize: "1", parameterCount: "27"},
	}
	for i, expected := range expectedBatches {
		if i < len(workerStarts) {
			assertCopyFallbackTelemetryAttrs(t, workerStarts[i],
				"scope=worker",
				"origin=copy_fallback",
				"operation=copy_fallback_batch",
				"target_table=telemetry_target",
				"row_start="+expected.rowStart,
				"row_end="+expected.rowEnd,
				"batch_size="+expected.batchSize,
				"column_count=27",
				"parameter_count="+expected.parameterCount,
			)
		}
		if i < len(workerFinishes) {
			assertCopyFallbackTelemetryAttrs(t, workerFinishes[i],
				"scope=worker",
				"origin=copy_fallback",
				"operation=copy_fallback_batch",
				"target_table=telemetry_target",
				"row_start="+expected.rowStart,
				"row_end="+expected.rowEnd,
				"batch_size="+expected.batchSize,
				"column_count=27",
				"parameter_count="+expected.parameterCount,
			)
		}
	}

	for _, line := range append(workerStarts, workerFinishes...) {
		for _, forbidden := range []string{
			"query=",
			"sql=",
			"statement=",
			"$27000",
			"column_01, column_02",
			argumentMarker,
		} {
			if strings.Contains(line, forbidden) {
				t.Errorf("generated COPY fallback worker event leaked %q:\n%s", forbidden, line)
			}
		}
		if strings.Contains(strings.ToUpper(line), "INSERT INTO") || strings.Contains(strings.ToUpper(line), "VALUES") {
			t.Errorf("generated COPY fallback worker event leaked generated SQL:\n%s", line)
		}
	}

	if got := copyFallbackTelemetryEventCount(tracker, "query_initiated"); got != 1 {
		t.Errorf("query_initiated analytics events = %d, want 1", got)
	}
	if got := copyFallbackTelemetryEventCount(tracker, "query_failed"); got != 0 {
		t.Errorf("query_failed analytics events = %d, want 0", got)
	}

	if got := len(queryLogSink.entries); got != 1 {
		t.Errorf("durable query-log entries = %d, want 1", got)
	} else if entry := queryLogSink.entries[0]; entry.Query != query || entry.Protocol != "simple" {
		t.Errorf("durable query-log entry = %#v, want one outer simple COPY", entry)
	}
}

type copySchemaProbeFailureRowSet struct {
	stage  string
	closed bool
}

func (r *copySchemaProbeFailureRowSet) Columns() ([]string, error) {
	if r.stage == "columns" {
		return nil, errors.New("schema probe columns failed")
	}
	return []string{"value"}, nil
}

func (r *copySchemaProbeFailureRowSet) ColumnTypes() ([]ColumnTyper, error) {
	if r.stage == "column types" {
		return nil, errors.New("schema probe column types failed")
	}
	return []ColumnTyper{copyFallbackTelemetryColumnType("VARCHAR")}, nil
}

func (*copySchemaProbeFailureRowSet) Next() bool        { return false }
func (*copySchemaProbeFailureRowSet) Scan(...any) error { return nil }
func (r *copySchemaProbeFailureRowSet) Close() error {
	r.closed = true
	if r.stage == "close" {
		return errors.New("schema probe close failed")
	}
	return nil
}
func (*copySchemaProbeFailureRowSet) Err() error { return nil }

func TestCopySchemaProbeMetadataFailureStopsCopyAndFailsWorkerTelemetry(t *testing.T) {
	for _, stage := range []string{"columns", "column types", "close"} {
		t.Run(stage, func(t *testing.T) {
			logs := captureCopyFallbackTelemetryLogs(t)
			c, cleanup := newLifecycleClientConn(t)
			defer cleanup()

			// A failed probe must terminate before the handler reads COPY data.
			c.reader = bufio.NewReader(bytes.NewReader(nil))
			c.writer = bufio.NewWriter(&bytes.Buffer{})
			probeRows := &copySchemaProbeFailureRowSet{stage: stage}
			executor := &lifecycleExecutor{queryRows: probeRows}
			c.executor = executor
			sink := &copyFallbackTelemetryQueryLogSink{}
			c.server.queryLogSink = sink

			const query = "COPY telemetry_target FROM STDIN"
			if err := c.handleCopyIn(query, strings.ToUpper(query)); err != nil {
				t.Fatalf("handleCopyIn: %v", err)
			}
			if !probeRows.closed {
				t.Fatal("schema probe rows were not closed")
			}
			if got := executor.execCalls.Load(); got != 0 {
				t.Fatalf("COPY load exec calls = %d, want 0 after schema probe failure", got)
			}
			if got := len(sink.entries); got != 1 {
				t.Fatalf("durable query-log entries = %d, want 1", got)
			}

			probeFinishes := copyFallbackTelemetryLogLinesWithAttr(logs.String(), "Worker statement finished.", "operation=copy_schema_probe")
			if got := len(probeFinishes); got != 1 {
				t.Fatalf("schema-probe worker finishes = %d, want 1; logs:\n%s", got, logs.String())
			}
			assertCopyFallbackTelemetryAttrs(t, probeFinishes[0], "scope=worker", "origin=copy", "error_code=", "error=")
			if strings.Contains(logs.String(), "operation=copy_from_stdin_") {
				t.Errorf("COPY continued to a load worker after failed schema probe:\n%s", logs.String())
			}
		})
	}
}

func TestCopySchemaProbeInfrastructureCancellationHidesGeneratedDetails(t *testing.T) {
	logs := captureCopyFallbackTelemetryLogs(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	const query = "COPY telemetry_target FROM STDIN"
	const generatedMarker = "copy-schema-probe-generated-marker"
	c.reader = bufio.NewReader(bytes.NewReader(nil))
	c.executor = &lifecycleExecutor{
		queryErr: fmt.Errorf("worker failed running SELECT * FROM telemetry_target LIMIT 0: %s: %w", generatedMarker, context.Canceled),
	}
	sink := &copyFallbackTelemetryQueryLogSink{}
	c.server.queryLogSink = sink

	if err := c.handleQuery([]byte(query + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	if got := len(sink.entries); got != 1 {
		t.Fatalf("durable query-log entries = %d, want 1", got)
	}
	entry := sink.entries[0]
	if entry.ExceptionCode != "57014" {
		t.Errorf("durable query log SQLSTATE = %q, want 57014", entry.ExceptionCode)
	}
	if strings.Contains(entry.Exception, generatedMarker) {
		t.Errorf("durable query log leaked generated schema-probe details: %#v", entry)
	}
	if strings.Contains(logs.String(), generatedMarker) {
		t.Errorf("logical telemetry leaked generated schema-probe details:\n%s", logs.String())
	}
}

func TestCopyLocalSpoolFailureDoesNotLeakTempPathToTelemetryOrDurableHistory(t *testing.T) {
	tempPath := filepath.Join(t.TempDir(), "missing-copy-temp-directory")
	t.Setenv("TMPDIR", tempPath)
	if got := os.TempDir(); got != tempPath {
		t.Fatalf("os.TempDir() = %q, want %q", got, tempPath)
	}

	logs := captureCopyFallbackTelemetryLogs(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()
	c.executor = &lifecycleExecutor{queryRows: &copyFallbackTelemetryRowSet{
		columns: []string{"value"},
		types:   []ColumnTyper{copyFallbackTelemetryColumnType("VARCHAR")},
	}}
	sink := &copyFallbackTelemetryQueryLogSink{}
	c.server.queryLogSink = sink

	const query = "COPY telemetry_target FROM STDIN"
	if err := c.handleQuery([]byte(query + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	output := logs.String()
	if strings.Contains(output, tempPath) {
		t.Errorf("telemetry leaked local COPY spool path %q:\n%s", tempPath, output)
	}
	if got := len(sink.entries); got != 1 {
		t.Fatalf("durable query-log entries = %d, want 1", got)
	}
	if strings.Contains(sink.entries[0].Exception, tempPath) {
		t.Errorf("durable query log leaked local COPY spool path %q: %#v", tempPath, sink.entries[0])
	}
}

type copyOutPartialScanErrorRowSet struct {
	next    int
	scanErr error
}

func (*copyOutPartialScanErrorRowSet) Columns() ([]string, error) {
	return []string{"value"}, nil
}
func (*copyOutPartialScanErrorRowSet) ColumnTypes() ([]ColumnTyper, error) {
	return []ColumnTyper{copyFallbackTelemetryColumnType("VARCHAR")}, nil
}
func (r *copyOutPartialScanErrorRowSet) Next() bool {
	r.next++
	return r.next <= 2
}
func (r *copyOutPartialScanErrorRowSet) Scan(dest ...any) error {
	if r.next == 2 {
		if r.scanErr != nil {
			return r.scanErr
		}
		return errors.New("copy out scan failed")
	}
	if len(dest) > 0 {
		if value, ok := dest[0].(*interface{}); ok {
			*value = "first row"
		}
	}
	return nil
}
func (*copyOutPartialScanErrorRowSet) Close() error { return nil }
func (*copyOutPartialScanErrorRowSet) Err() error   { return nil }

func TestCopyOutWorkerTelemetryKeepsPartialRowsOnScanFailure(t *testing.T) {
	logs := captureCopyFallbackTelemetryLogs(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()
	c.executor = &lifecycleExecutor{queryRows: &copyOutPartialScanErrorRowSet{}}

	const query = "COPY telemetry_target TO STDOUT"
	if err := c.handleCopyOut(query, strings.ToUpper(query)); err != nil {
		t.Fatalf("handleCopyOut: %v", err)
	}

	workerFinishes := copyFallbackTelemetryLogLinesWithAttr(logs.String(), "Worker statement finished.", "operation=copy_out_select")
	if got := len(workerFinishes); got != 1 {
		t.Fatalf("copy-out worker finishes = %d, want 1; logs:\n%s", got, logs.String())
	}
	assertCopyFallbackTelemetryAttrs(t, workerFinishes[0], "scope=worker", "origin=copy", "rows=1", "error_code=", "error=")
}

func TestCopyOutCancellationRemainsCanceledAtTheLogicalBoundary(t *testing.T) {
	logs := captureCopyFallbackTelemetryLogs(t)
	tracker := installCopyFallbackTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.cancel()
	c.executor = &lifecycleExecutor{queryRows: &copyOutPartialScanErrorRowSet{scanErr: context.Canceled}}
	sink := &copyFallbackTelemetryQueryLogSink{}
	c.server.queryLogSink = sink

	const query = "COPY telemetry_target TO STDOUT"
	if err := c.handleQuery([]byte(query + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	clientFinishes := assertCopyFallbackTelemetryLogCount(t, logs.String(), "Client query finished.", 1)
	assertCopyFallbackTelemetryAttrs(t, clientFinishes[0], "scope=client", "outcome=canceled", "sqlstate=57014")
	workerFinishes := copyFallbackTelemetryLogLinesWithAttr(logs.String(), "Worker statement finished.", "operation=copy_out_select")
	if got := len(workerFinishes); got != 1 {
		t.Fatalf("copy-out worker finishes = %d, want 1; logs:\n%s", got, logs.String())
	}
	assertCopyFallbackTelemetryAttrs(t, workerFinishes[0], "error_code=57014")
	if got := len(sink.entries); got != 1 {
		t.Fatalf("durable query-log entries = %d, want 1", got)
	} else if entry := sink.entries[0]; entry.ExceptionCode != "57014" {
		t.Errorf("durable query log SQLSTATE = %q, want 57014", entry.ExceptionCode)
	}
	if got := copyFallbackTelemetryEventCount(tracker, "query_initiated"); got != 1 {
		t.Errorf("query_initiated analytics events = %d, want 1", got)
	}
	if got := copyFallbackTelemetryEventCount(tracker, "query_failed"); got != 0 {
		t.Errorf("query_failed analytics events = %d, want 0", got)
	}
}

type copyOutCloseCancellationRowSet struct{}

func (*copyOutCloseCancellationRowSet) Columns() ([]string, error) {
	return []string{"value"}, nil
}
func (*copyOutCloseCancellationRowSet) ColumnTypes() ([]ColumnTyper, error) {
	return []ColumnTyper{copyFallbackTelemetryColumnType("VARCHAR")}, nil
}
func (*copyOutCloseCancellationRowSet) Next() bool        { return false }
func (*copyOutCloseCancellationRowSet) Scan(...any) error { return nil }
func (*copyOutCloseCancellationRowSet) Close() error      { return context.Canceled }
func (*copyOutCloseCancellationRowSet) Err() error        { return nil }

func TestCopyOutCloseCancellationRemainsCanceledAtTheLogicalBoundary(t *testing.T) {
	logs := captureCopyFallbackTelemetryLogs(t)
	tracker := installCopyFallbackTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.cancel()
	c.executor = &lifecycleExecutor{queryRows: &copyOutCloseCancellationRowSet{}}
	sink := &copyFallbackTelemetryQueryLogSink{}
	c.server.queryLogSink = sink

	const query = "COPY telemetry_target TO STDOUT"
	if err := c.handleQuery([]byte(query + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	clientFinishes := assertCopyFallbackTelemetryLogCount(t, logs.String(), "Client query finished.", 1)
	assertCopyFallbackTelemetryAttrs(t, clientFinishes[0], "scope=client", "outcome=canceled", "sqlstate=57014")
	workerFinishes := copyFallbackTelemetryLogLinesWithAttr(logs.String(), "Worker statement finished.", "operation=copy_out_select")
	if got := len(workerFinishes); got != 1 {
		t.Fatalf("copy-out worker finishes = %d, want 1; logs:\n%s", got, logs.String())
	}
	assertCopyFallbackTelemetryAttrs(t, workerFinishes[0], "error_code=57014")
	if got := len(sink.entries); got != 1 {
		t.Fatalf("durable query-log entries = %d, want 1", got)
	} else if entry := sink.entries[0]; entry.ExceptionCode != "57014" {
		t.Errorf("durable query log SQLSTATE = %q, want 57014", entry.ExceptionCode)
	}
	if got := copyFallbackTelemetryEventCount(tracker, "query_failed"); got != 0 {
		t.Errorf("query_failed analytics events = %d, want 0", got)
	}
}

func TestBinaryCopyFallbackCancellationRemainsCanceledAtTheLogicalBoundary(t *testing.T) {
	tracker := installCopyFallbackTelemetryTracker(t)
	logs := captureCopyFallbackTelemetryLogs(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	var inbound bytes.Buffer
	payload := copyFallbackTelemetryBinaryPayload(1, 1, "x")
	if err := wire.WriteMessage(&inbound, wire.MsgCopyData, payload); err != nil {
		t.Fatalf("write CopyData: %v", err)
	}
	if err := wire.WriteMessage(&inbound, wire.MsgCopyDone, nil); err != nil {
		t.Fatalf("write CopyDone: %v", err)
	}
	c.reader = bufio.NewReader(bytes.NewReader(inbound.Bytes()))
	c.writer = bufio.NewWriter(&bytes.Buffer{})
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.cancel()
	c.executor = &lifecycleExecutor{
		queryRows: &copyFallbackTelemetryRowSet{
			columns: []string{"value"},
			types:   []ColumnTyper{copyFallbackTelemetryColumnType("VARCHAR")},
		},
		execErr: context.Canceled,
	}
	sink := &copyFallbackTelemetryQueryLogSink{}
	c.server.queryLogSink = sink

	const query = "COPY telemetry_target (value) FROM STDIN WITH (FORMAT binary)"
	if err := c.handleQuery([]byte(query + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	clientFinishes := assertCopyFallbackTelemetryLogCount(t, logs.String(), "Client query finished.", 1)
	assertCopyFallbackTelemetryAttrs(t, clientFinishes[0], "scope=client", "outcome=canceled", "sqlstate=57014")
	if got := len(sink.entries); got != 1 {
		t.Fatalf("durable query-log entries = %d, want 1", got)
	} else if entry := sink.entries[0]; entry.ExceptionCode != "57014" {
		t.Errorf("durable query log SQLSTATE = %q, want 57014", entry.ExceptionCode)
	}
	if got := copyFallbackTelemetryEventCount(tracker, "query_initiated"); got != 1 {
		t.Errorf("query_initiated analytics events = %d, want 1", got)
	}
	if got := copyFallbackTelemetryEventCount(tracker, "query_failed"); got != 0 {
		t.Errorf("query_failed analytics events = %d, want 0", got)
	}
}

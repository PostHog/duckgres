package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
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

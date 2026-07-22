package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/posthog/duckgres/server/wire"
)

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
// generated fallback batch.
type copyFallbackTelemetryExecutor struct {
	columns []string
	types   []ColumnTyper

	execCalls int
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
	e.execCalls++

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
// 1,001 rows make the fallback run two batches.
func TestBinaryCopyFallbackTelemetrySeparatesClientAndBatchWorkerLifecycles(t *testing.T) {
	const (
		batchSize      = 1000
		columnCount    = 2
		rowCount       = batchSize + 1
		argumentMarker = "copy-fallback-argument-value"
	)

	logs := captureClientWorkerTelemetryLogs(t)
	tracker := installClientQueryTelemetryTracker(t)

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
	}
	c := &clientConn{
		server:   &Server{},
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

	if got := executor.execCalls; got != 2 {
		t.Fatalf("generated fallback Exec calls = %d, want 2", got)
	}

	// The logical client receipt must exist before binary CopyData is read.
	if !strings.Contains(inboundReader.firstReadLog, `msg="Client query received."`) {
		t.Errorf("missing client receipt before binary CopyData buffering; logs at first read:\n%s", inboundReader.firstReadLog)
	}

	output := logs.String()
	clientReceipts := requireTelemetryLogCount(t, output, "Client query received.", 1)
	assertTelemetryAttrs(t, clientReceipts, "scope=client", "protocol=simple", `query="`+query+`"`)

	workerStarts := telemetryLogLinesWithAttr(output, "Worker statement started.", "origin=copy_fallback")
	workerFinishes := telemetryLogLinesWithAttr(output, "Worker statement finished.", "origin=copy_fallback")
	if got := len(workerStarts); got != 2 {
		t.Errorf("copy_fallback worker starts = %d, want 2; logs:\n%s", got, output)
	}
	if got := len(workerFinishes); got != 2 {
		t.Errorf("copy_fallback worker finishes = %d, want 2; logs:\n%s", got, output)
	}

	for _, line := range append(workerStarts, workerFinishes...) {
		assertTelemetryAttrs(t, []string{line}, "scope=worker", "origin=copy_fallback", "operation=copy_fallback_batch")
		for _, forbidden := range []string{
			"query=",
			"target_table=",
			"sql=",
			"statement=",
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

	requireTelemetryEventCount(t, tracker, "query_initiated", 1)
}

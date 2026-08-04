package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/posthog/duckgres/server/wire"
)

// These tests cover the extended-query protocol's skip-until-Sync error
// recovery (issue #718): after an error while processing any extended-query
// message, the server must discard subsequent extended-protocol messages
// until Sync arrives, then respond ReadyForQuery. Without it, pipelined
// clients (libpq pipeline mode, pgx SendBatch, JDBC batch) execute queued
// messages against broken state and desync their response accounting.

// badParseSQL fails Parse deterministically: the pg_catalog reference forces
// the transpiler through pg_query (no Tier-0 direct passthrough), which
// cannot parse "SELEC", so handleParse falls back to DuckDB validation —
// whose EXPLAIN probe the test executor rejects.
const badParseSQL = "SELEC oid FROM pg_catalog.pg_class"

// pipelineRecordingExecutor fails all Query paths and EXPLAIN probes (so a
// Parse of unparseable SQL that falls back to DuckDB validation errors
// deterministically) and records every other Exec so tests can assert which
// statements actually executed.
type pipelineRecordingExecutor struct {
	noopProfiling
	execCalls []string
}

func (e *pipelineRecordingExecutor) exec(query string) (ExecResult, error) {
	query = strings.TrimSpace(query)
	if strings.HasPrefix(strings.ToUpper(query), "EXPLAIN") {
		// validateWithDuckDB probe for SQL pg_query couldn't parse.
		return nil, errors.New("Parser Error: syntax error at or near \"SELEC\"")
	}
	e.execCalls = append(e.execCalls, query)
	return &fakeExecResult{}, nil
}

func (e *pipelineRecordingExecutor) QueryContext(context.Context, string, ...any) (RowSet, error) {
	return nil, errors.New("Parser Error: syntax error at or near \"SELEC\"")
}

func (e *pipelineRecordingExecutor) ExecContext(_ context.Context, query string, _ ...any) (ExecResult, error) {
	return e.exec(query)
}

func (e *pipelineRecordingExecutor) Query(string, ...any) (RowSet, error) {
	return nil, errors.New("Parser Error: syntax error at or near \"SELEC\"")
}

func (e *pipelineRecordingExecutor) Exec(query string, _ ...any) (ExecResult, error) {
	return e.exec(query)
}

func (e *pipelineRecordingExecutor) ConnContext(context.Context) (RawConn, error) {
	return nil, errors.New("not implemented")
}

func (e *pipelineRecordingExecutor) PingContext(context.Context) error { return nil }

func (e *pipelineRecordingExecutor) Close() error { return nil }

// pgFrame builds a framed wire-protocol message: type byte + int32 length
// (including itself) + body.
func pgFrame(msgType byte, body []byte) []byte {
	frame := make([]byte, 0, 5+len(body))
	frame = append(frame, msgType)
	frame = binary.BigEndian.AppendUint32(frame, uint32(4+len(body)))
	return append(frame, body...)
}

func parseFrame(stmtName, query string) []byte {
	var body bytes.Buffer
	body.WriteString(stmtName)
	body.WriteByte(0)
	body.WriteString(query)
	body.WriteByte(0)
	_ = binary.Write(&body, binary.BigEndian, int16(0)) // no parameter types
	return pgFrame(wire.MsgParse, body.Bytes())
}

func bindFrame(portalName, stmtName string) []byte {
	var body bytes.Buffer
	body.WriteString(portalName)
	body.WriteByte(0)
	body.WriteString(stmtName)
	body.WriteByte(0)
	_ = binary.Write(&body, binary.BigEndian, int16(0)) // no param format codes
	_ = binary.Write(&body, binary.BigEndian, int16(0)) // no param values
	_ = binary.Write(&body, binary.BigEndian, int16(0)) // no result format codes
	return pgFrame(wire.MsgBind, body.Bytes())
}

func describePortalFrame(portalName string) []byte {
	var body bytes.Buffer
	body.WriteByte('P')
	body.WriteString(portalName)
	body.WriteByte(0)
	return pgFrame(wire.MsgDescribe, body.Bytes())
}

func executeFrame(portalName string) []byte {
	var body bytes.Buffer
	body.WriteString(portalName)
	body.WriteByte(0)
	_ = binary.Write(&body, binary.BigEndian, int32(0)) // no row limit
	return pgFrame(wire.MsgExecute, body.Bytes())
}

func queryFrame(query string) []byte {
	return pgFrame(wire.MsgQuery, append([]byte(query), 0))
}

type wireFrame struct {
	msgType byte
	body    []byte
}

// scanWireFrames splits the server's output buffer into framed messages.
func scanWireFrames(t *testing.T, buf []byte) []wireFrame {
	t.Helper()
	var frames []wireFrame
	for i := 0; i < len(buf); {
		if i+5 > len(buf) {
			t.Fatalf("truncated frame header at offset %d (%d bytes total)", i, len(buf))
		}
		length := int(binary.BigEndian.Uint32(buf[i+1 : i+5]))
		if length < 4 || i+1+length > len(buf) {
			t.Fatalf("malformed frame at offset %d: type=%c length=%d", i, buf[i], length)
		}
		frames = append(frames, wireFrame{msgType: buf[i], body: buf[i+5 : i+1+length]})
		i += 1 + length
	}
	return frames
}

func frameTypes(frames []wireFrame) string {
	var b strings.Builder
	for _, f := range frames {
		b.WriteByte(f.msgType)
	}
	return b.String()
}

// runPipeline feeds a pipelined client byte stream (terminated with a
// Terminate frame) through messageLoop and returns the parsed output frames.
func runPipeline(t *testing.T, c *clientConn, out *bytes.Buffer, frames ...[]byte) []wireFrame {
	t.Helper()
	stream := bytes.Join(frames, nil)
	stream = append(stream, pgFrame(wire.MsgTerminate, nil)...)
	c.reader = bufio.NewReader(bytes.NewReader(stream))
	if err := c.messageLoop(); err != nil {
		t.Fatalf("messageLoop returned error: %v", err)
	}
	_ = c.writer.Flush()
	return scanWireFrames(t, out.Bytes())
}

func newPipelineConn(t *testing.T, executor QueryExecutor) (*clientConn, *bytes.Buffer) {
	t.Helper()
	clientSide, serverSide := net.Pipe()
	t.Cleanup(func() {
		_ = clientSide.Close()
		_ = serverSide.Close()
	})

	var out bytes.Buffer
	c := &clientConn{
		server:   &Server{activeQueries: make(map[BackendKey]context.CancelFunc)},
		conn:     clientSide,
		writer:   bufio.NewWriter(&out),
		ctx:      context.Background(),
		cancel:   func() {},
		txStatus: txStatusIdle,
		executor: executor,
		stmts:    make(map[string]*preparedStmt),
		portals:  make(map[string]*portal),
	}
	return c, &out
}

// TestExtendedQueryErrorDiscardsPipelineUntilSync is the regression test for
// issue #718: after a failed Parse, the queued Bind/Describe/Execute of a
// previously prepared (valid) statement must be discarded — not executed —
// until Sync, which responds ReadyForQuery and resumes normal processing.
func TestExtendedQueryErrorDiscardsPipelineUntilSync(t *testing.T) {
	executor := &pipelineRecordingExecutor{}
	c, out := newPipelineConn(t, executor)

	frames := runPipeline(t, c, out,
		// Prepare a valid statement, complete the round trip.
		parseFrame("s_del", "DELETE FROM t WHERE id = 1"),
		pgFrame(wire.MsgSync, nil),
		// Pipelined batch: Parse fails, the rest must be discarded.
		parseFrame("s_bad", badParseSQL),
		bindFrame("p", "s_del"),
		describePortalFrame("p"),
		executeFrame("p"),
		pgFrame(wire.MsgSync, nil),
		// After Sync, the same Bind/Execute must work normally again.
		bindFrame("p", "s_del"),
		executeFrame("p"),
		pgFrame(wire.MsgSync, nil),
	)

	// ParseComplete, ReadyForQuery; exactly one ErrorResponse then
	// ReadyForQuery (no BindComplete/CommandComplete for the discarded
	// messages); BindComplete, CommandComplete, ReadyForQuery.
	if got, want := frameTypes(frames), "1ZEZ2CZ"; got != want {
		t.Fatalf("frame sequence = %q, want %q", got, want)
	}
	if code, ok := errorResponseField(out.Bytes(), 'C'); !ok || code != "42601" {
		t.Fatalf("ErrorResponse SQLSTATE = %q (ok=%v), want 42601", code, ok)
	}

	// The DELETE behind the discarded Execute must have run exactly once —
	// for the post-Sync Execute, not the discarded one.
	var deletes []string
	for _, call := range executor.execCalls {
		if strings.HasPrefix(strings.ToUpper(call), "DELETE") {
			deletes = append(deletes, call)
		}
	}
	if len(deletes) != 1 {
		t.Fatalf("DELETE executed %d times (%v), want exactly once (the post-Sync Execute)", len(deletes), executor.execCalls)
	}
}

// TestAbortedTransactionStillAllowsExtendedRollback pins the recovery
// trigger: it is the error event during extended-query processing, NOT
// txStatus == 'E'. A connection whose transaction is aborted must still
// accept Parse/Bind/Execute of ROLLBACK (real PostgreSQL accepts these in an
// aborted transaction; gating on txStatus would wedge clients).
func TestAbortedTransactionStillAllowsExtendedRollback(t *testing.T) {
	executor := &pipelineRecordingExecutor{}
	c, out := newPipelineConn(t, executor)
	c.txStatus = txStatusError

	frames := runPipeline(t, c, out,
		parseFrame("", "ROLLBACK"),
		bindFrame("", ""),
		executeFrame(""),
		pgFrame(wire.MsgSync, nil),
	)

	if got, want := frameTypes(frames), "12CZ"; got != want {
		t.Fatalf("frame sequence = %q, want %q", got, want)
	}
	if tag := string(bytes.TrimRight(frames[2].body, "\x00")); tag != "ROLLBACK" {
		t.Fatalf("CommandComplete tag = %q, want ROLLBACK", tag)
	}
	if status := frames[3].body[0]; status != txStatusIdle {
		t.Fatalf("ReadyForQuery status = %c, want %c", status, txStatusIdle)
	}
	if got := executor.execCalls; len(got) != 1 || got[0] != "ROLLBACK" {
		t.Fatalf("Exec calls = %v, want [ROLLBACK]", got)
	}
}

// TestSimpleQueryResetsSkipUntilSync: per the audit on #718, a simple Query
// message is processed (not discarded) during error recovery — it resets
// extended-protocol state and its trailing ReadyForQuery resynchronizes the
// client, so subsequent extended-query messages execute normally.
func TestSimpleQueryResetsSkipUntilSync(t *testing.T) {
	executor := &pipelineRecordingExecutor{}
	c, out := newPipelineConn(t, executor)

	frames := runPipeline(t, c, out,
		parseFrame("s_bad", badParseSQL), // error arms skip-until-Sync
		queryFrame(""),                   // simple Query: EmptyQueryResponse + ReadyForQuery
		parseFrame("s_del", "DELETE FROM t WHERE id = 1"),
		bindFrame("p", "s_del"),
		executeFrame("p"),
		pgFrame(wire.MsgSync, nil),
	)

	// ErrorResponse; EmptyQueryResponse + ReadyForQuery from the simple
	// Query; then the extended-query batch runs normally.
	if got, want := frameTypes(frames), "EIZ12CZ"; got != want {
		t.Fatalf("frame sequence = %q, want %q", got, want)
	}
	if len(executor.execCalls) != 1 || !strings.HasPrefix(strings.ToUpper(executor.execCalls[0]), "DELETE") {
		t.Fatalf("Exec calls = %v, want exactly the DELETE", executor.execCalls)
	}
}

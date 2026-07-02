package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
)

// TestQuerySourceGetter covers the session accessor a future compute meter
// reads: unset defaults to "standard", a SET value round-trips, and reset to
// empty falls back to the default.
func TestQuerySourceGetter(t *testing.T) {
	c := &clientConn{}

	// Unset → default "standard" (never an error for a missing value).
	if got := c.QuerySource(); got != "standard" {
		t.Fatalf("unset QuerySource() = %q, want %q", got, "standard")
	}
	if got := c.QuerySource(); got != defaultQuerySource {
		t.Fatalf("unset QuerySource() = %q, want defaultQuerySource %q", got, defaultQuerySource)
	}

	// SET → value round-trips (pass-through: any string is accepted).
	c.setQuerySource("endpoints")
	if got := c.QuerySource(); got != "endpoints" {
		t.Fatalf("after set, QuerySource() = %q, want %q", got, "endpoints")
	}

	c.setQuerySource("whatever")
	if got := c.QuerySource(); got != "whatever" {
		t.Fatalf("after set, QuerySource() = %q, want %q", got, "whatever")
	}

	// RESET / empty → back to default.
	c.setQuerySource("")
	if got := c.QuerySource(); got != "standard" {
		t.Fatalf("after reset, QuerySource() = %q, want %q", got, "standard")
	}
}

// TestQuerySourceStartupOption asserts a `-c duckgres.query_source=...` startup
// option is parsed into the map ParseStartupOptions returns (the value the
// startup handler applies to the session).
func TestQuerySourceStartupOption(t *testing.T) {
	opts := ParseStartupOptions("-c duckgres.query_source=endpoints")
	if got := opts[querySourceGUCName]; got != "endpoints" {
		t.Fatalf("ParseStartupOptions[%q] = %q, want %q", querySourceGUCName, got, "endpoints")
	}
}

// wireMsg is a decoded backend protocol message: a type byte and its body.
type wireMsg struct {
	typ  byte
	body []byte
}

// parseWireMsgs decodes the sequence of length-prefixed backend messages a
// clientConn wrote to its buffer (type byte + 4-byte length incl. length).
func parseWireMsgs(t *testing.T, buf []byte) []wireMsg {
	t.Helper()
	var msgs []wireMsg
	for i := 0; i+5 <= len(buf); {
		typ := buf[i]
		length := int(buf[i+1])<<24 | int(buf[i+2])<<16 | int(buf[i+3])<<8 | int(buf[i+4])
		if length < 4 || i+1+length > len(buf) {
			t.Fatalf("malformed wire message at offset %d (length %d, remaining %d)", i, length, len(buf)-i)
		}
		msgs = append(msgs, wireMsg{typ: typ, body: buf[i+5 : i+1+length]})
		i += 1 + length
	}
	return msgs
}

// newBufferedConn builds a clientConn that writes to an in-memory buffer, for
// asserting the exact backend messages a simple query produces. The returned
// *bytes.Buffer holds the flushed output after handleQuery.
func newBufferedConn(exec QueryExecutor) (*clientConn, *bytes.Buffer) {
	clientSide, serverSide := net.Pipe()
	// The pipe halves are only needed so bufio has something to wrap; the test
	// reads the flushed buffer, not the pipe. Closed by the caller via t.Cleanup.
	_ = serverSide
	var out bytes.Buffer
	c := &clientConn{
		server:   &Server{activeQueries: make(map[BackendKey]context.CancelFunc)},
		conn:     clientSide,
		reader:   bufio.NewReader(clientSide),
		writer:   bufio.NewWriter(&out),
		ctx:      context.Background(),
		cancel:   func() {},
		txStatus: txStatusIdle,
		executor: exec,
	}
	return c, &out
}

// selectOneExecutor answers any QueryContext with a single-row RowSet, so a
// SELECT in a batch alongside the query_source GUC actually executes.
type selectOneExecutor struct {
	noopProfiling
	queryCalls int
}

func (e *selectOneExecutor) QueryContext(context.Context, string, ...any) (RowSet, error) {
	e.queryCalls++
	return &staticCountRowSet{count: 1}, nil
}
func (e *selectOneExecutor) ExecContext(context.Context, string, ...any) (ExecResult, error) {
	return &fakeExecResult{}, nil
}
func (e *selectOneExecutor) Query(string, ...any) (RowSet, error) {
	return nil, errors.New("not implemented")
}
func (e *selectOneExecutor) Exec(string, ...any) (ExecResult, error) {
	return nil, errors.New("not implemented")
}
func (e *selectOneExecutor) ConnContext(context.Context) (RawConn, error) {
	return nil, errors.New("not implemented")
}
func (e *selectOneExecutor) PingContext(context.Context) error { return nil }
func (e *selectOneExecutor) Close() error                      { return nil }

// TestQuerySourceMixedBatchSetThenShow is the regression for the e2e bug: a
// single simple query batching `SET duckgres.query_source='endpoints'; SHOW
// duckgres.query_source` must run BOTH statements in order — the SET
// (session-side) then the SHOW returning the just-set value. Previously the
// whole-batch transpile surfaced QuerySourceSet on the first statement and
// returned early, swallowing the trailing SHOW so it never emitted `endpoints`.
func TestQuerySourceMixedBatchSetThenShow(t *testing.T) {
	c, out := newBufferedConn(&selectOneExecutor{})

	if err := c.handleQuery([]byte("SET duckgres.query_source = 'endpoints'; SHOW duckgres.query_source\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	msgs := parseWireMsgs(t, out.Bytes())

	// Expect: CommandComplete(SET), RowDescription, DataRow(endpoints),
	// CommandComplete(SHOW), ReadyForQuery — in that order.
	var sawSetComplete, sawShowRow, sawShowComplete bool
	for _, m := range msgs {
		switch m.typ {
		case 'C':
			if bytes.Contains(m.body, []byte("SET")) {
				sawSetComplete = true
			}
			if bytes.Contains(m.body, []byte("SHOW")) {
				if !sawShowRow {
					t.Fatalf("SHOW CommandComplete arrived before the SHOW DataRow — the SHOW was dropped")
				}
				sawShowComplete = true
			}
		case 'D':
			if bytes.Contains(m.body, []byte("endpoints")) {
				if !sawSetComplete {
					t.Fatalf("SHOW DataRow arrived before the SET completed — statements ran out of order")
				}
				sawShowRow = true
			}
		}
	}

	if !sawSetComplete {
		t.Fatalf("no CommandComplete(SET) in output; msgs=%s", describeMsgs(msgs))
	}
	if !sawShowRow {
		t.Fatalf("SHOW duckgres.query_source did not return 'endpoints' (trailing statement swallowed); msgs=%s", describeMsgs(msgs))
	}
	if !sawShowComplete {
		t.Fatalf("no CommandComplete(SHOW) in output; msgs=%s", describeMsgs(msgs))
	}
	if got := c.QuerySource(); got != "endpoints" {
		t.Fatalf("session QuerySource() = %q, want %q", got, "endpoints")
	}
}

// TestQuerySourceMixedBatchWithNormalStatement asserts a batch mixing the
// custom GUC with a normal statement (`SET duckgres.query_source='endpoints';
// SELECT 1`) still executes the SELECT — the GUC statement must not swallow the
// rest of the batch.
func TestQuerySourceMixedBatchWithNormalStatement(t *testing.T) {
	exec := &selectOneExecutor{}
	c, out := newBufferedConn(exec)

	if err := c.handleQuery([]byte("SET duckgres.query_source = 'endpoints'; SELECT 1\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	if exec.queryCalls != 1 {
		t.Fatalf("SELECT did not run: QueryContext called %d times, want 1", exec.queryCalls)
	}
	if got := c.QuerySource(); got != "endpoints" {
		t.Fatalf("session QuerySource() = %q, want %q", got, "endpoints")
	}

	msgs := parseWireMsgs(t, out.Bytes())
	var sawSetComplete, sawSelectComplete bool
	for _, m := range msgs {
		if m.typ == 'C' {
			if bytes.Contains(m.body, []byte("SET")) {
				sawSetComplete = true
			}
			if bytes.Contains(m.body, []byte("SELECT")) {
				sawSelectComplete = true
			}
		}
	}
	if !sawSetComplete {
		t.Fatalf("no CommandComplete(SET); msgs=%s", describeMsgs(msgs))
	}
	if !sawSelectComplete {
		t.Fatalf("SELECT 1 after the GUC did not complete; msgs=%s", describeMsgs(msgs))
	}
}

func describeMsgs(msgs []wireMsg) string {
	var b bytes.Buffer
	for _, m := range msgs {
		b.WriteByte(m.typ)
		b.WriteByte('(')
		for _, x := range m.body {
			if x >= 0x20 && x < 0x7f {
				b.WriteByte(x)
			} else {
				b.WriteByte('.')
			}
		}
		b.WriteString(") ")
	}
	return b.String()
}

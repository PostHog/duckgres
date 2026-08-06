package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
)

// Portal suspension (#Hex 1024-row truncation): a client that sends Execute
// with a nonzero row limit (JDBC setFetchSize, Hex's paging driver) must get
// PortalSuspended when the limit is reached with the result set unexhausted,
// and a subsequent Execute on the same portal must resume streaming from
// where the previous one stopped — without re-running the query. Pre-fix,
// duckgres sent CommandComplete after the first page, silently truncating
// every result set to the client's page size.

// newPortalSuspConn builds a clientConn whose wire output is captured in the
// returned buffer, so tests can assert on the exact message sequence sent.
func newPortalSuspConn(t *testing.T) (*clientConn, *bytes.Buffer, func()) {
	t.Helper()
	serverSide, clientSide := net.Pipe()
	out := &bytes.Buffer{}
	ql := &QueryLogger{ch: make(chan QueryLogEntry, 100)}
	srv := &Server{activeQueries: make(map[BackendKey]context.CancelFunc), queryLogger: ql}
	c := &clientConn{
		server:   srv,
		conn:     serverSide,
		reader:   bufio.NewReader(serverSide),
		writer:   bufio.NewWriter(out),
		txStatus: txStatusIdle,
		cursors:  map[string]*cursorState{},
		portals:  map[string]*portal{},
		stmts:    map[string]*preparedStmt{},
		ctx:      context.Background(),
	}
	cleanup := func() {
		_ = serverSide.Close()
		_ = clientSide.Close()
	}
	return c, out, cleanup
}

// portalExecBody builds an Execute message body: portal name + int32 maxRows.
func portalExecBody(portalName string, maxRows int32) []byte {
	b := append([]byte(portalName), 0)
	var mr [4]byte
	binary.BigEndian.PutUint32(mr[:], uint32(maxRows))
	return append(b, mr[:]...)
}

// drainWire parses buffered wire messages into a type sequence (e.g. "TDDs")
// and returns the CommandComplete tags encountered. Resets the buffer.
func drainWire(t *testing.T, c *clientConn, out *bytes.Buffer) (string, []string) {
	t.Helper()
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	data := out.Bytes()
	seq := make([]byte, 0, 8)
	var tags []string
	for len(data) > 0 {
		if len(data) < 5 {
			t.Fatalf("truncated wire message: % x", data)
		}
		msgType := data[0]
		msgLen := binary.BigEndian.Uint32(data[1:5])
		if int(msgLen)+1 > len(data) {
			t.Fatalf("wire message length %d exceeds buffer %d", msgLen, len(data))
		}
		payload := data[5 : 1+msgLen]
		if msgType == 'C' {
			tags = append(tags, string(bytes.TrimRight(payload, "\x00")))
		}
		seq = append(seq, msgType)
		data = data[1+msgLen:]
	}
	out.Reset()
	return string(seq), tags
}

func newSuspensionRowSet(n int) *streamingRowSet {
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{"v"}
	}
	return &streamingRowSet{
		rows:      rows,
		cols:      []string{"c"},
		colTypers: []ColumnTyper{stringColumnTyper{}},
	}
}

func newSuspensionPortal(c *clientConn, name string) {
	stmt := &preparedStmt{
		query:          "SELECT * FROM t",
		convertedQuery: "SELECT * FROM t",
	}
	c.stmts["s1"] = stmt
	c.portals[name] = &portal{stmt: stmt}
}

// TestExecuteMaxRowsSuspendsAndResumes pages a 5-row result set with
// Execute(maxRows=2): 2 rows + PortalSuspended, 2 rows + PortalSuspended,
// then the final row + CommandComplete. The query must execute exactly once.
func TestExecuteMaxRowsSuspendsAndResumes(t *testing.T) {
	c, out, cleanup := newPortalSuspConn(t)
	defer cleanup()

	rs := newSuspensionRowSet(5)
	ex := &lifecycleExecutor{queryRows: rs}
	c.executor = ex
	newSuspensionPortal(c, "p1")

	c.handleExecute(portalExecBody("p1", 2))
	seq, _ := drainWire(t, c, out)
	if seq != "TDDs" {
		t.Fatalf("first Execute: expected RowDescription + 2 DataRows + PortalSuspended (TDDs), got %q", seq)
	}

	c.handleExecute(portalExecBody("p1", 2))
	seq, _ = drainWire(t, c, out)
	if seq != "DDs" {
		t.Fatalf("second Execute: expected 2 DataRows + PortalSuspended (DDs), got %q", seq)
	}

	c.handleExecute(portalExecBody("p1", 2))
	seq, tags := drainWire(t, c, out)
	if seq != "DC" {
		t.Fatalf("final Execute: expected last DataRow + CommandComplete (DC), got %q", seq)
	}
	if len(tags) != 1 || tags[0] != "SELECT 5" {
		t.Errorf("expected cumulative CommandComplete tag \"SELECT 5\", got %v", tags)
	}
	if got := ex.queryCalls.Load(); got != 1 {
		t.Errorf("query must execute exactly once across Execute legs, ran %d times", got)
	}
	if !rs.closed {
		t.Error("rows must be closed after the portal completes")
	}
}

// TestExecuteMaxRowsExactBoundarySuspends matches PostgreSQL semantics when
// the row limit equals the result size: the server can't know the set is
// exhausted, so it suspends; the next Execute returns 0 rows + CommandComplete.
func TestExecuteMaxRowsExactBoundarySuspends(t *testing.T) {
	c, out, cleanup := newPortalSuspConn(t)
	defer cleanup()

	rs := newSuspensionRowSet(4)
	c.executor = &lifecycleExecutor{queryRows: rs}
	newSuspensionPortal(c, "p1")

	c.handleExecute(portalExecBody("p1", 4))
	seq, _ := drainWire(t, c, out)
	if seq != "TDDDDs" {
		t.Fatalf("expected 4 DataRows + PortalSuspended (TDDDDs), got %q", seq)
	}

	c.handleExecute(portalExecBody("p1", 4))
	seq, tags := drainWire(t, c, out)
	if seq != "C" {
		t.Fatalf("expected bare CommandComplete (C), got %q", seq)
	}
	if len(tags) != 1 || tags[0] != "SELECT 4" {
		t.Errorf("expected CommandComplete tag \"SELECT 4\", got %v", tags)
	}
	if !rs.closed {
		t.Error("rows must be closed after the portal completes")
	}
}

// TestExecuteNoMaxRowsStreamsAll guards the common path: maxRows=0 streams
// the whole result set in one Execute with no suspension.
func TestExecuteNoMaxRowsStreamsAll(t *testing.T) {
	c, out, cleanup := newPortalSuspConn(t)
	defer cleanup()

	rs := newSuspensionRowSet(5)
	c.executor = &lifecycleExecutor{queryRows: rs}
	newSuspensionPortal(c, "p1")

	c.handleExecute(portalExecBody("p1", 0))
	seq, tags := drainWire(t, c, out)
	if seq != "TDDDDDC" {
		t.Fatalf("expected all rows + CommandComplete (TDDDDDC), got %q", seq)
	}
	if len(tags) != 1 || tags[0] != "SELECT 5" {
		t.Errorf("expected CommandComplete tag \"SELECT 5\", got %v", tags)
	}
	if !rs.closed {
		t.Error("rows must be closed after completion")
	}
}

// TestCloseSuspendedPortalClosesRows: Close('P') on a suspended portal must
// close its open RowSet — an open rowset pins the session's single DuckDB
// connection (see closeCursorsAtTxEnd).
func TestCloseSuspendedPortalClosesRows(t *testing.T) {
	c, out, cleanup := newPortalSuspConn(t)
	defer cleanup()

	rs := newSuspensionRowSet(5)
	c.executor = &lifecycleExecutor{queryRows: rs}
	newSuspensionPortal(c, "p1")

	c.handleExecute(portalExecBody("p1", 2))
	seq, _ := drainWire(t, c, out)
	if seq != "TDDs" {
		t.Fatalf("expected suspension (TDDs), got %q", seq)
	}

	c.handleClose(append([]byte{'P'}, append([]byte("p1"), 0)...))
	if !rs.closed {
		t.Error("Close('P') on a suspended portal must close its rows")
	}
}

// TestTxEndClosesSuspendedPortalRows: COMMIT/ROLLBACK must release suspended
// portals' rowsets before executing, mirroring closeCursorsAtTxEnd — the open
// rowset holds the session's only DuckDB connection and would deadlock the
// transaction-end statement.
func TestTxEndClosesSuspendedPortalRows(t *testing.T) {
	c, out, cleanup := newPortalSuspConn(t)
	defer cleanup()

	rs := newSuspensionRowSet(5)
	c.executor = &lifecycleExecutor{queryRows: rs, execResult: emptyExecResult{}}
	newSuspensionPortal(c, "p1")

	c.handleExecute(portalExecBody("p1", 2))
	seq, _ := drainWire(t, c, out)
	if seq != "TDDs" {
		t.Fatalf("expected suspension (TDDs), got %q", seq)
	}

	commitStmt := &preparedStmt{query: "COMMIT", convertedQuery: "COMMIT"}
	c.portals["p2"] = &portal{stmt: commitStmt}
	c.handleExecute(portalExecBody("p2", 0))
	if !rs.closed {
		t.Error("COMMIT must close suspended portals' rows before executing")
	}
}

// TestBindOverwriteClosesSuspendedPortalRows: re-Binding a portal name whose
// previous portal is suspended must close the abandoned RowSet.
func TestBindOverwriteClosesSuspendedPortalRows(t *testing.T) {
	c, out, cleanup := newPortalSuspConn(t)
	defer cleanup()

	rs := newSuspensionRowSet(5)
	c.executor = &lifecycleExecutor{queryRows: rs}
	newSuspensionPortal(c, "p1")

	c.handleExecute(portalExecBody("p1", 2))
	seq, _ := drainWire(t, c, out)
	if seq != "TDDs" {
		t.Fatalf("expected suspension (TDDs), got %q", seq)
	}

	// Bind message: portal "p1", statement "s1", 0 param formats, 0 params,
	// 0 result formats.
	bind := append([]byte("p1"), 0)
	bind = append(bind, append([]byte("s1"), 0)...)
	bind = append(bind, 0, 0, 0, 0, 0, 0)
	c.handleBind(bind)
	if !rs.closed {
		t.Error("Bind overwriting a suspended portal must close the old rows")
	}
}

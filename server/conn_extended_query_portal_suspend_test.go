package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"

	"github.com/posthog/duckgres/server/wire"
)

// buildExecuteBody encodes an Execute message body: portal name followed by
// the int32 max_rows field.
func buildExecuteBody(portalName string, maxRows int32) []byte {
	var body bytes.Buffer
	body.WriteString(portalName)
	body.WriteByte(0)
	_ = binary.Write(&body, binary.BigEndian, maxRows)
	return body.Bytes()
}

// countMessages scans a pgwire backend message stream and returns how many
// times each message type byte appears, plus the payload of the last
// CommandComplete ('C') message seen (its tag, sans trailing NUL).
func countMessages(t *testing.T, buf []byte) (counts map[byte]int, lastCommandTag string) {
	t.Helper()
	counts = map[byte]int{}
	r := bytes.NewReader(buf)
	for {
		msgType, data, err := wire.ReadMessage(r)
		if err != nil {
			break
		}
		counts[msgType]++
		if msgType == wire.MsgCommandComplete {
			lastCommandTag = string(bytes.TrimRight(data, "\x00"))
		}
	}
	return counts, lastCommandTag
}

// TestHandleExecutePortalSuspendedResumesWithoutLosingOrDuplicatingRows is the
// regression test for the silent-truncation bug: an Execute whose max_rows
// cap is hit below the result set's true size must send PortalSuspended (not
// CommandComplete) and keep the portal's RowSet open so a subsequent Execute
// resumes and delivers every remaining row exactly once — no row dropped at
// the boundary, none duplicated on resume.
func TestHandleExecutePortalSuspendedResumesWithoutLosingOrDuplicatingRows(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer func() { _ = clientSide.Close() }()
	defer func() { _ = serverSide.Close() }()

	rows := &streamingRowSet{
		rows:      [][]any{{int64(1)}, {int64(2)}, {int64(3)}},
		cols:      []string{"n"},
		colTypers: []ColumnTyper{stringColumnTyper{}},
	}
	executor := &lifecycleExecutor{queryRows: rows}

	var out bytes.Buffer
	c := &clientConn{
		server:   &Server{activeQueries: make(map[BackendKey]context.CancelFunc)},
		conn:     clientSide,
		reader:   bufio.NewReader(clientSide),
		writer:   bufio.NewWriter(&out),
		ctx:      context.Background(),
		cancel:   func() {},
		txStatus: txStatusIdle,
		executor: executor,
		portals: map[string]*portal{
			"p": {
				stmt: &preparedStmt{
					query:          "SELECT n FROM t",
					convertedQuery: "SELECT n FROM t",
				},
			},
		},
	}

	// First Execute caps at 1 row out of 3.
	c.handleExecute(buildExecuteBody("p", 1))
	_ = c.writer.Flush()

	if executor.queryCalls.Load() != 1 {
		t.Fatalf("expected exactly one Query call (no restart), got %d", executor.queryCalls.Load())
	}
	if rows.idx != 1 {
		t.Fatalf("expected the RowSet to be positioned after exactly 1 row (no dropped lookahead row), got idx=%d", rows.idx)
	}
	if rows.closed {
		t.Fatalf("expected the RowSet to stay open across a suspended Execute")
	}
	p := c.portals["p"]
	if p.openRows == nil {
		t.Fatalf("expected the portal to retain the open RowSet for resumption")
	}
	if p.openRowsSent != 1 {
		t.Fatalf("expected openRowsSent=1 after the first Execute, got %d", p.openRowsSent)
	}

	counts1, tag1 := countMessages(t, out.Bytes())
	if counts1[wire.MsgCommandComplete] != 0 {
		t.Fatalf("expected NO CommandComplete on a suspended Execute, got tag %q", tag1)
	}
	if counts1[wire.MsgPortalSuspended] != 1 {
		t.Fatalf("expected exactly one PortalSuspended message, got %d", counts1[wire.MsgPortalSuspended])
	}
	if counts1[wire.MsgDataRow] != 1 {
		t.Fatalf("expected exactly 1 DataRow in the first Execute, got %d", counts1[wire.MsgDataRow])
	}

	// Second Execute (no cap) resumes and must deliver the remaining 2 rows,
	// then a CommandComplete with the row count across BOTH Executes.
	out.Reset()
	c.handleExecute(buildExecuteBody("p", 0))
	_ = c.writer.Flush()

	if executor.queryCalls.Load() != 1 {
		t.Fatalf("expected the resumed Execute to reuse the existing RowSet, not re-run Query; got %d Query calls", executor.queryCalls.Load())
	}
	if !rows.closed {
		t.Fatalf("expected the RowSet to be closed once the portal drains")
	}
	if p.openRows != nil {
		t.Fatalf("expected the portal's openRows to be cleared once drained")
	}

	counts2, tag2 := countMessages(t, out.Bytes())
	if counts2[wire.MsgDataRow] != 2 {
		t.Fatalf("expected exactly 2 DataRows on resume (rows 2 and 3, none dropped/duplicated), got %d", counts2[wire.MsgDataRow])
	}
	if counts2[wire.MsgCommandComplete] != 1 {
		t.Fatalf("expected a CommandComplete on the draining Execute, got %d", counts2[wire.MsgCommandComplete])
	}
	if tag2 != "SELECT 3" {
		t.Fatalf("expected cumulative command tag %q, got %q", "SELECT 3", tag2)
	}
}

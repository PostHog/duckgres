package server

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"
)

type passthroughRecordingExecutor struct {
	noopProfiling
	execQueries []string
}

func (e *passthroughRecordingExecutor) QueryContext(context.Context, string, ...any) (RowSet, error) {
	return nil, nil
}

func (e *passthroughRecordingExecutor) ExecContext(_ context.Context, query string, _ ...any) (ExecResult, error) {
	e.execQueries = append(e.execQueries, query)
	return &fakeExecResult{}, nil
}

func (e *passthroughRecordingExecutor) Query(string, ...any) (RowSet, error) {
	return nil, nil
}

func (e *passthroughRecordingExecutor) Exec(query string, _ ...any) (ExecResult, error) {
	e.execQueries = append(e.execQueries, query)
	return &fakeExecResult{}, nil
}

func (e *passthroughRecordingExecutor) ConnContext(context.Context) (RawConn, error) {
	return nil, nil
}

func (e *passthroughRecordingExecutor) PingContext(context.Context) error {
	return nil
}

func (e *passthroughRecordingExecutor) Close() error {
	return nil
}

type stubConn struct{}

func (stubConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (stubConn) Write(b []byte) (int, error)      { return len(b), nil }
func (stubConn) Close() error                     { return nil }
func (stubConn) LocalAddr() net.Addr              { return stubAddr("local") }
func (stubConn) RemoteAddr() net.Addr             { return stubAddr("remote") }
func (stubConn) SetDeadline(time.Time) error      { return nil }
func (stubConn) SetReadDeadline(time.Time) error  { return nil }
func (stubConn) SetWriteDeadline(time.Time) error { return nil }

type stubAddr string

func (a stubAddr) Network() string { return "tcp" }
func (a stubAddr) String() string  { return string(a) }

func TestExecuteQueryDirectPassthroughPreservesUse(t *testing.T) {
	exec := &passthroughRecordingExecutor{}
	server := &Server{}
	InitMinimalServer(server, Config{
		DuckLake: DuckLakeConfig{
			MetadataStore: "postgres:host=127.0.0.1 dbname=ducklake",
		},
	}, nil)

	c := &clientConn{
		server:     server,
		conn:       stubConn{},
		reader:     bufio.NewReader(bytes.NewBufferString("x")),
		writer:     bufio.NewWriter(io.Discard),
		executor:   exec,
		passthrough:true,
		database:   "test",
	}

	if err := c.executeQueryDirect("USE test", "USE"); err != nil {
		t.Fatalf("executeQueryDirect returned error: %v", err)
	}

	if len(exec.execQueries) != 1 {
		t.Fatalf("expected 1 exec query, got %d", len(exec.execQueries))
	}
	if got := exec.execQueries[0]; got != "USE test" {
		t.Fatalf("passthrough exec query = %q, want %q", got, "USE test")
	}
}

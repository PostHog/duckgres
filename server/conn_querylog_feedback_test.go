package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

type feedbackExecutor struct {
	queryFn func(query string, args ...any) (RowSet, error)
}

func (e *feedbackExecutor) QueryContext(_ context.Context, _ string, _ ...any) (RowSet, error) {
	return nil, errors.New("not implemented")
}

func (e *feedbackExecutor) ExecContext(_ context.Context, _ string, _ ...any) (ExecResult, error) {
	return nil, errors.New("not implemented")
}

func (e *feedbackExecutor) Query(query string, args ...any) (RowSet, error) {
	if e.queryFn == nil {
		return nil, errors.New("not implemented")
	}
	return e.queryFn(query, args...)
}

func (e *feedbackExecutor) Exec(_ string, _ ...any) (ExecResult, error) {
	return nil, errors.New("not implemented")
}

func (e *feedbackExecutor) ConnContext(_ context.Context) (RawConn, error) {
	return nil, errors.New("not implemented")
}

func (e *feedbackExecutor) PingContext(_ context.Context) error {
	return errors.New("not implemented")
}

func (e *feedbackExecutor) Close() error {
	return nil
}

type feedbackErrRows struct {
	err error
}

func (r *feedbackErrRows) Columns() ([]string, error) {
	return []string{"c1"}, nil
}

func (r *feedbackErrRows) ColumnTypes() ([]ColumnTyper, error) {
	return []ColumnTyper{describeColumnType("INTEGER")}, nil
}

func (r *feedbackErrRows) Next() bool {
	return false
}

func (r *feedbackErrRows) Scan(_ ...any) error {
	return nil
}

func (r *feedbackErrRows) Close() error {
	return nil
}

func (r *feedbackErrRows) Err() error {
	return r.err
}

func newFeedbackClientConn(t *testing.T) (*clientConn, *queryLogWAL, func()) {
	t.Helper()

	dir := t.TempDir()
	wal, err := newWAL(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	serverSide, clientSide := net.Pipe()
	out := &bytes.Buffer{}
	ql := &QueryLogger{
		wal:  wal,
		cfg:  QueryLogConfig{BatchSize: 1000},
		done: make(chan struct{}),
		stop: make(chan struct{}),
	}
	srv := &Server{activeQueries: make(map[BackendKey]context.CancelFunc), queryLogger: ql}
	c := &clientConn{
		server:   srv,
		conn:     clientSide,
		writer:   bufio.NewWriter(out),
		txStatus: txStatusIdle,
		cursors:  map[string]*cursorState{},
		portals:  map[string]*portal{},
		ctx:      context.Background(),
	}

	cleanup := func() {
		_ = clientSide.Close()
		_ = serverSide.Close()
		_ = wal.Close()
	}

	return c, wal, cleanup
}

// readWALEntry reads the latest WAL entry, retrying briefly for async writes.
func readWALEntry(t *testing.T, wal *queryLogWAL) QueryLogEntry {
	t.Helper()
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		entries, err := wal.ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) > 0 {
			return entries[len(entries)-1]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("expected query log entry in WAL, got none")
	return QueryLogEntry{} // unreachable
}

func TestHandleFetchCursorLogsMissingCursorError(t *testing.T) {
	c, wal, cleanup := newFeedbackClientConn(t)
	defer cleanup()

	stmt := &pg_query.FetchStmt{
		Portalname: "missing_cursor",
		Direction:  pg_query.FetchDirection_FETCH_FORWARD,
		HowMany:    1,
	}

	if err := c.handleFetchCursor("FETCH 1 FROM missing_cursor", stmt); err != nil {
		t.Fatalf("handleFetchCursor returned error: %v", err)
	}

	entry := readWALEntry(t, wal)
	if entry.ExceptionCode != "34000" {
		t.Fatalf("expected exception code 34000, got %q", entry.ExceptionCode)
	}
	if entry.Type != "ExceptionWhileProcessing" {
		t.Fatalf("expected exception entry type, got %q", entry.Type)
	}
}

func TestHandleCopyOutLogsSyntaxError(t *testing.T) {
	c, wal, cleanup := newFeedbackClientConn(t)
	defer cleanup()

	if err := c.handleCopyOut("COPY TO STDOUT", "COPY TO STDOUT"); err != nil {
		t.Fatalf("handleCopyOut returned error: %v", err)
	}

	entry := readWALEntry(t, wal)
	if entry.ExceptionCode != "42601" {
		t.Fatalf("expected exception code 42601, got %q", entry.ExceptionCode)
	}
	if entry.Type != "ExceptionWhileProcessing" {
		t.Fatalf("expected exception entry type, got %q", entry.Type)
	}
}

func TestHandleExecuteLogsRowsErr(t *testing.T) {
	c, wal, cleanup := newFeedbackClientConn(t)
	defer cleanup()

	c.executor = &feedbackExecutor{
		queryFn: func(_ string, _ ...any) (RowSet, error) {
			return &feedbackErrRows{err: errors.New("row iteration failed")}, nil
		},
	}

	c.portals[""] = &portal{
		stmt: &preparedStmt{
			query:          "SELECT 1",
			convertedQuery: "SELECT 1",
		},
	}

	// Execute body: empty portal name + maxRows=0
	c.handleExecute([]byte{0, 0, 0, 0, 0})

	entry := readWALEntry(t, wal)
	if entry.ExceptionCode != "42000" {
		t.Fatalf("expected exception code 42000, got %q", entry.ExceptionCode)
	}
	if entry.Exception != "row iteration failed" {
		t.Fatalf("expected row iteration error, got %q", entry.Exception)
	}
}

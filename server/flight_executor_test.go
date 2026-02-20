package server

import (
	"bufio"
	"context"
	"errors"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestFlightExecutor_EmptyQuery_QueryContext(t *testing.T) {
	// Empty queries (semicolons, whitespace) should return an empty result set
	// without touching the Flight SQL client. This supports PostgreSQL client pings.
	e := &FlightExecutor{client: nil} // client is nil — would panic if accessed

	tests := []struct {
		name  string
		query string
	}{
		{"empty string", ""},
		{"semicolon", ";"},
		{"semicolons", ";;;"},
		{"semicolon with newline", ";\n"},
		{"whitespace and semicolons", " ; ; "},
		{"just whitespace", "   "},
		{"tabs and semicolons", "\t;\t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := e.QueryContext(context.Background(), tt.query)
			if err != nil {
				t.Fatalf("expected no error for empty query %q, got: %v", tt.query, err)
			}
			if rows == nil {
				t.Fatal("expected non-nil rows")
			}
			if err := rows.Close(); err != nil {
				t.Fatalf("unexpected error closing rows: %v", err)
			}
		})
	}
}

func TestFlightExecutor_EmptyQuery_ExecContext(t *testing.T) {
	e := &FlightExecutor{client: nil}

	tests := []struct {
		name  string
		query string
	}{
		{"empty string", ""},
		{"semicolon", ";"},
		{"semicolon with newline", ";\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := e.ExecContext(context.Background(), tt.query)
			if err != nil {
				t.Fatalf("expected no error for empty query %q, got: %v", tt.query, err)
			}
			affected, err := result.RowsAffected()
			if err != nil {
				t.Fatalf("unexpected error getting rows affected: %v", err)
			}
			if affected != 0 {
				t.Fatalf("expected 0 rows affected, got %d", affected)
			}
		})
	}
}

func TestFlightExecutor_NonEmptyQuery_StillNeedsFlight(t *testing.T) {
	// A real (non-empty) query with nil client should still panic/error.
	e := &FlightExecutor{client: nil}

	_, err := e.QueryContext(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error for non-empty query with nil client")
	}
}

func TestFlightExecutorMarkDead_QueryContext(t *testing.T) {
	// A dead executor should return ErrWorkerDead without touching the client.
	e := &FlightExecutor{} // client is nil — would panic if accessed
	e.MarkDead()

	_, err := e.QueryContext(context.Background(), "SELECT 1")
	if !errors.Is(err, ErrWorkerDead) {
		t.Fatalf("expected ErrWorkerDead, got %v", err)
	}
}

func TestFlightExecutorMarkDead_ExecContext(t *testing.T) {
	e := &FlightExecutor{}
	e.MarkDead()

	_, err := e.ExecContext(context.Background(), "SET x = 1")
	if !errors.Is(err, ErrWorkerDead) {
		t.Fatalf("expected ErrWorkerDead, got %v", err)
	}
}

func TestFlightExecutorMarkDeadIdempotent(t *testing.T) {
	e := &FlightExecutor{}
	e.MarkDead()
	e.MarkDead() // should not panic

	_, err := e.QueryContext(context.Background(), "SELECT 1")
	if !errors.Is(err, ErrWorkerDead) {
		t.Fatalf("expected ErrWorkerDead after double MarkDead, got %v", err)
	}
}

func TestRecoverClientPanic_NilPointer(t *testing.T) {
	var err error
	func() {
		defer recoverClientPanic(&err)
		// Simulate the nil pointer dereference that arrow-go causes
		var i *int
		_ = *i //nolint:govet
	}()

	if err == nil {
		t.Fatal("expected error from recovered nil pointer panic")
	}
	if !strings.Contains(err.Error(), "worker likely crashed") {
		t.Fatalf("expected crash message, got: %v", err)
	}
}

func TestRecoverClientPanic_NonNilPointerRePanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected re-panic for non-nil-pointer panic")
		}
		// Should be the original string panic, not a wrapped error
		if s, ok := r.(string); !ok || s != "some other panic" {
			t.Fatalf("expected original panic value, got: %v", r)
		}
	}()

	var err error
	func() {
		defer recoverClientPanic(&err)
		panic("some other panic")
	}()

	t.Fatal("should not reach here — panic should propagate")
}

func TestRecoverClientPanic_RuntimeErrorRePanics(t *testing.T) {
	// runtime.Error that is NOT a nil pointer should re-panic
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected re-panic for non-nil-pointer runtime error")
		}
		if re, ok := r.(runtime.Error); !ok {
			t.Fatalf("expected runtime.Error, got %T: %v", r, r)
		} else if strings.Contains(re.Error(), "nil pointer") {
			t.Fatal("this test should use a non-nil-pointer runtime error")
		}
	}()

	var err error
	func() {
		defer recoverClientPanic(&err)
		// Index out of range is a runtime.Error but not a nil pointer
		s := []int{}
		_ = s[1] //nolint:govet
	}()

	t.Fatal("should not reach here — runtime error should re-panic")
}

func TestFlightExecutorNilClient_QueryContextRecovers(t *testing.T) {
	// Simulate the exact scenario: executor with a nil client (as if Close()
	// nilled it out). QueryContext should recover the panic, not crash.
	e := &FlightExecutor{
		client: nil, // simulates closed client
	}

	_, err := e.QueryContext(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error from nil client")
	}
	if !strings.Contains(err.Error(), "worker likely crashed") {
		t.Fatalf("expected crash recovery message, got: %v", err)
	}
}

func TestFlightExecutorNilClient_ExecContextRecovers(t *testing.T) {
	e := &FlightExecutor{
		client: nil,
	}

	_, err := e.ExecContext(context.Background(), "SET x = 1")
	if err == nil {
		t.Fatal("expected error from nil client")
	}
	if !strings.Contains(err.Error(), "worker likely crashed") {
		t.Fatalf("expected crash recovery message, got: %v", err)
	}
}

func TestFlightExecutor_CloseCancel(t *testing.T) {
	// Verify that Close() cancels the executor's base context.
	ctx, cancel := context.WithCancel(context.Background())
	e := &FlightExecutor{ctx: ctx, cancel: cancel}

	merged, mergedCancel := e.mergedContext(context.Background())
	defer mergedCancel()

	if err := e.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case <-merged.Done():
		// Expected: merged context cancelled because executor was closed
	case <-time.After(time.Second):
		t.Fatal("merged context was not cancelled after executor Close()")
	}
}

func TestFlightExecutor_MergedContextCallerCancel(t *testing.T) {
	// Verify that cancelling the caller's context cancels the merged context,
	// even when the executor's base context is still alive.
	eCtx, eCancel := context.WithCancel(context.Background())
	defer eCancel()
	e := &FlightExecutor{ctx: eCtx, cancel: eCancel}

	callerCtx, callerCancel := context.WithCancel(context.Background())
	merged, mergedCancel := e.mergedContext(callerCtx)
	defer mergedCancel()

	callerCancel()

	select {
	case <-merged.Done():
		// Expected
	case <-time.After(time.Second):
		t.Fatal("merged context was not cancelled after caller cancel")
	}
}

func TestDisconnectMonitor_DetectsClose(t *testing.T) {
	// Create a connected pair of net.Conn (using net.Pipe).
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel()

	cc := &clientConn{
		conn:   server,
		reader: bufio.NewReader(server),
		ctx:    connCtx,
		cancel: connCancel,
	}

	queryCtx, queryCancel := context.WithCancel(connCtx)
	defer queryCancel()

	stop := cc.startDisconnectMonitor(queryCtx)
	defer stop()

	// Close the client side to simulate client disconnect.
	_ = client.Close()

	// The monitor should detect the disconnect and cancel connCtx.
	select {
	case <-connCtx.Done():
		// Expected: connection context cancelled
	case <-time.After(3 * time.Second):
		t.Fatal("disconnect monitor did not cancel context after client close")
	}
}

func TestDisconnectMonitor_StopBeforeDisconnect(t *testing.T) {
	// Verify that stop() returns promptly and the context is NOT cancelled
	// when the connection is still alive.
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel()

	cc := &clientConn{
		conn:   server,
		reader: bufio.NewReader(server),
		ctx:    connCtx,
		cancel: connCancel,
	}

	queryCtx, queryCancel := context.WithCancel(connCtx)
	defer queryCancel()

	stop := cc.startDisconnectMonitor(queryCtx)

	// Stop the monitor (simulating query completion) before any disconnect.
	done := make(chan struct{})
	go func() {
		stop()
		close(done)
	}()

	select {
	case <-done:
		// Expected: stop returned promptly
	case <-time.After(3 * time.Second):
		t.Fatal("stop() did not return promptly")
	}

	// Connection context should still be alive.
	select {
	case <-connCtx.Done():
		t.Fatal("connection context was cancelled unexpectedly")
	default:
		// Expected
	}
}

func TestDisconnectMonitor_BufferedDataNotLost(t *testing.T) {
	// Verify that data arriving during monitoring stays in the bufio.Reader
	// buffer and can be read after the monitor stops.
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel()

	cc := &clientConn{
		conn:   server,
		reader: bufio.NewReader(server),
		ctx:    connCtx,
		cancel: connCancel,
	}

	queryCtx, queryCancel := context.WithCancel(connCtx)
	defer queryCancel()

	stop := cc.startDisconnectMonitor(queryCtx)

	// Send data from the client side while the monitor is running.
	_, err := client.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Give the monitor time to Peek and buffer the data.
	time.Sleep(100 * time.Millisecond)

	stop()

	// The data should be available in the bufio.Reader.
	buf := make([]byte, 5)
	n, err := cc.reader.Read(buf)
	if err != nil {
		t.Fatalf("read after monitor stop failed: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(buf[:n]))
	}
}
